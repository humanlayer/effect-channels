import { assert, it } from '@effect/vitest'
import { layer as memory } from '@humanlayer/channels-github/memory'
import { Context, Deferred, Effect, Exit, Fiber, Layer, Match, Ref } from 'effect'
import { TestClock } from 'effect/testing'

import { GitHubError, GitHubIngress, GitHubSubscriptions } from '../src/index.js'
import { event, policy } from './fixtures.js'

for (const callback of ['onCreation', 'onMention', 'onSubscribedEvent'] as const) {
	it.effect(`releases ${callback} handler resources before the rate-limit wait`, () =>
		Effect.gen(function* () {
			const released = yield* Deferred.make<void>()
			const handler = () =>
				Effect.gen(function* () {
					yield* Effect.addFinalizer(() => Deferred.succeed(released, undefined))
					return yield* GitHubError.make({ reason: 'unavailable', retryAfterMs: 60_000 })
				})
			const environment = yield* Layer.build(
				GitHubIngress.layer({
					namespace: 'rate-limit-scope',
					policy,
					handlers: [
						Match.value(callback).pipe(
							Match.when('onCreation', () => ({ id: 'receive', onCreation: handler })),
							Match.when('onMention', () => ({ id: 'receive', onMention: handler })),
							Match.when('onSubscribedEvent', () => ({ id: 'receive', onSubscribedEvent: handler })),
							Match.exhaustive,
						),
					],
				}).pipe(Layer.provideMerge(memory({ maxMailboxes: 10 }))),
			)
			const ingress = Context.get(environment, GitHubIngress)
			yield* Context.get(environment, GitHubSubscriptions).subscribe({
				namespace: 'rate-limit-scope',
				resource: event.resource,
			})
			yield* ingress.acceptActivity({ event, mentioned: true, own: false })
			const work = yield* ingress.processActivity({ event }).pipe(Effect.forkChild)
			yield* Deferred.await(released)
			yield* TestClock.adjust(60_000)
			yield* Fiber.join(work)
		}),
	)
}

for (const outcome of ['success', 'failure', 'defect', 'interruption'] as const) {
	it.effect(`handler resources finalize on ${outcome} while the application layer stays alive`, () =>
		Effect.gen(function* () {
			const entered = yield* Deferred.make<void>()
			const released = yield* Ref.make(0)
			const environment = yield* Layer.build(
				GitHubIngress.layer({
					namespace: 'scope',
					policy,
					handlers: [
						{
							id: 'receive',
							onCreation: () =>
								Effect.gen(function* () {
									yield* Effect.addFinalizer(() => Ref.update(released, (n) => n + 1))
									yield* Deferred.succeed(entered, undefined)
									if (outcome === 'failure') return yield* GitHubError.make({ reason: 'forbidden' })
									if (outcome === 'defect') return yield* Effect.die('synthetic defect')
									if (outcome === 'interruption') return yield* Effect.never
								}),
						},
					],
				}).pipe(Layer.provideMerge(memory({ maxMailboxes: 10 }))),
			)
			const ingress = Context.get(environment, GitHubIngress)
			yield* ingress.acceptActivity({ event, mentioned: false, own: false })
			const work = yield* ingress.processActivity({ event }).pipe(Effect.forkChild)
			yield* Deferred.await(entered)
			if (outcome === 'interruption') yield* Fiber.interrupt(work)
			else if (outcome === 'defect') assert.ok(Exit.isFailure(yield* Fiber.await(work)))
			else yield* Fiber.join(work)
			assert.equal(yield* Ref.get(released), 1)
		}),
	)
}
