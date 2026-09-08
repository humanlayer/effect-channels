import { assert, it } from '@effect/vitest'
import { layer as memory } from '@humanlayer/channels-github/memory'
import { Context, Deferred, Effect, Exit, Fiber, Layer, Ref } from 'effect'
import { TestClock } from 'effect/testing'

import { GitHubError, GitHubIngress } from '../src/index.js'
import { event, policy } from './fixtures.js'

for (const activity of [false, true]) {
	it.effect(`releases ${activity ? 'activity' : 'legacy'} handler resources before the rate-limit wait`, () =>
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
					...(activity
						? { activityHandlers: [{ id: 'receive', onCreation: handler }] }
						: { handlers: [{ id: 'receive', handler }] }),
				}).pipe(Layer.provide(memory({ maxMailboxes: 10 }))),
			)
			const ingress = Context.get(environment, GitHubIngress)
			if (activity) yield* ingress.acceptActivity({ event, mentioned: false, own: false })
			else yield* ingress.accept({ event })
			const work = yield* (activity ? ingress.processActivity({ event }) : ingress.process({ event })).pipe(
				Effect.forkChild,
			)
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
							handler: () =>
								Effect.gen(function* () {
									yield* Effect.addFinalizer(() => Ref.update(released, (n) => n + 1))
									yield* Deferred.succeed(entered, undefined)
									if (outcome === 'failure') return yield* GitHubError.make({ reason: 'forbidden' })
									if (outcome === 'defect') return yield* Effect.die('synthetic defect')
									if (outcome === 'interruption') return yield* Effect.never
								}),
						},
					],
				}).pipe(Layer.provide(memory({ maxMailboxes: 10 }))),
			)
			const ingress = Context.get(environment, GitHubIngress)
			yield* ingress.accept({ event })
			const work = yield* ingress.process({ event }).pipe(Effect.forkChild)
			yield* Deferred.await(entered)
			if (outcome === 'interruption') yield* Fiber.interrupt(work)
			else if (outcome === 'defect') assert.ok(Exit.isFailure(yield* Fiber.await(work)))
			else yield* Fiber.join(work)
			assert.equal(yield* Ref.get(released), 1)
		}),
	)
}
