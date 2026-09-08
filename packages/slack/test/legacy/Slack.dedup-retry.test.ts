import { assert, it } from '@effect/vitest'
import { Deferred, Effect, Fiber, Queue, Ref } from 'effect'
import { TestClock } from 'effect/testing'

import { HistoryFailed, IngressAccepted, SlackIngress, type MessageEvent } from '../../src/index.js'
import { ingressLayer, makeTestNormalizedMessage, runnerOptions } from './support.js'

it.effect('delivers a duplicate notification once and still delivers the next same-thread message', () =>
	Effect.gen(function* () {
		const delivered = yield* Queue.unbounded<string>()
		yield* Effect.gen(function* () {
			const ingress = yield* SlackIngress
			const worker = yield* Effect.forkChild(ingress.run(runnerOptions))
			const original = makeTestNormalizedMessage({ messageTs: '100.1', mentioned: true })
			for (const event of [original, original]) {
				assert.deepStrictEqual(
					yield* ingress.acceptMessage(event),
					IngressAccepted.make({ idempotencyKey: original.idempotencyKey }),
				)
			}
			yield* ingress.acceptMessage(
				makeTestNormalizedMessage({ messageTs: '100.2', rootTs: '100.1', mentioned: true }),
			)
			yield* TestClock.adjust(runnerOptions.pollMs)
			assert.strictEqual(yield* Queue.take(delivered), '100.1')
			yield* TestClock.adjust(runnerOptions.pollMs)
			assert.strictEqual(yield* Queue.take(delivered), '100.2')
			assert.strictEqual(yield* Queue.size(delivered), 0)
			yield* Fiber.interrupt(worker)
		}).pipe(
			Effect.provide(
				ingressLayer({
					onNewMention: [
						{
							id: 'mention',
							handler: (event) => Queue.offer(delivered, event.message.ref).pipe(Effect.asVoid),
						},
					],
				}),
			),
		)
	}),
)

it.effect('retries with capped backoff and does not advance serial delivery past the failed event', () =>
	Effect.gen(function* () {
		const attempts = yield* Ref.make(0)
		const entered = yield* Queue.unbounded<number>()
		const processed = yield* Queue.unbounded<string>()
		const firstAttempt = yield* Deferred.make<void>()
		const handler = (event: MessageEvent) =>
			Effect.gen(function* () {
				if (event.message.ref === '100.1') {
					const attempt = yield* Ref.updateAndGet(attempts, (value) => value + 1)
					yield* Queue.offer(entered, attempt)
					yield* Deferred.succeed(firstAttempt, undefined)
					if (attempt <= 4) return yield* Effect.fail('retryable failure')
				}
				yield* Queue.offer(processed, event.message.ref)
			})
		yield* Effect.gen(function* () {
			const ingress = yield* SlackIngress
			const worker = yield* Effect.forkChild(ingress.run(runnerOptions))
			yield* ingress.acceptMessage(makeTestNormalizedMessage({ messageTs: '100.1', mentioned: true }))
			yield* TestClock.adjust('10 millis')
			yield* Deferred.await(firstAttempt)
			assert.strictEqual(yield* Queue.take(entered), 1)
			yield* ingress.acceptMessage(
				makeTestNormalizedMessage({ messageTs: '100.2', rootTs: '100.1', mentioned: true }),
			)
			for (const [delay, attempt] of [
				[100, 2],
				[200, 3],
				[400, 4],
				[400, 5],
			] as const) {
				assert.strictEqual(yield* Queue.size(entered), 0)
				assert.strictEqual(yield* Queue.size(processed), 0)
				yield* TestClock.adjust(delay)
				assert.strictEqual(yield* Queue.take(entered), attempt)
			}
			yield* TestClock.adjust('20 millis')
			assert.deepStrictEqual(yield* Queue.takeAll(processed), ['100.1', '100.2'])
			yield* Fiber.interrupt(worker)
		}).pipe(Effect.provide(ingressLayer({ onNewMention: [{ id: 'mention', handler }] })))
	}),
)

it.effect('preserves native non-retryable handler metadata and advances to the next message', () =>
	Effect.gen(function* () {
		const attempts = yield* Ref.make(0)
		const delivered = yield* Queue.unbounded<string>()
		const handler = (event: MessageEvent) =>
			event.message.ref === '100.1'
				? Ref.update(attempts, (count) => count + 1).pipe(
						Effect.andThen(
							Effect.fail(
								HistoryFailed.make({
									provider: 'slack',
									message: 'missing scope',
									retryability: 'non_retryable',
								}),
							),
						),
					)
				: Queue.offer(delivered, event.message.ref).pipe(Effect.asVoid)
		yield* Effect.gen(function* () {
			const ingress = yield* SlackIngress
			const worker = yield* Effect.forkChild(ingress.run(runnerOptions))
			yield* ingress.acceptMessage(makeTestNormalizedMessage({ messageTs: '100.1', mentioned: true }))
			yield* ingress.acceptMessage(
				makeTestNormalizedMessage({ messageTs: '100.2', rootTs: '100.1', mentioned: true }),
			)
			yield* TestClock.adjust('20 millis')
			assert.deepStrictEqual(yield* Queue.takeAll(delivered), ['100.2'])
			assert.strictEqual(yield* Ref.get(attempts), 1)
			yield* Fiber.interrupt(worker)
		}).pipe(Effect.provide(ingressLayer({ onNewMention: [{ id: 'mention', handler }] })))
	}),
)
