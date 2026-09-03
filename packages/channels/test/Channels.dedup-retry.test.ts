import { assert, it } from '@effect/vitest'
import { Data, Deferred, Effect, Fiber, Queue, Ref } from 'effect'
import { TestClock } from 'effect/testing'

import { Channels, HistoryFailed, Ingress, IngressAccepted, type MessageRef } from '../src/index.ts'
import { ChannelsWithIngressLayer, makeTestNormalizedMessage } from './support.ts'

class FirstDeliveryAttemptFailed extends Data.TaggedError('FirstDeliveryAttemptFailed') {}

it.effect('delivers an event once when the same idempotency key arrives twice', () =>
	Effect.gen(function* () {
		const channels = yield* Channels
		const ingress = yield* Ingress
		const delivered = yield* Queue.unbounded<MessageRef>()
		yield* channels.onNewMention((_thread, message) => Queue.offer(delivered, message.ref).pipe(Effect.asVoid))
		const worker = yield* Effect.forkChild(channels.run)

		const original = makeTestNormalizedMessage({ messageTs: '100.1', mentioned: true })
		const redelivered = makeTestNormalizedMessage({ messageTs: '100.1', mentioned: true })
		assert.strictEqual(redelivered.idempotencyKey, original.idempotencyKey)
		assert.deepStrictEqual(
			yield* ingress.acceptMessage(original),
			IngressAccepted.make({ idempotencyKey: original.idempotencyKey }),
		)
		assert.deepStrictEqual(
			yield* ingress.acceptMessage(redelivered),
			IngressAccepted.make({ idempotencyKey: original.idempotencyKey }),
		)
		yield* ingress.acceptMessage(
			makeTestNormalizedMessage({ messageTs: '100.2', rootTs: '100.1', mentioned: true }),
		)

		assert.strictEqual(yield* Queue.take(delivered), '100.1')
		assert.strictEqual(yield* Queue.take(delivered), '100.2')
		assert.strictEqual(yield* Queue.size(delivered), 0)

		yield* Fiber.interrupt(worker)
	}).pipe(Effect.provide(ChannelsWithIngressLayer)),
)

it.effect('retries a failed delivery after the backoff delay and then succeeds', () =>
	Effect.gen(function* () {
		const channels = yield* Channels
		const ingress = yield* Ingress
		const attempts = yield* Ref.make(0)
		const failedOnce = yield* Deferred.make<void>()
		const delivered = yield* Queue.unbounded<MessageRef>()
		yield* channels.onNewMention((_thread, message) =>
			Effect.gen(function* () {
				const attempt = yield* Ref.updateAndGet(attempts, (count) => count + 1)
				if (attempt === 1) {
					yield* Deferred.succeed(failedOnce, undefined)
					return yield* new FirstDeliveryAttemptFailed()
				}
				yield* Queue.offer(delivered, message.ref)
			}),
		)
		const worker = yield* Effect.forkChild(channels.run)

		yield* ingress.acceptMessage(makeTestNormalizedMessage({ messageTs: '100.1', mentioned: true }))
		yield* Deferred.await(failedOnce)
		assert.strictEqual(yield* Queue.size(delivered), 0)

		yield* TestClock.adjust('100 millis')
		assert.strictEqual(yield* Queue.take(delivered), '100.1')
		assert.strictEqual(yield* Ref.get(attempts), 2)

		yield* Fiber.interrupt(worker)
	}).pipe(Effect.provide(ChannelsWithIngressLayer)),
)

it.effect('preserves non-retryable handler metadata and advances to the next message', () =>
	Effect.gen(function* () {
		const channels = yield* Channels
		const ingress = yield* Ingress
		const attempts = yield* Ref.make(0)
		const delivered = yield* Queue.unbounded<MessageRef>()
		yield* channels.onNewMention((_thread, message) =>
			message.ref === '100.1'
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
				: Queue.offer(delivered, message.ref).pipe(Effect.asVoid),
		)
		const worker = yield* Effect.forkChild(channels.run)
		yield* ingress.acceptMessage(makeTestNormalizedMessage({ messageTs: '100.1', mentioned: true }))
		yield* ingress.acceptMessage(
			makeTestNormalizedMessage({ messageTs: '100.2', rootTs: '100.1', mentioned: true }),
		)
		assert.strictEqual(yield* Queue.take(delivered), '100.2')
		assert.strictEqual(yield* Ref.get(attempts), 1)
		yield* Fiber.interrupt(worker)
	}).pipe(Effect.provide(ChannelsWithIngressLayer)),
)
