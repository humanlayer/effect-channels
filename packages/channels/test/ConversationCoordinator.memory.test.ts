import { assert, it } from '@effect/vitest'
import { Effect, Fiber, Layer, Logger, Predicate, Queue, Ref } from 'effect'
import { TestClock } from 'effect/testing'

import type { InboundEvent } from '../src/index.ts'
import { ConversationCoordinator, ConversationCoordinatorOptions } from '../src/index.ts'
import { makeTestMessageEvent } from './support.ts'

const options = ConversationCoordinatorOptions.make({
	leaseTtlMs: 30_000,
	heartbeatEveryMs: 10_000,
	acquireTimeoutMs: 30_000,
	retryBaseMs: 100,
	retryMaxMs: 400,
	alertAfterAttempts: 2,
})

const firstEvent = makeTestMessageEvent(`evt_${'a'.repeat(32)}`)
const secondEvent = makeTestMessageEvent(`evt_${'b'.repeat(32)}`)

it.effect('retries a failing conversation with capped backoff, alerts after the threshold, and keeps FIFO order', () =>
	Effect.gen(function* () {
		const records: Array<{ readonly level: string; readonly text: string }> = []
		const recorder = Logger.make((entry) => {
			const first = Array.isArray(entry.message) ? entry.message.at(0) : entry.message
			records.push({ level: entry.logLevel, text: Predicate.isString(first) ? first : '' })
		})
		const program = Effect.gen(function* () {
			const attempts = yield* Ref.make(0)
			const attemptEntered = yield* Queue.unbounded<number>()
			const processed = yield* Queue.unbounded<string>()
			const coordinator = yield* ConversationCoordinator
			const handler = (event: InboundEvent) =>
				Effect.gen(function* () {
					if (event.idempotencyKey === firstEvent.idempotencyKey) {
						const attempt = yield* Ref.updateAndGet(attempts, (count) => count + 1)
						yield* Queue.offer(attemptEntered, attempt)
						if (attempt <= 4) {
							return yield* Effect.fail('handler unavailable' as const)
						}
					}
					yield* Queue.offer(processed, event.idempotencyKey)
				})
			const worker = yield* Effect.forkChild(coordinator.run(handler))

			yield* coordinator.submit(firstEvent)
			assert.strictEqual(yield* Queue.take(attemptEntered), 1)
			yield* coordinator.submit(secondEvent)

			assert.strictEqual(yield* Queue.size(attemptEntered), 0)
			yield* TestClock.adjust('100 millis')
			assert.strictEqual(yield* Queue.take(attemptEntered), 2)
			assert.strictEqual(yield* Queue.size(attemptEntered), 0)
			yield* TestClock.adjust('200 millis')
			assert.strictEqual(yield* Queue.take(attemptEntered), 3)
			assert.strictEqual(yield* Queue.size(attemptEntered), 0)
			yield* TestClock.adjust('400 millis')
			assert.strictEqual(yield* Queue.take(attemptEntered), 4)
			assert.strictEqual(yield* Queue.size(attemptEntered), 0)
			yield* TestClock.adjust('400 millis')
			assert.strictEqual(yield* Queue.take(attemptEntered), 5)

			assert.strictEqual(yield* Queue.take(processed), firstEvent.idempotencyKey)
			assert.strictEqual(yield* Queue.take(processed), secondEvent.idempotencyKey)
			yield* Fiber.interrupt(worker)
		})
		yield* program.pipe(
			Effect.provide(Layer.mergeAll(ConversationCoordinator.layerMemory(options), Logger.layer([recorder]))),
		)

		const warnings = records.filter((record) => record.level === 'Warn' && record.text.includes('retrying'))
		const alerts = records.filter((record) => record.level === 'Error' && record.text.includes('keeps failing'))
		assert.strictEqual(warnings.length, 1)
		assert.strictEqual(alerts.length, 3)
	}),
)
