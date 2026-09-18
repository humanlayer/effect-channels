import { describe, it } from '@effect/vitest'
import { Cause, Context, Deferred, Effect, Exit, Fiber, Layer, Queue, Ref } from 'effect'
import { TestClock } from 'effect/testing'

import {
	BurstDeliveryMode,
	DebounceDeliveryMode,
	DeliveryAdmission,
	MailboxDelivery,
	MailboxProcessing,
	MailboxProcessingBackend,
	MailboxProcessingClaimLost,
	MailboxProcessingLive,
	ProviderEventDispatcher,
	ProviderEventExecutionFailed,
	ProviderEventHandled,
	QueueDeliveryMode,
	SerialDeliveryMode,
	type DeliveryAdmissionBatch,
	type DeliveryMode,
	type MailboxProcessingOptions,
	type ProviderEventProcessingError,
	type ProviderEventResult,
} from '../src'
import { MailboxBackendMemory } from './MailboxBackendMemory'

const admission = (eventId: string, resourceId = 'thread-1') =>
	DeliveryAdmission.make({
		namespace: 'test',
		provider: 'example',
		installationId: 'installation',
		resourceId,
		eventId,
		payload: { eventId },
	})

const deliver = (eventId: string, resourceId?: string) =>
	Effect.gen(function* () {
		yield* (yield* MailboxDelivery).deliver(admission(eventId, resourceId))
	})

const processReady = Effect.gen(function* () {
	return yield* (yield* MailboxProcessing).processReady
})

const eventIds = (batch: DeliveryAdmissionBatch) => batch.map(({ eventId }) => eventId)

const handled = Effect.succeed(ProviderEventHandled.make({}))

const temporaryFailure = new ProviderEventExecutionFailed({
	provider: 'example',
	safeCode: 'temporary',
	retryable: true,
})

/** A dispatcher that records every batch it is handed, then runs the test's callback. */
const recordingDispatcher = (
	dispatched: Queue.Queue<ReadonlyArray<string>>,
	callback: (
		batch: DeliveryAdmissionBatch,
	) => Effect.Effect<ProviderEventResult, ProviderEventProcessingError> = () => handled,
) =>
	Layer.succeed(
		ProviderEventDispatcher,
		ProviderEventDispatcher.of({
			process: (batch) => Queue.offer(dispatched, eventIds(batch)).pipe(Effect.andThen(callback(batch))),
		}),
	)

const processingOptions = (overrides: Partial<MailboxProcessingOptions> = {}): MailboxProcessingOptions => ({
	concurrency: 2,
	leaseMs: 30_000,
	deliveryModeFor: () => QueueDeliveryMode.make({}),
	polling: 'disabled',
	...overrides,
})

/** The real processing layer over the in-memory store, with the store's services left visible to the test. */
const processingOver = (dispatcher: Layer.Layer<ProviderEventDispatcher>, options: MailboxProcessingOptions) =>
	MailboxProcessingLive(options).pipe(Layer.provide(dispatcher), Layer.provideMerge(MailboxBackendMemory))

/** An in-memory store built ahead of the processing layer, for tests that wrap or call the store themselves. */
const buildMemoryStore = Effect.gen(function* () {
	const store = yield* Layer.build(MailboxBackendMemory)
	return {
		store,
		delivery: Context.get(store, MailboxDelivery),
		backend: Context.get(store, MailboxProcessingBackend),
	}
})

const withMode = (mode: DeliveryMode) => processingOptions({ deliveryModeFor: () => mode })

describe('mailbox processing', () => {
	it.effect('isolates a failed mailbox while processing distinct mailboxes concurrently', ({ expect }) =>
		Effect.gen(function* () {
			const dispatched = yield* Queue.unbounded<ReadonlyArray<string>>()
			const dispatcher = recordingDispatcher(dispatched, (batch) =>
				batch[0].resourceId === 'broken' ? Effect.die('simulated processor defect') : handled,
			)
			yield* Effect.gen(function* () {
				yield* deliver('a', 'broken')
				yield* deliver('b', 'healthy')
				expect(yield* processReady).toEqual({ claimed: 2, deferred: 0 })
				expect(yield* (yield* MailboxProcessingBackend).findReadyMailboxes).toEqual([])
				yield* TestClock.adjust(30_000)
				expect(yield* (yield* MailboxProcessingBackend).findReadyMailboxes).toHaveLength(1)
			}).pipe(Effect.provide(processingOver(dispatcher, processingOptions())))
		}),
	)

	it.effect('preserves interruption while isolating ordinary claim failures', ({ expect }) =>
		Effect.gen(function* () {
			const dispatched = yield* Queue.unbounded<ReadonlyArray<string>>()
			const dispatcher = recordingDispatcher(dispatched, () => Effect.interrupt)
			const result = yield* deliver('a').pipe(
				Effect.andThen(processReady),
				Effect.provide(processingOver(dispatcher, processingOptions())),
				Effect.exit,
			)
			expect(Exit.isFailure(result) && Cause.hasInterruptsOnly(result.cause)).toBe(true)
		}),
	)

	it.effect('retries the frozen batch ahead of later events, then gives up at the last attempt', ({ expect }) =>
		Effect.gen(function* () {
			const dispatched = yield* Queue.unbounded<ReadonlyArray<string>>()
			const failuresLeft = yield* Ref.make(2)
			const dispatcher = recordingDispatcher(dispatched, (batch) =>
				batch[0].eventId === 'first'
					? Ref.getAndUpdate(failuresLeft, (left) => left - 1).pipe(
							Effect.flatMap((left) => (left > 0 ? Effect.fail(temporaryFailure) : handled)),
						)
					: handled,
			)
			yield* Effect.gen(function* () {
				yield* deliver('first')
				yield* processReady
				yield* deliver('later')
				expect((yield* processReady).claimed).toBe(0)
				yield* TestClock.adjust(1_000)
				yield* processReady
				yield* processReady
				expect(yield* Queue.takeAll(dispatched)).toEqual([['first'], ['first'], ['later']])
			}).pipe(Effect.provide(processingOver(dispatcher, processingOptions({ maxAttempts: 2 }))))
		}),
	)

	it.effect('recovers a claim whose worker died, with the same frozen batch', ({ expect }) =>
		Effect.gen(function* () {
			const dispatched = yield* Queue.unbounded<ReadonlyArray<string>>()
			const firstRun = yield* Ref.make(true)
			const dispatcher = recordingDispatcher(dispatched, () =>
				Ref.getAndSet(firstRun, false).pipe(
					Effect.flatMap((first) => (first ? Effect.die('simulated abandoned claim') : handled)),
				),
			)
			yield* Effect.gen(function* () {
				yield* deliver('stale')
				yield* processReady
				expect((yield* processReady).claimed).toBe(0)
				yield* TestClock.adjust(30_000)
				expect((yield* processReady).claimed).toBe(1)
				expect(yield* Queue.takeAll(dispatched)).toEqual([['stale'], ['stale']])
				expect(yield* (yield* MailboxProcessingBackend).findReadyMailboxes).toEqual([])
			}).pipe(Effect.provide(processingOver(dispatcher, processingOptions())))
		}),
	)
})

describe('attempt limit', () => {
	it.effect('fails a batch that keeps killing its worker, without running it again', ({ expect }) =>
		Effect.gen(function* () {
			const dispatched = yield* Queue.unbounded<ReadonlyArray<string>>()
			const dispatcher = recordingDispatcher(dispatched, (batch) =>
				batch[0].eventId === 'poison' ? Effect.die('simulated worker crash') : handled,
			)
			yield* Effect.gen(function* () {
				yield* deliver('poison')
				yield* processReady
				yield* TestClock.adjust(30_000)
				yield* processReady
				yield* deliver('next')
				yield* TestClock.adjust(30_000)
				expect((yield* processReady).claimed).toBe(1)
				expect(yield* Queue.takeAll(dispatched)).toEqual([['poison'], ['poison']])
				expect((yield* processReady).claimed).toBe(1)
				expect(yield* Queue.takeAll(dispatched)).toEqual([['next']])
			}).pipe(Effect.provide(processingOver(dispatcher, processingOptions({ maxAttempts: 2 }))))
		}),
	)
})

describe('delivery modes', () => {
	it.effect('queue hands everything that is waiting to one callback', ({ expect }) =>
		Effect.gen(function* () {
			const dispatched = yield* Queue.unbounded<ReadonlyArray<string>>()
			yield* Effect.gen(function* () {
				yield* deliver('a')
				yield* deliver('b')
				yield* processReady
				expect(yield* Queue.takeAll(dispatched)).toEqual([['a', 'b']])
			}).pipe(
				Effect.provide(processingOver(recordingDispatcher(dispatched), withMode(QueueDeliveryMode.make({})))),
			)
		}),
	)

	it.effect('serial hands over one event per callback, oldest first', ({ expect }) =>
		Effect.gen(function* () {
			const dispatched = yield* Queue.unbounded<ReadonlyArray<string>>()
			yield* Effect.gen(function* () {
				yield* deliver('a')
				yield* deliver('b')
				yield* processReady
				yield* processReady
				expect(yield* Queue.takeAll(dispatched)).toEqual([['a'], ['b']])
			}).pipe(
				Effect.provide(processingOver(recordingDispatcher(dispatched), withMode(SerialDeliveryMode.make({})))),
			)
		}),
	)

	it.effect('debounce waits for quiet, and every new event restarts the wait', ({ expect }) =>
		Effect.gen(function* () {
			const dispatched = yield* Queue.unbounded<ReadonlyArray<string>>()
			yield* Effect.gen(function* () {
				yield* deliver('a')
				expect(yield* processReady).toEqual({ claimed: 0, deferred: 1 })
				yield* TestClock.adjust(1_500)
				yield* deliver('b')
				expect(yield* processReady).toEqual({ claimed: 0, deferred: 1 })
				yield* TestClock.adjust(1_999)
				expect(yield* processReady).toEqual({ claimed: 0, deferred: 0 })
				yield* TestClock.adjust(1)
				expect(yield* processReady).toEqual({ claimed: 1, deferred: 0 })
				expect(yield* Queue.takeAll(dispatched)).toEqual([['a', 'b']])
			}).pipe(
				Effect.provide(
					processingOver(
						recordingDispatcher(dispatched),
						withMode(DebounceDeliveryMode.make({ quietPeriodMs: 2_000 })),
					),
				),
			)
		}),
	)

	it.effect('debounce stops waiting once the oldest event has waited the maximum', ({ expect }) =>
		Effect.gen(function* () {
			const dispatched = yield* Queue.unbounded<ReadonlyArray<string>>()
			yield* Effect.gen(function* () {
				yield* deliver('a')
				yield* processReady
				yield* TestClock.adjust(1_500)
				yield* deliver('b')
				yield* processReady
				yield* TestClock.adjust(1_500)
				yield* deliver('c')
				expect(yield* processReady).toEqual({ claimed: 1, deferred: 0 })
				expect(yield* Queue.takeAll(dispatched)).toEqual([['a', 'b', 'c']])
			}).pipe(
				Effect.provide(
					processingOver(
						recordingDispatcher(dispatched),
						withMode(DebounceDeliveryMode.make({ quietPeriodMs: 2_000, maxWaitMs: 3_000 })),
					),
				),
			)
		}),
	)

	it.effect('burst collects for a fixed window after the first event', ({ expect }) =>
		Effect.gen(function* () {
			const dispatched = yield* Queue.unbounded<ReadonlyArray<string>>()
			yield* Effect.gen(function* () {
				yield* deliver('a')
				expect(yield* processReady).toEqual({ claimed: 0, deferred: 1 })
				yield* TestClock.adjust(1_500)
				yield* deliver('b')
				expect(yield* processReady).toEqual({ claimed: 0, deferred: 1 })
				yield* TestClock.adjust(500)
				expect(yield* processReady).toEqual({ claimed: 1, deferred: 0 })
				expect(yield* Queue.takeAll(dispatched)).toEqual([['a', 'b']])
			}).pipe(
				Effect.provide(
					processingOver(
						recordingDispatcher(dispatched),
						withMode(BurstDeliveryMode.make({ windowMs: 2_000 })),
					),
				),
			)
		}),
	)

	it.effect('debounces events that arrived during a run, after the run settles', ({ expect }) =>
		Effect.gen(function* () {
			const dispatched = yield* Queue.unbounded<ReadonlyArray<string>>()
			const { store, delivery } = yield* buildMemoryStore
			const dispatcher = recordingDispatcher(dispatched, (batch) =>
				batch[0].eventId === 'a'
					? delivery.deliver(admission('during-run')).pipe(Effect.orDie, Effect.andThen(handled))
					: handled,
			)
			yield* Effect.gen(function* () {
				yield* deliver('a')
				yield* TestClock.adjust(2_000)
				expect(yield* processReady).toEqual({ claimed: 1, deferred: 0 })
				expect(yield* processReady).toEqual({ claimed: 0, deferred: 1 })
				yield* TestClock.adjust(2_000)
				yield* processReady
				expect(yield* Queue.takeAll(dispatched)).toEqual([['a'], ['during-run']])
			}).pipe(
				Effect.provide(
					MailboxProcessingLive(withMode(DebounceDeliveryMode.make({ quietPeriodMs: 2_000 }))).pipe(
						Layer.provide(dispatcher),
						Layer.provideMerge(Layer.succeedContext(store)),
					),
				),
			)
		}),
	)
})

describe('claim lease', () => {
	it.effect('keeps renewing the lease while the callback runs, and stops when it finishes', ({ expect }) =>
		Effect.gen(function* () {
			const dispatched = yield* Queue.unbounded<ReadonlyArray<string>>()
			const finishCallback = yield* Deferred.make<void>()
			const dispatcher = recordingDispatcher(dispatched, () =>
				Deferred.await(finishCallback).pipe(Effect.andThen(handled)),
			)
			yield* Effect.gen(function* () {
				const backend = yield* MailboxProcessingBackend
				yield* deliver('slow')
				const pass = yield* processReady.pipe(Effect.forkChild)
				yield* Queue.take(dispatched)
				yield* TestClock.adjust(9_000)
				expect(yield* backend.findReadyMailboxes).toEqual([])
				yield* Deferred.succeed(finishCallback, undefined)
				expect(yield* Fiber.join(pass)).toEqual({ claimed: 1, deferred: 0 })
				yield* TestClock.adjust(60_000)
				expect(yield* backend.findReadyMailboxes).toEqual([])
			}).pipe(Effect.provide(processingOver(dispatcher, processingOptions({ leaseMs: 3_000 }))))
		}),
	)

	it.effect('interrupts the callback and records nothing once the claim belongs to someone else', ({ expect }) =>
		Effect.gen(function* () {
			const callbackStarted = yield* Deferred.make<void>()
			const callbackInterrupted = yield* Deferred.make<void>()
			const recorded = yield* Ref.make(0)
			const { store, backend: realBackend } = yield* buildMemoryStore
			const backendThatLosesClaims = Layer.succeed(
				MailboxProcessingBackend,
				MailboxProcessingBackend.of({
					...realBackend,
					renewClaim: ({ mailboxKey, claimId }) =>
						Effect.fail(new MailboxProcessingClaimLost({ mailboxKey, claimId })),
					recordProcessingAttemptResult: (input) =>
						Ref.update(recorded, (count) => count + 1).pipe(
							Effect.andThen(realBackend.recordProcessingAttemptResult(input)),
						),
				}),
			)
			const dispatcher = Layer.succeed(
				ProviderEventDispatcher,
				ProviderEventDispatcher.of({
					process: () =>
						Deferred.succeed(callbackStarted, undefined).pipe(
							Effect.andThen(Effect.never),
							Effect.onInterrupt(() => Deferred.succeed(callbackInterrupted, undefined)),
						),
				}),
			)
			yield* Effect.gen(function* () {
				yield* deliver('slow')
				const pass = yield* processReady.pipe(Effect.forkChild)
				yield* Deferred.await(callbackStarted)
				yield* TestClock.adjust(1_000)
				yield* Deferred.await(callbackInterrupted)
				expect(yield* Fiber.join(pass)).toEqual({ claimed: 1, deferred: 0 })
				expect(yield* Ref.get(recorded)).toBe(0)
			}).pipe(
				Effect.provide(
					MailboxProcessingLive(processingOptions({ leaseMs: 3_000 })).pipe(
						Layer.provide(Layer.merge(dispatcher, backendThatLosesClaims)),
					),
				),
				Effect.provide(store),
			)
		}),
	)
})

describe('polling', () => {
	it.effect('processes on its own, goes again at once after finding work, and survives a failed pass', ({ expect }) =>
		Effect.gen(function* () {
			const dispatched = yield* Queue.unbounded<ReadonlyArray<string>>()
			const failNextLook = yield* Ref.make(false)
			const looks = yield* Queue.unbounded<void>()
			const { store, backend: realBackend } = yield* buildMemoryStore
			const observedBackend = Layer.succeed(
				MailboxProcessingBackend,
				MailboxProcessingBackend.of({
					...realBackend,
					findReadyMailboxes: Queue.offer(looks, undefined).pipe(
						Effect.andThen(Ref.getAndSet(failNextLook, false)),
						Effect.flatMap((fail) =>
							fail ? Effect.die('simulated look defect') : realBackend.findReadyMailboxes,
						),
					),
				}),
			)
			yield* Effect.gen(function* () {
				yield* Queue.take(looks)
				yield* deliver('a')
				yield* deliver('b')
				yield* TestClock.adjust(1_000)
				expect(yield* Queue.take(dispatched)).toEqual(['a'])
				expect(yield* Queue.take(dispatched)).toEqual(['b'])
				yield* Queue.take(looks).pipe(Effect.repeat({ times: 2 }))

				yield* Ref.set(failNextLook, true)
				yield* deliver('c')
				yield* TestClock.adjust(1_000)
				expect(yield* Queue.size(dispatched)).toBe(0)
				yield* TestClock.adjust(1_000)
				expect(yield* Queue.take(dispatched)).toEqual(['c'])
			}).pipe(
				Effect.provide(
					MailboxProcessingLive(
						processingOptions({
							deliveryModeFor: () => SerialDeliveryMode.make({}),
							polling: { intervalMs: 1_000 },
						}),
					).pipe(Layer.provide(Layer.merge(recordingDispatcher(dispatched), observedBackend))),
				),
				Effect.provide(store),
			)
		}),
	)
})
