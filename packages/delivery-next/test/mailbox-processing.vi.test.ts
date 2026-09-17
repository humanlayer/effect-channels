import { describe, it } from '@effect/vitest'
import { Cause, Effect, Exit, Layer, Ref } from 'effect'

import {
	ClaimedMailboxBatch,
	DeliveryAdmission,
	DeliveryAdmissionBatch,
	MailboxProcessing,
	MailboxProcessingBackend,
	MailboxProcessingAttemptTerminalFailure,
	MailboxProcessingLive,
	ProviderEventDispatcher,
	ProviderEventExecutionFailed,
	ProviderEventHandled,
	type RecordProcessingAttemptResult,
} from '../src'

const admission = (resourceId: string) =>
	DeliveryAdmission.make({
		namespace: 'test',
		provider: 'example',
		installationId: 'installation',
		resourceId,
		eventId: `event-${resourceId}`,
		payload: { resourceId },
	})

const claim = (resourceId: string, attempt = 1) =>
	ClaimedMailboxBatch.make({
		mailboxKey: `test:example:installation:${resourceId}`,
		claimId: `claim-${resourceId}`,
		attempt,
		admissions: DeliveryAdmissionBatch.make([admission(resourceId)]),
	})

describe('mailbox processing', () => {
	it.effect('isolates a failed mailbox while processing distinct mailboxes concurrently', ({ expect }) =>
		Effect.gen(function* () {
			const recorded = yield* Ref.make<ReadonlyArray<RecordProcessingAttemptResult>>([])
			const backend = Layer.succeed(
				MailboxProcessingBackend,
				MailboxProcessingBackend.of({
					claimReadyMailboxes: Effect.succeed([claim('broken'), claim('healthy')]),
					recordProcessingAttemptResult: (result) => Ref.update(recorded, (results) => [...results, result]),
				}),
			)
			const dispatcher = Layer.succeed(
				ProviderEventDispatcher,
				ProviderEventDispatcher.of({
					process: (batch) =>
						batch[0].resourceId === 'broken'
							? Effect.die('simulated processor defect')
							: Effect.succeed(ProviderEventHandled.make({})),
				}),
			)
			const live = MailboxProcessingLive({ concurrency: 2 }).pipe(Layer.provide(Layer.merge(backend, dispatcher)))

			const summary = yield* Effect.gen(function* () {
				return yield* (yield* MailboxProcessing).processReady
			}).pipe(Effect.provide(live))

			expect(summary.claimed).toBe(2)
			const results = yield* Ref.get(recorded)
			expect(results).toHaveLength(1)
			expect(results[0]?.claim.mailboxKey).toContain('healthy')
		}),
	)

	it.effect('preserves interruption while isolating ordinary claim failures', ({ expect }) =>
		Effect.gen(function* () {
			const backend = Layer.succeed(
				MailboxProcessingBackend,
				MailboxProcessingBackend.of({
					claimReadyMailboxes: Effect.succeed([claim('interrupted')]),
					recordProcessingAttemptResult: () => Effect.void,
				}),
			)
			const dispatcher = Layer.succeed(
				ProviderEventDispatcher,
				ProviderEventDispatcher.of({ process: () => Effect.interrupt }),
			)
			const live = MailboxProcessingLive({ concurrency: 1 }).pipe(Layer.provide(Layer.merge(backend, dispatcher)))

			const result = yield* Effect.gen(function* () {
				return yield* (yield* MailboxProcessing).processReady
			}).pipe(Effect.provide(live), Effect.exit)

			expect(Exit.isFailure(result) && Cause.hasInterruptsOnly(result.cause)).toBe(true)
		}),
	)

	it.effect('turns the final retryable attempt into a terminal result', ({ expect }) =>
		Effect.gen(function* () {
			const recorded = yield* Ref.make<ReadonlyArray<RecordProcessingAttemptResult>>([])
			const backend = Layer.succeed(
				MailboxProcessingBackend,
				MailboxProcessingBackend.of({
					claimReadyMailboxes: Effect.succeed([claim('final', 3)]),
					recordProcessingAttemptResult: (result) => Ref.update(recorded, (results) => [...results, result]),
				}),
			)
			const dispatcher = Layer.succeed(
				ProviderEventDispatcher,
				ProviderEventDispatcher.of({
					process: () =>
						Effect.fail(
							new ProviderEventExecutionFailed({
								provider: 'example',
								safeCode: 'temporary',
								retryable: true,
							}),
						),
				}),
			)
			const live = MailboxProcessingLive({ concurrency: 1, maxAttempts: 3 }).pipe(
				Layer.provide(Layer.merge(backend, dispatcher)),
			)

			yield* Effect.gen(function* () {
				yield* (yield* MailboxProcessing).processReady
			}).pipe(Effect.provide(live))

			const results = yield* Ref.get(recorded)
			expect(results[0]?.result).toEqual(
				MailboxProcessingAttemptTerminalFailure.make({ safeCode: 'attempts_exhausted' }),
			)
		}),
	)

	it.effect('keeps a retry batch frozen ahead of admissions that arrive later', ({ expect }) =>
		Effect.gen(function* () {
			const first = admission('first')
			const later = admission('later')
			const claimPass = yield* Ref.make(0)
			const dispatchPass = yield* Ref.make(0)
			const dispatched = yield* Ref.make<ReadonlyArray<DeliveryAdmissionBatch>>([])
			const recorded = yield* Ref.make<ReadonlyArray<RecordProcessingAttemptResult>>([])
			const firstBatch = DeliveryAdmissionBatch.make([first])
			const laterBatch = DeliveryAdmissionBatch.make([later])
			const backend = Layer.succeed(
				MailboxProcessingBackend,
				MailboxProcessingBackend.of({
					claimReadyMailboxes: Ref.getAndUpdate(claimPass, (pass) => pass + 1).pipe(
						Effect.map((pass) => {
							if (pass === 0)
								return [
									ClaimedMailboxBatch.make({
										mailboxKey: 'mailbox',
										claimId: 'first-attempt',
										attempt: 1,
										admissions: firstBatch,
									}),
								]
							if (pass === 1)
								return [
									ClaimedMailboxBatch.make({
										mailboxKey: 'mailbox',
										claimId: 'retry-attempt',
										attempt: 2,
										admissions: firstBatch,
									}),
								]
							return [
								ClaimedMailboxBatch.make({
									mailboxKey: 'mailbox',
									claimId: 'later-admission',
									attempt: 1,
									admissions: laterBatch,
								}),
							]
						}),
					),
					recordProcessingAttemptResult: (result) => Ref.update(recorded, (results) => [...results, result]),
				}),
			)
			const dispatcher = Layer.succeed(
				ProviderEventDispatcher,
				ProviderEventDispatcher.of({
					process: (batch) =>
						Effect.gen(function* () {
							yield* Ref.update(dispatched, (batches) => [...batches, batch])
							const pass = yield* Ref.getAndUpdate(dispatchPass, (current) => current + 1)
							if (pass === 0) {
								return yield* new ProviderEventExecutionFailed({
									provider: 'example',
									safeCode: 'temporary',
									retryable: true,
								})
							}
							return ProviderEventHandled.make({})
						}),
				}),
			)
			const live = MailboxProcessingLive({ concurrency: 1 }).pipe(Layer.provide(Layer.merge(backend, dispatcher)))

			yield* Effect.gen(function* () {
				const processing = yield* MailboxProcessing
				yield* processing.processReady
				yield* processing.processReady
				yield* processing.processReady
			}).pipe(Effect.provide(live))

			expect(yield* Ref.get(dispatched)).toEqual([firstBatch, firstBatch, laterBatch])
			expect(yield* Ref.get(recorded)).toHaveLength(3)
		}),
	)

	it.effect('processes a recovered stale claim without losing its frozen batch', ({ expect }) =>
		Effect.gen(function* () {
			const frozenBatch = DeliveryAdmissionBatch.make([admission('stale')])
			const claimPass = yield* Ref.make(0)
			const dispatchPass = yield* Ref.make(0)
			const dispatched = yield* Ref.make<ReadonlyArray<DeliveryAdmissionBatch>>([])
			const recorded = yield* Ref.make<ReadonlyArray<RecordProcessingAttemptResult>>([])
			const backend = Layer.succeed(
				MailboxProcessingBackend,
				MailboxProcessingBackend.of({
					claimReadyMailboxes: Ref.getAndUpdate(claimPass, (pass) => pass + 1).pipe(
						Effect.map((pass) => [
							ClaimedMailboxBatch.make({
								mailboxKey: 'mailbox',
								claimId: pass === 0 ? 'abandoned-claim' : 'recovered-claim',
								attempt: pass + 1,
								admissions: frozenBatch,
							}),
						]),
					),
					recordProcessingAttemptResult: (result) => Ref.update(recorded, (results) => [...results, result]),
				}),
			)
			const dispatcher = Layer.succeed(
				ProviderEventDispatcher,
				ProviderEventDispatcher.of({
					process: (batch) =>
						Ref.update(dispatched, (batches) => [...batches, batch]).pipe(
							Effect.andThen(Ref.getAndUpdate(dispatchPass, (pass) => pass + 1)),
							Effect.flatMap((pass) =>
								pass === 0
									? Effect.die('simulated abandoned claim')
									: Effect.succeed(ProviderEventHandled.make({})),
							),
						),
				}),
			)
			const live = MailboxProcessingLive({ concurrency: 1 }).pipe(Layer.provide(Layer.merge(backend, dispatcher)))

			yield* Effect.gen(function* () {
				const processing = yield* MailboxProcessing
				yield* processing.processReady
				yield* processing.processReady
			}).pipe(Effect.provide(live))

			expect(yield* Ref.get(dispatched)).toEqual([frozenBatch, frozenBatch])
			const results = yield* Ref.get(recorded)
			expect(results).toHaveLength(1)
			expect(results[0]?.claim.claimId).toBe('recovered-claim')
			expect(results[0]?.claim.attempt).toBe(2)
		}),
	)
})
