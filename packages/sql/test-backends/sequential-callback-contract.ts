import { it } from '@effect/vitest'
import {
	CompleteDelivery,
	DeliveryControlBackend,
	DeliveryOutputSettlement,
	MailboxDelivery,
	MailboxProcessingAttemptTerminalFailure,
	MailboxProcessingBackend,
	PreparedDeliveryInvocation,
	Timestamp,
	makeDeliveryId,
	parseDeliveryId,
} from '@humanlayer/channels-delivery'
import { Clock, Effect, Option, type Layer } from 'effect'
import { TestClock } from 'effect/testing'

import {
	claimAll,
	claimFrozen,
	deliver,
	findWaiting,
	leaseMs,
	mailboxKey,
	settle,
} from '../../delivery/test/backend-contract'

type Store = MailboxDelivery | MailboxProcessingBackend | DeliveryControlBackend

/** Reused by the three persistent adapters; every call reloads state through the real backend seam. */
export const sequentialCallbackContract = <E>(name: string, makeStore: () => Layer.Layer<Store, E>) => {
	it.effect(`${name}: round-trips sequential callback identities, cleanup, output, and retry`, ({ expect }) =>
		Effect.gen(function* () {
			const backend = yield* MailboxProcessingBackend
			const control = yield* DeliveryControlBackend
			yield* deliver('sequence')
			const first = yield* claimAll(yield* findWaiting)
			const tokens = [first.accessToken, 'sequence-second', 'sequence-third'] as const
			const prepared = PreparedDeliveryInvocation.make({
				callbacks: [
					{
						name: 'inspect',
						presentationVersion: 1,
						destination: { thread: 'first' },
						supportedOperations: ['PresentOutcome'],
					},
					{
						name: 'implement',
						presentationVersion: 2,
						destination: { thread: 'second' },
						supportedOperations: [],
					},
					{
						name: 'review',
						presentationVersion: 3,
						destination: { thread: 'third' },
						supportedOperations: ['PresentOutcome'],
					},
				],
			})
			const reference = (callbackIndex: number) =>
				Option.getOrThrow(
					parseDeliveryId(makeDeliveryId({ mailboxKey, batchId: first.batchId, callbackIndex })),
				)
			const read = (callbackIndex: number, accessToken = tokens[callbackIndex] ?? '') =>
				control.readDeliveryStatus({ reference: reference(callbackIndex), accessToken })
			yield* backend.prepareDelivery({
				mailboxKey,
				claimId: first.claimId,
				prepared,
				callbackAccessTokens: tokens,
			})
			yield* backend.handOffDelivery({
				mailboxKey,
				claimId: first.claimId,
				handedOffAt: Timestamp.make(yield* Clock.currentTimeMillis),
				links: [],
			})
			yield* control.applyDeliveryMutation({
				reference: reference(0),
				accessToken: tokens[0],
				mutation: CompleteDelivery.make({ markdown: 'done' }),
			})
			expect(yield* claimFrozen).toEqual(Option.none())
			yield* settle(first, 'completed')
			const output = Option.getOrThrow(
				yield* backend.claimDeliveryOutput({
					mailboxKey,
					leaseMs,
					idempotencyKey: '00000000-0000-4000-8000-000000000099',
				}),
			)
			expect(output.callbackIndex).toBe(0)
			expect(output.prepared).toEqual(prepared.callbacks[0])
			expect(yield* claimFrozen).toEqual(Option.none())
			yield* backend.settleDeliveryOutput({
				mailboxKey,
				claimId: output.claimId,
				operationId: output.operationId,
				settlement: DeliveryOutputSettlement.cases.Applied.make({}),
				settledAt: Timestamp.make(yield* Clock.currentTimeMillis),
			})
			expect((yield* read(0)).stage).toBe('Retired')
			expect((yield* read(1)).supportedOperations).toEqual([])
			expect((yield* read(1, tokens[0]).pipe(Effect.flip))._tag).toBe('DeliveryNotFound')
			const second = Option.getOrThrow(yield* claimFrozen)
			expect(second).toMatchObject({
				batchId: first.batchId,
				callbackIndex: 1,
				accessToken: tokens[1],
				attempt: 1,
				prepared,
			})
			yield* settle(second, { retryAfterMs: 50 })
			expect(yield* claimFrozen).toEqual(Option.none())
			yield* TestClock.adjust(50)
			const retry = Option.getOrThrow(yield* claimFrozen)
			expect(retry).toMatchObject({ callbackIndex: 1, accessToken: tokens[1], attempt: 2, prepared })
			yield* settle(retry, 'completed')
			const third = Option.getOrThrow(yield* claimFrozen)
			expect(third).toMatchObject({
				batchId: first.batchId,
				callbackIndex: 2,
				accessToken: tokens[2],
				attempt: 1,
				prepared,
			})
			yield* backend.handOffDelivery({
				mailboxKey,
				claimId: third.claimId,
				handedOffAt: Timestamp.make(yield* Clock.currentTimeMillis),
				links: [],
			})
			expect((yield* read(0)).stage).toBe('Retired')
			yield* settle(third, 'completed')
			yield* control.applyDeliveryMutation({
				reference: reference(2),
				accessToken: tokens[2],
				mutation: CompleteDelivery.make({}),
			})
			const lastOutput = Option.getOrThrow(
				yield* backend.claimDeliveryOutput({
					mailboxKey,
					leaseMs,
					idempotencyKey: '00000000-0000-4000-8000-000000000098',
				}),
			)
			expect(lastOutput.callbackIndex).toBe(2)
			expect(lastOutput.prepared).toEqual(prepared.callbacks[2])
			expect((yield* read(1)).stage).toBe('Retired')
			yield* backend.settleDeliveryOutput({
				mailboxKey,
				claimId: lastOutput.claimId,
				operationId: lastOutput.operationId,
				settlement: DeliveryOutputSettlement.cases.Applied.make({}),
				settledAt: Timestamp.make(yield* Clock.currentTimeMillis),
			})
			for (const index of [0, 1, 2]) expect((yield* read(index)).stage).toBe('Retired')
			expect((yield* read(0, tokens[2]).pipe(Effect.flip))._tag).toBe('DeliveryNotFound')
			expect(yield* backend.findReadyMailboxes).toEqual([])
		}).pipe(Effect.provide(makeStore())),
	)
	it.effect(`${name}: a terminal callback failure does not start the next stored step`, ({ expect }) =>
		Effect.gen(function* () {
			const backend = yield* MailboxProcessingBackend
			yield* deliver('failed-sequence')
			const claim = yield* claimAll(yield* findWaiting)
			const callback = { name: 'first', presentationVersion: 1, destination: {}, supportedOperations: [] }
			yield* backend.prepareDelivery({
				mailboxKey,
				claimId: claim.claimId,
				prepared: PreparedDeliveryInvocation.make({ callbacks: [callback, { ...callback, name: 'second' }] }),
				callbackAccessTokens: [claim.accessToken, 'never-started'],
			})
			yield* backend.recordProcessingAttemptResult({
				claim,
				finishedAt: Timestamp.make(yield* Clock.currentTimeMillis),
				result: MailboxProcessingAttemptTerminalFailure.make({ safeCode: 'failed' }),
			})
			expect(yield* claimFrozen).toEqual(Option.none())
			const control = yield* DeliveryControlBackend
			expect(
				(yield* control.readDeliveryStatus({
					reference: Option.getOrThrow(parseDeliveryId(makeDeliveryId(claim))),
					accessToken: claim.accessToken,
				})).stage,
			).toBe('Retired')
			expect(
				(yield* control
					.readDeliveryStatus({
						reference: Option.getOrThrow(parseDeliveryId(makeDeliveryId({ ...claim, callbackIndex: 1 }))),
						accessToken: 'never-started',
					})
					.pipe(Effect.flip))._tag,
			).toBe('DeliveryNotFound')
		}).pipe(Effect.provide(makeStore())),
	)
}
