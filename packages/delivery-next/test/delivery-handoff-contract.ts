/**
 * The contract for stores that support remote handoff: memory and the Durable Object.
 *
 * It drives a store through MailboxDelivery, MailboxProcessingBackend, and DeliveryControlBackend,
 * under the test clock. Stores that do not support handoff yet run `handoffUnsupportedContract`.
 */
import { it } from '@effect/vitest'
import { Clock, Effect, Option, type Layer } from 'effect'
import { TestClock } from 'effect/testing'
import { expect } from 'vite-plus/test'

import {
	AddDeliveryLink,
	BatchId,
	CompleteDelivery,
	DELIVERY_RETENTION_MS,
	DeliveryAdmission,
	DeliveryControlBackend,
	DeliveryOutcome,
	DeliveryOutputSettlement,
	ExternalLink,
	FailDelivery,
	MailboxDelivery,
	MailboxProcessingBackend,
	OutputReadyMailbox,
	RecoverableMailbox,
	Timestamp,
	makeDeliveryId,
	parseDeliveryId,
	type ClaimedDeliveryOutput,
	type ClaimedMailboxBatch,
	type DeliveryMutation,
} from '../src'
import {
	claimAll,
	claimFrozen,
	deliver,
	event,
	findReady,
	findWaiting,
	leaseMs,
	mailboxKey,
	preparation,
	settle,
} from './backend-contract'

type HandoffStore = MailboxDelivery | MailboxProcessingBackend | DeliveryControlBackend

const reference = (claim: ClaimedMailboxBatch) =>
	Option.getOrThrow(parseDeliveryId(makeDeliveryId({ mailboxKey: claim.mailboxKey, batchId: claim.batchId })))

const handOff = (claim: ClaimedMailboxBatch, links: ReadonlyArray<ExternalLink> = []) =>
	Effect.gen(function* () {
		const backend = yield* MailboxProcessingBackend
		const handedOffAt = Timestamp.make(yield* Clock.currentTimeMillis)
		yield* backend.handOffDelivery({ mailboxKey: claim.mailboxKey, claimId: claim.claimId, handedOffAt, links })
	})

const status = (claim: ClaimedMailboxBatch, accessToken = claim.accessToken) =>
	Effect.gen(function* () {
		return yield* (yield* DeliveryControlBackend).readDeliveryStatus({ reference: reference(claim), accessToken })
	})

const completed = CompleteDelivery.make({ markdown: 'done' })
const failed = FailDelivery.make({})

const apply = (claim: ClaimedMailboxBatch, mutation: DeliveryMutation, accessToken = claim.accessToken) =>
	Effect.gen(function* () {
		return yield* (yield* DeliveryControlBackend).applyDeliveryMutation({
			reference: reference(claim),
			accessToken,
			mutation,
		})
	})

const finish = (claim: ClaimedMailboxBatch, mutation: DeliveryMutation = completed, accessToken = claim.accessToken) =>
	apply(claim, mutation, accessToken)

const link = (url: string) => ExternalLink.make({ label: 'Run', url })

const claimOutput = Effect.gen(function* () {
	return yield* (yield* MailboxProcessingBackend).claimDeliveryOutput({ mailboxKey, leaseMs })
})

const settleOutput = (
	claim: ClaimedDeliveryOutput,
	settlement: DeliveryOutputSettlement = DeliveryOutputSettlement.cases.Applied.make({}),
) =>
	Effect.gen(function* () {
		const settledAt = Timestamp.make(yield* Clock.currentTimeMillis)
		return yield* (yield* MailboxProcessingBackend).settleDeliveryOutput({
			mailboxKey,
			operationId: claim.operationId,
			claimId: claim.claimId,
			settlement,
			settledAt,
		})
	})

/** Claim the next due output operation and settle it as applied. Dies when none is due. */
const sendOutput = Effect.gen(function* () {
	const claim = Option.getOrThrow(yield* claimOutput)
	yield* settleOutput(claim)
	return claim
})

/** Claim the one waiting batch, prepare it, and hand it off. */
const claimAndHandOff = Effect.gen(function* () {
	const claim = yield* claimAll(yield* findWaiting)
	yield* (yield* MailboxProcessingBackend).prepareDelivery({
		mailboxKey: claim.mailboxKey,
		claimId: claim.claimId,
		prepared: preparation('onNewMention'),
	})
	yield* handOff(claim)
	return claim
})

/** A delivery handed off, whose callback has returned, waiting for its remote worker. */
const waitingDelivery = Effect.gen(function* () {
	yield* deliver('a')
	const claim = yield* claimAndHandOff
	yield* settle(claim, 'completed')
	return claim
})

export const deliveryHandoffContract = <E>(storeName: string, makeEmptyStore: () => Layer.Layer<HandoffStore, E>) => {
	const contract = <TestError>(name: string, test: Effect.Effect<void, TestError, HandoffStore>) =>
		it.effect(`${storeName} handoff: ${name}`, () => test.pipe(Effect.provide(makeEmptyStore())))

	contract(
		'a handed-off delivery holds the mailbox, with no lease, until its remote worker finishes and its output is sent',
		Effect.gen(function* () {
			yield* deliver('a')
			const claim = yield* claimAndHandOff
			expect((yield* status(claim)).stage).toEqual('ExternalCleaning')
			yield* settle(claim, 'completed')
			expect((yield* status(claim)).stage).toEqual('ExternalWaiting')
			expect((yield* status(claim)).supportedOperations).toEqual(['CreateMessage'])
			yield* deliver('follow-up')
			yield* TestClock.adjust(60 * leaseMs)
			expect(yield* findReady).toEqual([])

			expect((yield* finish(claim)).status).toEqual('accepted')
			expect((yield* status(claim)).stage).toEqual('Finishing')
			expect(yield* findReady).toEqual([OutputReadyMailbox.make({ mailboxKey })])
			const output = yield* sendOutput
			expect(output.operation).toEqual({ _tag: 'PresentOutcome', outcome: { _tag: 'Completed' }, markdown: 'done' })
			expect((yield* status(claim)).stage).toEqual('Retired')
			expect((yield* status(claim)).outcome).toEqual(DeliveryOutcome.cases.Completed.make({}))
			expect((yield* findWaiting).waiting.count).toEqual(1)
		}),
	)

	contract(
		'saves one PresentOutcome with the result, in the same change',
		Effect.gen(function* () {
			const claim = yield* waitingDelivery
			yield* finish(claim, CompleteDelivery.make({ awaitingInput: { options: ['staging', 'production'] } }))
			yield* finish(claim, CompleteDelivery.make({ awaitingInput: { options: ['staging', 'production'] } }))
			expect((yield* status(claim)).output).toEqual([
				{ operationId: 'outcome', kind: 'PresentOutcome', state: 'Pending', attempts: 0 },
			])
			const output = yield* sendOutput
			expect(output.operationId).toEqual('outcome')
			expect(output.operation).toEqual({
				_tag: 'PresentOutcome',
				outcome: { _tag: 'AwaitingInput', options: ['staging', 'production'] },
			})
			expect(output.provider).toEqual('example')
			expect(output.prepared).toEqual(preparation('onNewMention'))
			expect((yield* status(claim)).output).toEqual([
				{ operationId: 'outcome', kind: 'PresentOutcome', state: 'Delivered', attempts: 1 },
			])
		}),
	)

	contract(
		'gives output to one claimer at a time, and keeps it while the lease is renewed',
		Effect.gen(function* () {
			const claim = yield* waitingDelivery
			yield* finish(claim)
			const output = Option.getOrThrow(yield* claimOutput)
			expect(output.attempt).toEqual(1)
			expect(output.hadAmbiguousAttempt).toEqual(false)
			expect(Option.isNone(yield* claimOutput)).toEqual(true)
			expect(yield* findReady).toEqual([])
			yield* TestClock.adjust(leaseMs - 1)
			yield* (yield* MailboxProcessingBackend).renewDeliveryOutput({
				mailboxKey,
				operationId: output.operationId,
				claimId: output.claimId,
				leaseMs,
			})
			yield* TestClock.adjust(leaseMs - 1)
			expect(yield* findReady).toEqual([])
			expect(Option.isNone(yield* claimOutput)).toEqual(true)
			yield* settleOutput(output)
			expect((yield* status(claim)).stage).toEqual('Retired')
		}),
	)

	contract(
		'claims output again after its lease runs out, marks it ambiguous, and refuses the late settlement',
		Effect.gen(function* () {
			const claim = yield* waitingDelivery
			yield* finish(claim)
			const first = Option.getOrThrow(yield* claimOutput)
			yield* TestClock.adjust(leaseMs)
			expect(yield* findReady).toEqual([OutputReadyMailbox.make({ mailboxKey })])
			const second = Option.getOrThrow(yield* claimOutput)
			expect(second.operationId).toEqual(first.operationId)
			expect(second.claimId === first.claimId).toEqual(false)
			expect(second.attempt).toEqual(2)
			expect(second.hadAmbiguousAttempt).toEqual(true)
			expect((yield* settleOutput(first).pipe(Effect.flip))._tag).toEqual('MailboxProcessingClaimLost')
			const lateRenewal = yield* (yield* MailboxProcessingBackend)
				.renewDeliveryOutput({ mailboxKey, operationId: first.operationId, claimId: first.claimId, leaseMs })
				.pipe(Effect.flip)
			expect(lateRenewal._tag).toEqual('MailboxProcessingClaimLost')
			yield* settleOutput(second)
			expect((yield* status(claim)).stage).toEqual('Retired')
		}),
	)

	contract(
		'a retried output waits for its time, and the events behind it wait too',
		Effect.gen(function* () {
			const claim = yield* waitingDelivery
			yield* deliver('follow-up')
			yield* finish(claim)
			const first = Option.getOrThrow(yield* claimOutput)
			const now = yield* Clock.currentTimeMillis
			yield* settleOutput(first, DeliveryOutputSettlement.cases.Retry.make({ readyAt: Timestamp.make(now + 5_000) }))
			expect((yield* status(claim)).output[0]?.state).toEqual('Pending')
			yield* TestClock.adjust(4_999)
			expect(yield* findReady).toEqual([])
			expect(Option.isNone(yield* claimOutput)).toEqual(true)
			yield* TestClock.adjust(1)
			expect(yield* findReady).toEqual([OutputReadyMailbox.make({ mailboxKey })])
			const second = yield* sendOutput
			expect(second.attempt).toEqual(2)
			expect(second.hadAmbiguousAttempt).toEqual(false)
			expect((yield* findWaiting).waiting.count).toEqual(1)
		}),
	)

	contract(
		'a failed output retires the delivery and keeps its outcome',
		Effect.gen(function* () {
			const claim = yield* waitingDelivery
			yield* finish(claim)
			const output = Option.getOrThrow(yield* claimOutput)
			yield* settleOutput(output, DeliveryOutputSettlement.cases.Failed.make({ safeCode: 'attempts_exhausted' }))
			const retired = yield* status(claim)
			expect(retired.stage).toEqual('Retired')
			expect(retired.outcome).toEqual(DeliveryOutcome.cases.Completed.make({}))
			expect(retired.output).toEqual([{ operationId: 'outcome', kind: 'PresentOutcome', state: 'Failed', attempts: 1 }])
		}),
	)

	contract(
		'a callback failure after handoff does not undo it or schedule a retry',
		Effect.gen(function* () {
			yield* deliver('a')
			const claim = yield* claimAndHandOff
			yield* settle(claim, { retryAfterMs: 10 })
			yield* TestClock.adjust(1_000)
			expect(yield* findReady).toEqual([])
			expect((yield* status(claim)).stage).toEqual('ExternalWaiting')
		}),
	)

	contract(
		'handing off twice is harmless',
		Effect.gen(function* () {
			yield* deliver('a')
			const claim = yield* claimAndHandOff
			yield* handOff(claim)
			expect((yield* status(claim)).stage).toEqual('ExternalCleaning')
		}),
	)

	contract(
		'keeps a result that arrives before handoff, and sends its output once the callback returns',
		Effect.gen(function* () {
			yield* deliver('a')
			const claim = yield* claimAll(yield* findWaiting)
			expect((yield* finish(claim)).status).toEqual('accepted')
			expect((yield* status(claim)).stage).toEqual('Finishing')
			expect(Option.isNone(yield* claimOutput)).toEqual(true)
			yield* handOff(claim)
			yield* deliver('b')
			yield* settle(claim, 'completed')
			expect((yield* status(claim)).stage).toEqual('Finishing')
			yield* sendOutput
			expect((yield* status(claim)).stage).toEqual('Retired')
			expect((yield* findWaiting).waiting.count).toEqual(1)
		}),
	)

	contract(
		'keeps a result that arrives while the handed-off callback is still returning',
		Effect.gen(function* () {
			yield* deliver('a')
			const claim = yield* claimAndHandOff
			yield* finish(claim)
			expect((yield* status(claim)).stage).toEqual('Finishing')
			expect(Option.isNone(yield* claimOutput)).toEqual(true)
			yield* settle(claim, 'completed')
			yield* sendOutput
			expect((yield* status(claim)).stage).toEqual('Retired')
		}),
	)

	contract(
		'a lease that runs out after handoff does not run the callback again',
		Effect.gen(function* () {
			yield* deliver('a')
			const claim = yield* claimAndHandOff
			yield* TestClock.adjust(leaseMs)
			expect(yield* findReady).toEqual([RecoverableMailbox.make({ mailboxKey })])
			expect(Option.isNone(yield* claimFrozen)).toEqual(true)
			expect((yield* status(claim)).stage).toEqual('ExternalWaiting')
			const late = yield* settle(claim, 'completed').pipe(Effect.flip)
			expect(late._tag).toEqual('MailboxProcessingClaimLost')
		}),
	)

	contract(
		'a lease that runs out after a result sends the output without running the callback again',
		Effect.gen(function* () {
			yield* deliver('a')
			const claim = yield* claimAll(yield* findWaiting)
			yield* finish(claim)
			yield* TestClock.adjust(leaseMs)
			expect(Option.isNone(yield* claimFrozen)).toEqual(true)
			expect((yield* status(claim)).stage).toEqual('Finishing')
			yield* sendOutput
			expect((yield* status(claim)).stage).toEqual('Retired')
		}),
	)

	contract(
		'a result that arrives while waiting to retry sends its output instead of retrying',
		Effect.gen(function* () {
			yield* deliver('a')
			const claim = yield* claimAll(yield* findWaiting)
			yield* settle(claim, { retryAfterMs: 5_000 })
			yield* finish(claim)
			expect(yield* findReady).toEqual([OutputReadyMailbox.make({ mailboxKey })])
			yield* sendOutput
			expect((yield* status(claim)).stage).toEqual('Retired')
			yield* TestClock.adjust(5_000)
			expect(yield* findReady).toEqual([])
		}),
	)

	contract(
		'replays the same result and refuses a different one',
		Effect.gen(function* () {
			const claim = yield* waitingDelivery
			yield* finish(claim)
			expect((yield* finish(claim)).status).toEqual('already_recorded')
			expect((yield* finish(claim, failed).pipe(Effect.flip))._tag).toEqual('DeliveryTerminalConflict')
			const otherMarkdown = CompleteDelivery.make({ markdown: 'different' })
			expect((yield* finish(claim, otherMarkdown).pipe(Effect.flip))._tag).toEqual('DeliveryTerminalConflict')
			yield* sendOutput
			expect((yield* finish(claim)).status).toEqual('already_recorded')
			expect((yield* status(claim)).output).toHaveLength(1)
		}),
	)

	contract(
		'a wrong token and an unknown delivery look the same',
		Effect.gen(function* () {
			yield* deliver('a')
			const claim = yield* claimAndHandOff
			expect((yield* status(claim, 'wrong-token').pipe(Effect.flip))._tag).toEqual('DeliveryNotFound')
			expect((yield* finish(claim, completed, 'wrong-token').pipe(Effect.flip))._tag).toEqual('DeliveryNotFound')
			const addWithWrongToken = apply(claim, AddDeliveryLink.make({ link: link('https://example.com/x') }), 'wrong')
			expect((yield* addWithWrongToken.pipe(Effect.flip))._tag).toEqual('DeliveryNotFound')
			const unknown = { ...claim, batchId: BatchId.make('unknown-batch') }
			expect((yield* status(unknown).pipe(Effect.flip))._tag).toEqual('DeliveryNotFound')
			expect((yield* status(claim)).stage).toEqual('ExternalCleaning')
		}),
	)

	contract(
		'a delivery that finished without handoff takes no remote result and sends nothing',
		Effect.gen(function* () {
			yield* deliver('a')
			const claim = yield* claimAll(yield* findWaiting)
			yield* settle(claim, 'completed')
			expect((yield* status(claim)).stage).toEqual('Retired')
			expect((yield* status(claim)).output).toEqual([])
			expect((yield* finish(claim).pipe(Effect.flip))._tag).toEqual('DeliveryClosed')
		}),
	)

	contract(
		'an interrupting event marks the active delivery and still waits its turn',
		Effect.gen(function* () {
			const claim = yield* waitingDelivery
			expect((yield* status(claim)).interruptRequested).toEqual(false)
			const delivery = yield* MailboxDelivery
			yield* delivery.deliver(DeliveryAdmission.make({ ...event('stop'), interrupt: true }))
			expect((yield* status(claim)).interruptRequested).toEqual(true)
			expect(yield* findReady).toEqual([])
			yield* finish(claim)
			yield* sendOutput
			expect((yield* status(claim)).interruptRequested).toEqual(true)
			expect((yield* findWaiting).waiting.count).toEqual(1)
		}),
	)

	contract(
		'an interrupting event marks a finishing delivery',
		Effect.gen(function* () {
			const claim = yield* waitingDelivery
			yield* finish(claim)
			const delivery = yield* MailboxDelivery
			yield* delivery.deliver(DeliveryAdmission.make({ ...event('stop'), interrupt: true }))
			expect((yield* status(claim)).interruptRequested).toEqual(true)
		}),
	)

	contract(
		'an interrupting event at an idle mailbox marks nothing',
		Effect.gen(function* () {
			const delivery = yield* MailboxDelivery
			yield* delivery.deliver(DeliveryAdmission.make({ ...event('stop'), interrupt: true }))
			const claim = yield* claimAll(yield* findWaiting)
			expect((yield* status(claim)).interruptRequested).toEqual(false)
		}),
	)

	contract(
		'keeps a finished delivery readable for the retention period, then forgets it',
		Effect.gen(function* () {
			const claim = yield* waitingDelivery
			yield* finish(claim)
			yield* sendOutput
			yield* TestClock.adjust(DELIVERY_RETENTION_MS - 1)
			expect((yield* status(claim)).stage).toEqual('Retired')
			yield* TestClock.adjust(1)
			expect((yield* status(claim).pipe(Effect.flip))._tag).toEqual('DeliveryNotFound')
		}),
	)

	contract(
		'saves each new link given at handoff once, and sends it after the callback returns',
		Effect.gen(function* () {
			yield* deliver('a')
			const claim = yield* claimAll(yield* findWaiting)
			const run = link('https://example.com/run/1')
			yield* handOff(claim, [run, run, link('https://example.com/run/2')])
			yield* handOff(claim, [link('https://example.com/run/3')])
			expect((yield* status(claim)).output.map(({ operationId }) => operationId)).toEqual(['link-1', 'link-2'])
			expect(Option.isNone(yield* claimOutput)).toEqual(true)
			yield* settle(claim, 'completed')
			expect(yield* findReady).toEqual([OutputReadyMailbox.make({ mailboxKey })])
			const first = yield* sendOutput
			expect(first.operation).toEqual({ _tag: 'AddExternalLink', link: run })
			yield* sendOutput
			expect(yield* findReady).toEqual([])
			expect((yield* status(claim)).stage).toEqual('ExternalWaiting')
		}),
	)

	contract(
		'adds links from the remote worker, replays a repeated URL, and refuses new ones once the delivery ends',
		Effect.gen(function* () {
			const claim = yield* waitingDelivery
			const pullRequest = AddDeliveryLink.make({ link: link('https://github.com/org/repo/pull/1') })
			expect((yield* apply(claim, pullRequest)).status).toEqual('accepted')
			expect(yield* findReady).toEqual([OutputReadyMailbox.make({ mailboxKey })])
			const relabeled = AddDeliveryLink.make({
				link: ExternalLink.make({ label: 'Other', url: 'https://github.com/org/repo/pull/1' }),
			})
			expect((yield* apply(claim, relabeled)).status).toEqual('already_recorded')
			yield* finish(claim)
			const late = AddDeliveryLink.make({ link: link('https://github.com/org/repo/pull/2') })
			expect((yield* apply(claim, late).pipe(Effect.flip))._tag).toEqual('DeliveryClosed')
			const sent = [yield* sendOutput, yield* sendOutput].map(({ operation }) => operation._tag)
			expect(sent).toEqual(['AddExternalLink', 'PresentOutcome'])
			expect((yield* status(claim)).stage).toEqual('Retired')
			expect((yield* apply(claim, pullRequest)).status).toEqual('already_recorded')
			expect((yield* apply(claim, late).pipe(Effect.flip))._tag).toEqual('DeliveryClosed')
		}),
	)

	contract(
		'holds output while the callback runs, and sends it before a local delivery retires',
		Effect.gen(function* () {
			yield* deliver('a')
			const claim = yield* claimAll(yield* findWaiting)
			yield* apply(claim, AddDeliveryLink.make({ link: link('https://example.com/run/1') }))
			expect(Option.isNone(yield* claimOutput)).toEqual(true)
			yield* deliver('b')
			yield* settle(claim, 'completed')
			expect((yield* status(claim)).stage).toEqual('Finishing')
			expect((yield* finish(claim).pipe(Effect.flip))._tag).toEqual('DeliveryClosed')
			yield* sendOutput
			expect((yield* status(claim)).stage).toEqual('Retired')
			expect((yield* findWaiting).waiting.count).toEqual(1)
		}),
	)
}

/** The contract for stores that keep batch identity but cannot hand deliveries off yet. */
export const handoffUnsupportedContract = <E>(
	storeName: string,
	makeEmptyStore: () => Layer.Layer<MailboxDelivery | MailboxProcessingBackend, E>,
) =>
	it.effect(`${storeName}: refuses handoff until it supports remote control`, () =>
		Effect.gen(function* () {
			yield* deliver('a')
			const claim = yield* claimAll(yield* findWaiting)
			const refused = yield* handOff(claim).pipe(Effect.flip)
			expect(refused._tag).toEqual('DeliveryHandoffUnsupported')
			yield* settle(claim, 'completed')
			expect(yield* findReady).toEqual([])
			const backend = yield* MailboxProcessingBackend
			expect(Option.isNone(yield* backend.claimDeliveryOutput({ mailboxKey, leaseMs }))).toEqual(true)
		}).pipe(Effect.provide(makeEmptyStore())),
	)
