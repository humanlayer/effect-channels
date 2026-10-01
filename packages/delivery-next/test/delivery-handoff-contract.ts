/**
 * The contract for stores that support remote handoff: memory, the Durable Object, Postgres, and Redis.
 *
 * It drives a store through MailboxDelivery, MailboxProcessingBackend, and DeliveryControlBackend,
 * under the test clock.
 */
import { it } from '@effect/vitest'
import { Clock, Effect, Option, type Layer } from 'effect'
import { TestClock } from 'effect/testing'
import { expect } from 'vite-plus/test'

import {
	AddDeliveryLink,
	BatchId,
	CompleteDelivery,
	CreateDeliveryMessage,
	DeleteDeliveryMessage,
	DELIVERY_RETENTION_MS,
	DeliveryAdmission,
	DeliveryControlBackend,
	DeliveryOutcome,
	DeliveryOutputSettlement,
	ExternalLink,
	DeliveryActivity,
	FailDelivery,
	MailboxDelivery,
	MessageId,
	PreparedDeliveryInvocation,
	SetDeliveryActivity,
	UpdateDeliveryMessage,
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

/** Claim the next due output operation, offering `idempotencyKey` for an operation claimed for the first time. */
const claimOutputWithKey = (idempotencyKey: string) =>
	Effect.gen(function* () {
		return yield* (yield* MailboxProcessingBackend).claimDeliveryOutput({ mailboxKey, leaseMs, idempotencyKey })
	})

const claimOutput = claimOutputWithKey('00000000-0000-4000-8000-000000000001')

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
const sendOutputWithKey = (idempotencyKey: string) =>
	Effect.gen(function* () {
		const claim = Option.getOrThrow(yield* claimOutputWithKey(idempotencyKey))
		yield* settleOutput(claim)
		return claim
	})

const sendOutput = sendOutputWithKey('00000000-0000-4000-8000-000000000001')

/** Claim the one waiting batch and prepare it, as a provider does before its callback runs. */
const claimPreparedWith = (prepared: PreparedDeliveryInvocation) =>
	Effect.gen(function* () {
		const claim = yield* claimAll(yield* findWaiting)
		yield* (yield* MailboxProcessingBackend).prepareDelivery({
			mailboxKey: claim.mailboxKey,
			claimId: claim.claimId,
			prepared,
		})
		return claim
	})

const claimPrepared = claimPreparedWith(preparation('onNewMention'))

/** Claim the one waiting batch, prepare it, and hand it off. */
const claimAndHandOff = Effect.gen(function* () {
	const claim = yield* claimPrepared
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

/** A destination that supports the whole message lifecycle. */
const messagePreparation = PreparedDeliveryInvocation.make({
	callback: 'onNewMention',
	presentationVersion: 1,
	destination: { thread: 'thread-1' },
	supportedOperations: ['PresentOutcome', 'AddExternalLink', 'CreateMessage', 'UpdateMessage', 'DeleteMessage', 'SetActivity'],
})

/** A delivery whose destination supports messages, handed off, whose callback has returned. */
const messageDelivery = Effect.gen(function* () {
	yield* deliver('a')
	const claim = yield* claimPreparedWith(messagePreparation)
	yield* handOff(claim)
	yield* settle(claim, 'completed')
	return claim
})

const progress = MessageId.make('progress')
const createProgress = (markdown: string) => CreateDeliveryMessage.make({ messageId: progress, markdown })
const updateProgress = (markdown: string) => UpdateDeliveryMessage.make({ messageId: progress, markdown })
const deleteProgress = DeleteDeliveryMessage.make({ messageId: progress })
const working = (message: string) =>
	SetDeliveryActivity.make({ activity: DeliveryActivity.cases.Working.make({ message }) })
const idle = SetDeliveryActivity.make({ activity: DeliveryActivity.cases.Idle.make({}) })

/** A delivery prepared for a destination that supports only `supportedOperations`, handed off and waiting. */
const deliveryFor = (supportedOperations: PreparedDeliveryInvocation['supportedOperations']) =>
	Effect.gen(function* () {
		yield* deliver('a')
		const claim = yield* claimPreparedWith(PreparedDeliveryInvocation.make({ ...messagePreparation, supportedOperations }))
		yield* handOff(claim)
		yield* settle(claim, 'completed')
		return claim
	})

const postedReceipt = (ts: string) => DeliveryOutputSettlement.cases.Applied.make({ receipt: { ts } })

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
			expect((yield* status(claim)).supportedOperations).toEqual(['PresentOutcome', 'AddExternalLink', 'CreateMessage'])
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
				{ operationId: 'outcome', kind: 'PresentOutcome', state: 'Pending', attempts: 0, hadAmbiguousAttempt: false },
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
				{ operationId: 'outcome', kind: 'PresentOutcome', state: 'Delivered', attempts: 1, hadAmbiguousAttempt: false },
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
			expect(retired.output).toEqual([{ operationId: 'outcome', kind: 'PresentOutcome', state: 'Failed', attempts: 1, hadAmbiguousAttempt: false }])
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
			const claim = yield* claimPrepared
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
			const claim = yield* claimPrepared
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
			const claim = yield* claimPrepared
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
			const claim = yield* claimPrepared
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
			const claim = yield* claimPrepared
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
			const claim = yield* claimPrepared
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
			const claim = yield* claimPrepared
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

	contract(
		'runs a message\'s create, update, and delete in order, handing later changes the create\'s receipt',
		Effect.gen(function* () {
			const claim = yield* messageDelivery
			expect((yield* apply(claim, createProgress('Running tests…'))).status).toEqual('accepted')
			expect(yield* findReady).toEqual([OutputReadyMailbox.make({ mailboxKey })])
			expect((yield* apply(claim, updateProgress('Tests passed.'))).status).toEqual('accepted')
			expect((yield* apply(claim, deleteProgress)).status).toEqual('accepted')
			expect((yield* status(claim)).output).toEqual([
				{ operationId: 'message-1', kind: 'CreateMessage', messageId: progress, state: 'Pending', attempts: 0, hadAmbiguousAttempt: false },
				{ operationId: 'message-2', kind: 'UpdateMessage', messageId: progress, state: 'Pending', attempts: 0, hadAmbiguousAttempt: false },
				{ operationId: 'message-3', kind: 'DeleteMessage', messageId: progress, state: 'Pending', attempts: 0, hadAmbiguousAttempt: false },
			])

			const create = Option.getOrThrow(yield* claimOutput)
			expect(create.operation).toEqual({ _tag: 'CreateMessage', messageId: progress, markdown: 'Running tests…' })
			expect(create.messageReference).toBeUndefined()
			expect(Option.isNone(yield* claimOutput)).toEqual(true)
			yield* settleOutput(create, postedReceipt('1'))

			const update = Option.getOrThrow(yield* claimOutput)
			expect(update.operation).toEqual({ _tag: 'UpdateMessage', messageId: progress, markdown: 'Tests passed.' })
			expect(update.messageReference).toEqual({ ts: '1' })
			yield* settleOutput(update)
			const deletion = yield* sendOutput
			expect(deletion.operation).toEqual({ _tag: 'DeleteMessage', messageId: progress })
			expect(deletion.messageReference).toEqual({ ts: '1' })
			expect(yield* findReady).toEqual([])
			expect((yield* status(claim)).stage).toEqual('ExternalWaiting')
		}),
	)

	contract(
		'replays repeated message changes, and refuses a reused ID with other text or a change to a removed message',
		Effect.gen(function* () {
			const claim = yield* messageDelivery
			yield* apply(claim, createProgress('a'))
			expect((yield* apply(claim, createProgress('a'))).status).toEqual('already_recorded')
			expect((yield* apply(claim, createProgress('b')).pipe(Effect.flip))._tag).toEqual('DeliveryMessageConflict')
			expect((yield* apply(claim, updateProgress('a'))).status).toEqual('already_recorded')
			expect((yield* apply(claim, updateProgress('b'))).status).toEqual('accepted')
			expect((yield* apply(claim, updateProgress('b'))).status).toEqual('already_recorded')
			expect((yield* apply(claim, deleteProgress)).status).toEqual('accepted')
			expect((yield* apply(claim, deleteProgress)).status).toEqual('already_recorded')
			expect((yield* apply(claim, updateProgress('c')).pipe(Effect.flip))._tag).toEqual('DeliveryMessageDeleted')
			expect((yield* status(claim)).output.map(({ kind }) => kind)).toEqual([
				'CreateMessage',
				'UpdateMessage',
				'DeleteMessage',
			])
		}),
	)

	contract(
		'refuses changes to a message never created, and fails a change whose message could not be posted',
		Effect.gen(function* () {
			const claim = yield* messageDelivery
			const unknown = UpdateDeliveryMessage.make({ messageId: MessageId.make('unknown'), markdown: 'x' })
			expect((yield* apply(claim, unknown).pipe(Effect.flip))._tag).toEqual('DeliveryMessageNotFound')
			yield* apply(claim, createProgress('a'))
			yield* apply(claim, updateProgress('b'))
			const create = Option.getOrThrow(yield* claimOutput)
			yield* settleOutput(create, DeliveryOutputSettlement.cases.Failed.make({ safeCode: 'slack_post_failed' }))
			const update = Option.getOrThrow(yield* claimOutput)
			expect(update.operation._tag).toEqual('UpdateMessage')
			expect(update.messageReference).toBeUndefined()
			expect((yield* apply(claim, updateProgress('c')).pipe(Effect.flip))._tag).toEqual('DeliveryMessageNotFound')
			expect((yield* apply(claim, deleteProgress).pipe(Effect.flip))._tag).toEqual('DeliveryMessageNotFound')
		}),
	)

	contract(
		'refuses an operation the destination does not support, and saves nothing',
		Effect.gen(function* () {
			const claim = yield* waitingDelivery
			expect((yield* apply(claim, createProgress('a'))).status).toEqual('accepted')
			const refused = yield* apply(claim, updateProgress('b')).pipe(Effect.flip)
			expect(refused).toMatchObject({ _tag: 'DeliveryOperationUnsupported', operation: 'UpdateMessage' })
			expect((yield* status(claim)).output.map(({ kind }) => kind)).toEqual(['CreateMessage'])

			yield* deliver('b')
			yield* sendOutput
			yield* finish(claim)
			yield* sendOutput
			const unprepared = yield* claimAll(yield* findWaiting)
			const refusedUnprepared = yield* apply(unprepared, createProgress('a')).pipe(Effect.flip)
			expect(refusedUnprepared).toMatchObject({ _tag: 'DeliveryOperationUnsupported', operation: 'CreateMessage' })
		}),
	)

	contract(
		'refuses new message changes once the delivery ends, and still replays saved ones until it retires',
		Effect.gen(function* () {
			const claim = yield* messageDelivery
			yield* apply(claim, createProgress('a'))
			yield* finish(claim)
			expect((yield* apply(claim, createProgress('a'))).status).toEqual('already_recorded')
			expect((yield* apply(claim, updateProgress('b')).pipe(Effect.flip))._tag).toEqual('DeliveryClosed')
			expect((yield* apply(claim, deleteProgress).pipe(Effect.flip))._tag).toEqual('DeliveryClosed')
			const other = CreateDeliveryMessage.make({ messageId: MessageId.make('other'), markdown: 'x' })
			expect((yield* apply(claim, other).pipe(Effect.flip))._tag).toEqual('DeliveryClosed')
			yield* settleOutput(Option.getOrThrow(yield* claimOutput), postedReceipt('1'))
			yield* sendOutput
			expect((yield* status(claim)).stage).toEqual('Retired')
			expect((yield* apply(claim, deleteProgress).pipe(Effect.flip))._tag).toEqual('DeliveryClosed')
		}),
	)

	contract(
		'posts a create again after its lease runs out, shows the ambiguity, and updates the message that settled',
		Effect.gen(function* () {
			const claim = yield* messageDelivery
			yield* apply(claim, createProgress('a'))
			yield* apply(claim, updateProgress('b'))
			const first = Option.getOrThrow(yield* claimOutput)
			yield* TestClock.adjust(leaseMs)
			const second = Option.getOrThrow(yield* claimOutput)
			expect(second.operationId).toEqual(first.operationId)
			expect(second.hadAmbiguousAttempt).toEqual(true)
			expect((yield* settleOutput(first, postedReceipt('1')).pipe(Effect.flip))._tag).toEqual(
				'MailboxProcessingClaimLost',
			)
			yield* settleOutput(second, postedReceipt('2'))
			expect((yield* status(claim)).output[0]).toMatchObject({
				kind: 'CreateMessage',
				state: 'Delivered',
				attempts: 2,
				hadAmbiguousAttempt: true,
			})
			const update = Option.getOrThrow(yield* claimOutput)
			expect(update.messageReference).toEqual({ ts: '2' })
		}),
	)

	contract(
		'refuses a link or a result the destination cannot show, and saves nothing',
		Effect.gen(function* () {
			const claim = yield* deliveryFor(['PresentOutcome'])
			const link = AddDeliveryLink.make({ link: ExternalLink.make({ label: 'Run', url: 'https://example.com/run/1' }) })
			const refused = yield* apply(claim, link).pipe(Effect.flip)
			expect(refused).toMatchObject({ _tag: 'DeliveryOperationUnsupported', operation: 'AddExternalLink' })
			const activity = yield* apply(claim, working('x')).pipe(Effect.flip)
			expect(activity).toMatchObject({ _tag: 'DeliveryOperationUnsupported', operation: 'SetActivity' })
			expect((yield* status(claim)).output).toEqual([])
			expect(yield* findReady).toEqual([])
			expect((yield* finish(claim)).status).toEqual('accepted')
		}),
	)

	contract(
		'refuses a result for a destination that cannot present it',
		Effect.gen(function* () {
			const claim = yield* deliveryFor([])
			const refused = yield* finish(claim).pipe(Effect.flip)
			expect(refused).toMatchObject({ _tag: 'DeliveryOperationUnsupported', operation: 'PresentOutcome' })
			const unchanged = yield* status(claim)
			expect(unchanged.stage).toEqual('ExternalWaiting')
			expect(unchanged.outcome).toBeUndefined()
		}),
	)

	contract(
		'keeps only the latest desired activity: a waiting SetActivity is replaced, one being sent is followed',
		Effect.gen(function* () {
			const claim = yield* messageDelivery
			expect((yield* apply(claim, idle)).status).toEqual('already_recorded')
			expect((yield* status(claim)).output).toEqual([])
			expect((yield* apply(claim, working('Reading logs'))).status).toEqual('accepted')
			expect(yield* findReady).toEqual([OutputReadyMailbox.make({ mailboxKey })])
			expect((yield* apply(claim, working('Running tests'))).status).toEqual('accepted')
			expect((yield* apply(claim, working('Running tests'))).status).toEqual('already_recorded')
			expect((yield* status(claim)).activity).toEqual(DeliveryActivity.cases.Working.make({ message: 'Running tests' }))
			expect((yield* status(claim)).output.map(({ operationId }) => operationId)).toEqual(['activity-1'])

			const first = Option.getOrThrow(yield* claimOutput)
			expect(first.operation).toEqual({ _tag: 'SetActivity', activity: { _tag: 'Working', message: 'Running tests' } })
			yield* apply(claim, working('Fixing'))
			yield* apply(claim, idle)
			yield* settleOutput(first)
			const second = yield* sendOutput
			expect(second.operationId).toEqual('activity-2')
			expect(second.operation).toEqual({ _tag: 'SetActivity', activity: { _tag: 'Idle' } })
			expect((yield* status(claim)).activity).toEqual(DeliveryActivity.cases.Idle.make({}))
			expect(yield* findReady).toEqual([])
		}),
	)

	contract(
		'a retried or lost SetActivity is sent again with the latest activity',
		Effect.gen(function* () {
			const claim = yield* messageDelivery
			yield* apply(claim, working('a'))
			const first = Option.getOrThrow(yield* claimOutput)
			const now = yield* Clock.currentTimeMillis
			yield* settleOutput(first, DeliveryOutputSettlement.cases.Retry.make({ readyAt: Timestamp.make(now + 5_000) }))
			yield* apply(claim, working('b'))
			expect((yield* status(claim)).output.map(({ operationId }) => operationId)).toEqual(['activity-1'])
			yield* TestClock.adjust(5_000)
			const retried = Option.getOrThrow(yield* claimOutput)
			expect(retried.attempt).toEqual(2)
			expect(retried.operation).toEqual({ _tag: 'SetActivity', activity: { _tag: 'Working', message: 'b' } })

			yield* TestClock.adjust(leaseMs)
			const recovered = Option.getOrThrow(yield* claimOutput)
			expect(recovered.operationId).toEqual('activity-1')
			expect(recovered.hadAmbiguousAttempt).toEqual(true)
			yield* settleOutput(recovered)
			expect((yield* status(claim)).output[0]).toMatchObject({ state: 'Delivered', attempts: 3, hadAmbiguousAttempt: true })
		}),
	)

	contract(
		'every attempt at an operation sends the idempotency key its first claim gave it',
		Effect.gen(function* () {
			const claim = yield* waitingDelivery
			yield* finish(claim)
			const first = Option.getOrThrow(yield* claimOutputWithKey('key-1'))
			expect(first.idempotencyKey).toEqual('key-1')
			yield* TestClock.adjust(leaseMs)
			const recovered = Option.getOrThrow(yield* claimOutputWithKey('key-2'))
			expect(recovered.hadAmbiguousAttempt).toEqual(true)
			expect(recovered.idempotencyKey).toEqual('key-1')
			const now = yield* Clock.currentTimeMillis
			yield* settleOutput(recovered, DeliveryOutputSettlement.cases.Retry.make({ readyAt: Timestamp.make(now) }))
			const retried = Option.getOrThrow(yield* claimOutputWithKey('key-3'))
			expect(retried.attempt).toEqual(3)
			expect(retried.idempotencyKey).toEqual('key-1')
		}),
	)

	contract(
		'a waiting SetActivity given a new activity gets a new idempotency key; operations saved later get their own',
		Effect.gen(function* () {
			const claim = yield* messageDelivery
			yield* apply(claim, working('a'))
			const first = Option.getOrThrow(yield* claimOutputWithKey('key-1'))
			yield* TestClock.adjust(leaseMs)
			const now = yield* Clock.currentTimeMillis
			const recovered = Option.getOrThrow(yield* claimOutputWithKey('key-2'))
			expect(recovered.idempotencyKey).toEqual(first.idempotencyKey)
			yield* settleOutput(recovered, DeliveryOutputSettlement.cases.Retry.make({ readyAt: Timestamp.make(now) }))
			yield* apply(claim, working('b'))
			const replaced = Option.getOrThrow(yield* claimOutputWithKey('key-3'))
			expect(replaced.operation).toEqual({ _tag: 'SetActivity', activity: { _tag: 'Working', message: 'b' } })
			expect(replaced.hadAmbiguousAttempt).toEqual(true)
			expect(replaced.idempotencyKey).toEqual('key-3')
			yield* settleOutput(replaced)
			yield* apply(claim, createProgress('Summary'))
			expect((yield* sendOutputWithKey('key-4')).idempotencyKey).toEqual('key-4')
		}),
	)

	contract(
		'a result clears the activity: status shows Idle, its outcome asks the provider to clear, and new activity is refused',
		Effect.gen(function* () {
			const claim = yield* messageDelivery
			yield* apply(claim, working('Running tests'))
			yield* sendOutput
			yield* finish(claim)
			expect((yield* status(claim)).activity).toEqual(DeliveryActivity.cases.Idle.make({}))
			expect((yield* apply(claim, working('Running tests'))).status).toEqual('already_recorded')
			expect((yield* apply(claim, working('More')).pipe(Effect.flip))._tag).toEqual('DeliveryClosed')
			const outcome = yield* sendOutput
			expect(outcome.operation._tag).toEqual('PresentOutcome')
			expect(outcome.clearActivity).toEqual(true)
			expect((yield* status(claim)).stage).toEqual('Retired')
			expect((yield* apply(claim, idle).pipe(Effect.flip))._tag).toEqual('DeliveryClosed')
		}),
	)

	contract(
		'a result after Idle, or with no activity, has nothing to clear',
		Effect.gen(function* () {
			const claim = yield* messageDelivery
			yield* apply(claim, working('a'))
			yield* apply(claim, idle)
			yield* sendOutput
			yield* finish(claim)
			expect((yield* sendOutput).clearActivity).toEqual(false)
		}),
	)

	contract(
		'a result accepted while an earlier output is being sent runs after it, then the delivery retires and the next event runs',
		Effect.gen(function* () {
			const claim = yield* messageDelivery
			yield* apply(claim, createProgress('Summary'))
			const create = Option.getOrThrow(yield* claimOutput)
			yield* deliver('follow-up')
			expect((yield* finish(claim)).status).toEqual('accepted')
			expect((yield* status(claim)).stage).toEqual('Finishing')
			yield* settleOutput(create, postedReceipt('1'))
			expect(yield* findReady).toEqual([OutputReadyMailbox.make({ mailboxKey })])
			const outcome = yield* sendOutput
			expect(outcome.operation._tag).toEqual('PresentOutcome')
			expect((yield* status(claim)).stage).toEqual('Retired')
			expect((yield* findWaiting).waiting.count).toEqual(1)
		}),
	)

	contract(
		'a message or activity accepted while an earlier output is being sent runs after it',
		Effect.gen(function* () {
			const claim = yield* messageDelivery
			yield* apply(claim, working('a'))
			const first = Option.getOrThrow(yield* claimOutput)
			yield* apply(claim, createProgress('Summary'))
			yield* apply(claim, working('b'))
			yield* settleOutput(first)
			expect(yield* findReady).toEqual([OutputReadyMailbox.make({ mailboxKey })])
			const sent = [yield* sendOutput, yield* sendOutput].map(({ operation }) => operation._tag)
			expect(sent).toEqual(['CreateMessage', 'SetActivity'])
			expect(yield* findReady).toEqual([])
		}),
	)
}
