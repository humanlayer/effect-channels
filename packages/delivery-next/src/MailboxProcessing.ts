/**
 * This file defines the mailbox processing service.
 *
 * It is responsible for processing provider events that have been saved in a mailbox, and for sending
 * the provider output a delivery owes, such as its final message. Output has its own claim, lease,
 * and retries, so a provider outage never runs an application callback again.
 *
 */
import {
	Cause,
	Clock,
	Context,
	Crypto,
	Data,
	Duration,
	Effect,
	Exit,
	Layer,
	Match,
	Option,
	Predicate,
	Random,
	Redacted,
	Ref,
	Schema,
} from 'effect'

import {
	DeliveryContext,
	DeliveryHandoff,
	DeliveryHandoffRejected,
	DeliveryHandoffUnavailable,
	DeliveryPreparationConflict,
	DeliveryPreparationUnavailable,
	PreparedDeliveryInvocation,
	ProviderDeliveryExecution,
	type DeliveryHandoffUnsupported,
	type HandoffOptions,
} from './DeliveryContext'
import { ExternalLink } from './DeliveryLink'
import { activityToClear, sentMessageReference, type ActiveDelivery } from './DeliveryLifecycle'
import { ProviderDeleteMessage, ProviderMessageReference, ProviderUpdateMessage } from './DeliveryMessage'
import {
	DeliveryOperationId,
	DeliveryOutputOperation,
	DeliveryOutputSettlement,
	operationMessageId,
	type DeliveryOperation,
} from './DeliveryOperation'
import {
	BatchId,
	DeliveryAccessToken,
	isRoutableMailboxKey,
	makeBatchId,
	makeConversationId,
	makeDeliveryAccessToken,
	makeDeliveryId,
} from './DeliveryReference'

import {
	decideMailboxClaim,
	MailboxClaimDecision,
	MailboxSequence,
	Timestamp,
	WaitingEvents,
	type DeliveryMode,
} from './MailboxPolicy'
import {
	ProviderOutputAttempt,
	ProviderOutputDispatcher,
	ProviderPresentOutcome,
	type ProviderOutputOperation,
} from './ProviderOutput'
import {
	DeliveryAdmissionBatch,
	processProviderEvent,
	type ProviderEventProcessingError,
	type ProviderEventResult,
	type ProviderEventProcessor,
} from './ProviderEventProcessing'

const LeaseMilliseconds = Schema.Int.check(Schema.isGreaterThan(0)).check(
	Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER),
)

/** An idle mailbox with events waiting. The delivery mode decides whether it runs now. */
export const WaitingMailbox = Schema.TaggedStruct('WaitingMailbox', {
	mailboxKey: Schema.NonEmptyString,
	provider: Schema.NonEmptyString,
	waiting: WaitingEvents,
})
export type WaitingMailbox = typeof WaitingMailbox.Type

/**
 * A mailbox whose frozen batch must run again: a retry has come due,
 * or the worker that claimed it stopped renewing its lease.
 */
export const RecoverableMailbox = Schema.TaggedStruct('RecoverableMailbox', {
	mailboxKey: Schema.NonEmptyString,
})
export type RecoverableMailbox = typeof RecoverableMailbox.Type

/** A mailbox whose active delivery has provider output due: a remote worker's result, or a link. */
export const OutputReadyMailbox = Schema.TaggedStruct('OutputReadyMailbox', {
	mailboxKey: Schema.NonEmptyString,
})
export type OutputReadyMailbox = typeof OutputReadyMailbox.Type

/** One mailbox that is due now, as reported by `findReadyMailboxes`. */
export const ReadyMailbox = Schema.Union([WaitingMailbox, RecoverableMailbox, OutputReadyMailbox])
export type ReadyMailbox = typeof ReadyMailbox.Type

/**
 * Claim the waiting events of an idle mailbox, at or below `upToSequence`, as one new frozen batch.
 *
 * @property batchId - the new batch's permanent ID, made by the caller so no store needs a random source
 * @property accessToken - the new batch's remote-worker token, saved with it
 */
export const ClaimWaitingEvents = Schema.TaggedStruct('ClaimWaitingEvents', {
	mailboxKey: Schema.NonEmptyString,
	upToSequence: MailboxSequence,
	leaseMs: LeaseMilliseconds,
	batchId: BatchId,
	accessToken: DeliveryAccessToken,
})

/** Claim the frozen batch of a mailbox that is due for a retry or whose lease ran out. */
export const ClaimFrozenBatch = Schema.TaggedStruct('ClaimFrozenBatch', {
	mailboxKey: Schema.NonEmptyString,
	leaseMs: LeaseMilliseconds,
})

export const ClaimMailbox = Schema.Union([ClaimWaitingEvents, ClaimFrozenBatch])
export type ClaimMailbox = typeof ClaimMailbox.Type

/**
 * "Not yet": look at this idle mailbox again at `until`.
 *
 * @property lastSequenceSeen - the newest waiting event the decision was based on. If a newer
 * event has arrived since, the store ignores the deferral, because delivery already woke the mailbox.
 */
export const DeferMailbox = Schema.Struct({
	mailboxKey: Schema.NonEmptyString,
	until: Timestamp,
	lastSequenceSeen: MailboxSequence,
})
export type DeferMailbox = typeof DeferMailbox.Type

/** "Still working": move the lease of a live claim forward by `leaseMs` from now. */
export const RenewMailboxClaim = Schema.Struct({
	mailboxKey: Schema.NonEmptyString,
	claimId: Schema.NonEmptyString,
	leaseMs: LeaseMilliseconds,
})
export type RenewMailboxClaim = typeof RenewMailboxClaim.Type

/**
 * A batch of things from the mailbox that we successfully claimed for processing from a given mailbox
 *
 * @property batchId - the same on every attempt at this batch
 * @property claimId - new on every attempt; proves this attempt still owns the batch
 * @property accessToken - the batch's remote-worker token, the same on every attempt
 * @property prepared - what an earlier attempt saved about its callback and destination
 */
export const ClaimedMailboxBatch = Schema.Struct({
	mailboxKey: Schema.NonEmptyString,
	batchId: BatchId,
	claimId: Schema.NonEmptyString,
	attempt: Schema.Int.check(Schema.isGreaterThan(0)),
	accessToken: DeliveryAccessToken,
	admissions: DeliveryAdmissionBatch,
	prepared: Schema.optionalKey(PreparedDeliveryInvocation),
})
export type ClaimedMailboxBatch = typeof ClaimedMailboxBatch.Type

/** Save the callback choice and destination for the batch this claim owns. */
export const PrepareMailboxDelivery = Schema.Struct({
	mailboxKey: Schema.NonEmptyString,
	claimId: Schema.NonEmptyString,
	prepared: PreparedDeliveryInvocation,
})
export type PrepareMailboxDelivery = typeof PrepareMailboxDelivery.Type

/** Hand the batch this claim owns to a remote worker. The claim stays as ownership of callback cleanup. */
export const HandOffMailboxDelivery = Schema.Struct({
	mailboxKey: Schema.NonEmptyString,
	claimId: Schema.NonEmptyString,
	handedOffAt: Timestamp,
	links: Schema.Array(ExternalLink),
})
export type HandOffMailboxDelivery = typeof HandOffMailboxDelivery.Type

/** Claim the next due output operation of a mailbox's active delivery. */
export const ClaimDeliveryOutput = Schema.Struct({
	mailboxKey: Schema.NonEmptyString,
	leaseMs: LeaseMilliseconds,
})
export type ClaimDeliveryOutput = typeof ClaimDeliveryOutput.Type

/**
 * One output operation claimed for one attempt.
 *
 * @property claimId - new on every attempt; proves this attempt still owns the operation
 * @property namespace - with `provider`, names the provider that sends the output
 * @property prepared - where the output goes; missing when the provider never prepared the delivery
 * @property clearActivity - the delivery's last activity was `Working`; its `PresentOutcome` must clear it
 * @property messageReference - for an update or deletion, the provider's reference to the message; missing when its create failed
 * @property hadAmbiguousAttempt - an earlier attempt's lease ran out, so the provider may already have applied it
 */
export const ClaimedDeliveryOutput = Schema.Struct({
	mailboxKey: Schema.NonEmptyString,
	batchId: BatchId,
	claimId: Schema.NonEmptyString,
	namespace: Schema.NonEmptyString,
	provider: Schema.NonEmptyString,
	prepared: Schema.optionalKey(PreparedDeliveryInvocation),
	operationId: DeliveryOperationId,
	operation: DeliveryOutputOperation,
	messageReference: Schema.optionalKey(ProviderMessageReference),
	clearActivity: Schema.Boolean,
	attempt: Schema.Int.check(Schema.isGreaterThan(0)),
	hadAmbiguousAttempt: Schema.Boolean,
})
export type ClaimedDeliveryOutput = typeof ClaimedDeliveryOutput.Type

/** The claim a store returns for an operation the lifecycle has just claimed. */
export const toClaimedDeliveryOutput = (input: {
	readonly mailboxKey: string
	readonly claimId: string
	readonly active: ActiveDelivery
	readonly operation: DeliveryOperation
}) => {
	const { active, operation } = input
	const first = active.admissions[0]
	const messageId = operationMessageId(operation.operation)
	const messageReference =
		Predicate.isUndefined(messageId) || Predicate.isTagged(operation.operation, 'CreateMessage')
			? undefined
			: sentMessageReference(active, messageId)
	const claimed = {
		mailboxKey: input.mailboxKey,
		batchId: active.batchId,
		claimId: input.claimId,
		namespace: first.namespace,
		provider: first.provider,
		operationId: operation.operationId,
		operation: operation.operation,
		clearActivity: activityToClear(active),
		attempt: operation.attempt,
		hadAmbiguousAttempt: operation.hadAmbiguousAttempt,
	}
	const withReference = Predicate.isUndefined(messageReference) ? claimed : { ...claimed, messageReference }
	return ClaimedDeliveryOutput.make(
		Predicate.isUndefined(active.prepared) ? withReference : { ...withReference, prepared: active.prepared },
	)
}

/** "Still sending": move the lease of a live output attempt forward by `leaseMs` from now. */
export const RenewDeliveryOutput = Schema.Struct({
	mailboxKey: Schema.NonEmptyString,
	operationId: DeliveryOperationId,
	claimId: Schema.NonEmptyString,
	leaseMs: LeaseMilliseconds,
})
export type RenewDeliveryOutput = typeof RenewDeliveryOutput.Type

/** Record how one output attempt ended. */
export const SettleDeliveryOutput = Schema.Struct({
	mailboxKey: Schema.NonEmptyString,
	operationId: DeliveryOperationId,
	claimId: Schema.NonEmptyString,
	settlement: DeliveryOutputSettlement,
	settledAt: Timestamp,
})
export type SettleDeliveryOutput = typeof SettleDeliveryOutput.Type

/** The result of attempting to process a batch from mailbox - complete (or ignored) / retrable failure / terminal failure */
export const MailboxProcessingAttemptCompleted = Schema.TaggedStruct('Completed', {
	ignoredReason: Schema.optionalKey(Schema.NonEmptyString),
})
export const MailboxProcessingAttemptRetryableFailure = Schema.TaggedStruct('RetryableFailure', {
	safeCode: Schema.NonEmptyString,
	retryAfterMs: Schema.optionalKey(
		Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)).check(Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER)),
	),
})
export const MailboxProcessingAttemptTerminalFailure = Schema.TaggedStruct('TerminalFailure', {
	safeCode: Schema.NonEmptyString,
})
export const MailboxProcessingAttemptResult = Schema.Union([
	MailboxProcessingAttemptCompleted,
	MailboxProcessingAttemptRetryableFailure,
	MailboxProcessingAttemptTerminalFailure,
])

/** We finished processing this batch and are ready to record the result back into the mailbox */
export const RecordProcessingAttemptResult = Schema.Struct({
	claim: ClaimedMailboxBatch,
	result: MailboxProcessingAttemptResult,
	finishedAt: Timestamp,
})
export type RecordProcessingAttemptResult = typeof RecordProcessingAttemptResult.Type

/** Errors  */
export class MailboxProcessingUnavailable extends Schema.TaggedError<MailboxProcessingUnavailable>()(
	'MailboxProcessingUnavailable',
	{ reason: Schema.String },
) {}
export class MailboxProcessingClaimLost extends Schema.TaggedError<MailboxProcessingClaimLost>()(
	'MailboxProcessingClaimLost',
	{
		mailboxKey: Schema.String,
		claimId: Schema.NonEmptyString,
	},
) {}

export type MailboxProcessingBackendError = MailboxProcessingUnavailable | MailboxProcessingClaimLost

/**
 * MailboxProcessingBackend underlays MailboxProcessing - it's the redis/postgres/DO interface
 * that underlays storage-agnostic processing.
 *
 * A backend reports facts and takes exactly what it is told to take. It never decides
 * which events form a batch or when a mailbox runs; `MailboxPolicy` decides that once, for every store.
 */
export class MailboxProcessingBackend extends Context.Service<
	MailboxProcessingBackend,
	{
		/**
		 * Look: report the mailboxes that are due now. A plain read that claims nothing.
		 * SQL / Redis may report several mailboxes, DO reports zero or one
		 */
		readonly findReadyMailboxes: Effect.Effect<ReadonlyArray<ReadyMailbox>, MailboxProcessingUnavailable>

		/**
		 * Take: atomically freeze a batch and start its lease.
		 * Returns none when the mailbox is no longer in the state the caller saw,
		 * for example because another worker claimed it first.
		 */
		readonly claimMailbox: (
			input: ClaimMailbox,
		) => Effect.Effect<Option.Option<ClaimedMailboxBatch>, MailboxProcessingUnavailable>

		/** Wake an idle mailbox later instead of now. */
		readonly deferMailbox: (input: DeferMailbox) => Effect.Effect<void, MailboxProcessingUnavailable>

		/** Move the lease of a live claim forward. Fails with claim lost when the claim is no longer ours. */
		readonly renewClaim: (input: RenewMailboxClaim) => Effect.Effect<void, MailboxProcessingBackendError>

		/**
		 * Record the result of processing ONE frozen claim against the latest mailbox state,
		 * preserving admissions that were received while it ran.
		 *
		 * The delivery's stage decides what happens, not only the result: a handed-off delivery waits
		 * for its remote worker whatever the callback returned, and a delivery with a remote terminal
		 * result retires.
		 */
		readonly recordProcessingAttemptResult: (
			input: RecordProcessingAttemptResult,
		) => Effect.Effect<void, MailboxProcessingBackendError>

		/**
		 * Save the callback choice and destination, once per batch. Returns the saved record, which is
		 * an earlier identical one on replay. Fails with a conflict when a different record is saved.
		 */
		readonly prepareDelivery: (
			input: PrepareMailboxDelivery,
		) => Effect.Effect<PreparedDeliveryInvocation, MailboxProcessingBackendError | DeliveryPreparationConflict>

		/**
		 * Hand the claimed batch off. Repeating it is harmless. Stores without remote control fail
		 * with `DeliveryHandoffUnsupported`.
		 */
		readonly handOffDelivery: (
			input: HandOffMailboxDelivery,
		) => Effect.Effect<void, MailboxProcessingBackendError | DeliveryHandoffUnsupported>

		/**
		 * Take the next due output operation of the mailbox's active delivery under a new lease.
		 * Returns none when nothing is due. Stores without remote control never have output.
		 */
		readonly claimDeliveryOutput: (
			input: ClaimDeliveryOutput,
		) => Effect.Effect<Option.Option<ClaimedDeliveryOutput>, MailboxProcessingUnavailable>

		/** Move the lease of a live output attempt forward. Fails with claim lost when it is no longer ours. */
		readonly renewDeliveryOutput: (input: RenewDeliveryOutput) => Effect.Effect<void, MailboxProcessingBackendError>

		/**
		 * Record how an output attempt ended. A delivery whose result is in and whose output is all
		 * settled retires, and the events waiting behind it become due.
		 */
		readonly settleDeliveryOutput: (input: SettleDeliveryOutput) => Effect.Effect<void, MailboxProcessingBackendError>
	}
>()('@humanlayer/channels-delivery-next/MailboxProcessingBackend') {}

/**
 * @property claimed - batches whose callbacks ran
 * @property output - output operations sent, or tried
 */
export const MailboxProcessingSummary = Schema.Struct({
	claimed: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
	deferred: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
	output: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
})
export type MailboxProcessingSummary = typeof MailboxProcessingSummary.Type

/**
 * Mailbox Processing is the high level service that sits on top of mailbox processing backend
 * and on top of the provider processing
 *
 * It will depend on MailboxProcessingBackend (see above) for the storage and claim stuff
 * and it will depend on ProviderEventDispatcher to handle provider execution of claimed batches
 *
 */

export class MailboxProcessing extends Context.Service<
	MailboxProcessing,
	{
		readonly processReady: Effect.Effect<MailboxProcessingSummary, MailboxProcessingBackendError>
	}
>()('@humanlayer/channels-delivery-next/MailboxProcessing') {}

/** Convert a provider processing success to a mailbox processing success result */
const providerSuccessToAttemptResult = (result: ProviderEventResult) =>
	Match.value(result).pipe(
		Match.tagsExhaustive({
			ProviderEventHandled: () => MailboxProcessingAttemptCompleted.make({}),
			ProviderEventIgnored: ({ reason }) =>
				MailboxProcessingAttemptCompleted.make({
					ignoredReason: reason,
				}),
		}),
	)

/** Convert a provider processing failures to a mailbox processing failures */
const providerFailureToAttemptResult = (error: ProviderEventProcessingError) =>
	Match.value(error).pipe(
		Match.tagsExhaustive({
			ProviderEventProcessorNotFound: () =>
				MailboxProcessingAttemptRetryableFailure.make({
					safeCode: 'processor_not_found',
				}),
			ProviderEventInvalid: ({ reason }) =>
				MailboxProcessingAttemptTerminalFailure.make({
					safeCode: reason,
				}),
			ProviderEventExecutionFailed: ({ retryable, retryAfterMs, safeCode }) =>
				Match.value(retryable).pipe(
					Match.when(true, () =>
						Predicate.isUndefined(retryAfterMs)
							? MailboxProcessingAttemptRetryableFailure.make({ safeCode })
							: MailboxProcessingAttemptRetryableFailure.make({ safeCode, retryAfterMs }),
					),
					Match.when(false, () => MailboxProcessingAttemptTerminalFailure.make({ safeCode })),
					Match.exhaustive,
				),
		}),
	)

/**
 * Given events from the mailbox hand them off for provider processing
 */
export class ProviderEventDispatcher extends Context.Service<
	ProviderEventDispatcher,
	{
		readonly process: (
			admissions: DeliveryAdmissionBatch,
			execution: ProviderDeliveryExecution,
		) => Effect.Effect<ProviderEventResult, ProviderEventProcessingError>
	}
>()('@humanlayer/channels-delivery-next/ProviderEventDispatcher') {}

/**
 * A list of event processors where each entry keeps its own requirements, so processors that need
 * different services can share a list.
 */
export type ProviderEventProcessors<Requirements extends ReadonlyArray<unknown>> = {
	readonly [Index in keyof Requirements]: ProviderEventProcessor<Requirements[Index]>
}

/**
 * Constructor for ProviderEventDispatcher live layer which accepts a set of processors
 * BUT this lets the MailboxProcessing layer stub/mock this out and not have to worry about the providers
 */
export const ProviderEventDispatcherLive = <const Requirements extends ReadonlyArray<unknown>>(
	processors: ProviderEventProcessors<Requirements>,
) =>
	Layer.effect(
		ProviderEventDispatcher,
		Effect.gen(function* () {
			const processorContext = yield* Effect.context<Requirements[number]>()
			return ProviderEventDispatcher.of({
				process: (admissions, execution) =>
					processProviderEvent<Requirements[number]>(processors)(admissions, execution).pipe(
						Effect.provide(processorContext),
					),
			})
		}),
	)

/**
 * given a set of claims from a mailbox, hand off to thte provider event dispatcher for procesisng
 * Then record the processing result. This will be used in the parent effect
 *
 * The callback and the lease renewal run side by side, and whichever finishes first stops the other:
 * a finished callback stops the renewal, and a lost claim interrupts the callback.
 *
 * A batch that keeps killing its worker never reports a failure; it only comes back through lease
 * recovery with a higher attempt. Once it is past `maxAttempts` it is failed without running again.
 */
export type ProcessClaimInput = {
	readonly claim: ClaimedMailboxBatch
	readonly maxAttempts: number
	readonly leaseMs: number
}

/**
 * Tell the store "still working" for as long as the callback runs.
 *
 * Renews three times per lease, so one or two renewals may fail before the lease lapses.
 * Never succeeds: it runs until it is interrupted, or fails once the claim belongs to someone else.
 */
const keepClaimLeaseAlive = (input: { readonly claim: ClaimedMailboxBatch; readonly leaseMs: number }) =>
	Effect.gen(function* () {
		const mailboxProcessingBackend = yield* MailboxProcessingBackend
		return yield* Effect.sleep(Duration.millis(Math.max(1, Math.floor(input.leaseMs / 3)))).pipe(
			Effect.andThen(
				mailboxProcessingBackend.renewClaim({
					mailboxKey: input.claim.mailboxKey,
					claimId: input.claim.claimId,
					leaseMs: input.leaseMs,
				}),
			),
			Effect.catchTag('MailboxProcessingUnavailable', (error) =>
				Effect.logWarning('Mailbox claim lease renewal failed; will try again', error),
			),
			Effect.forever,
		)
	})

/**
 * The execution a provider receives for one attempt: the delivery's identity, what an earlier attempt
 * saved, and the claim-guarded prepare and handoff operations.
 */
const makeProviderDeliveryExecution = (input: {
	readonly claim: ClaimedMailboxBatch
	readonly handedOff: Ref.Ref<boolean>
}) =>
	Effect.gen(function* () {
		const { claim } = input
		const backend = yield* MailboxProcessingBackend
		const deliveryId = makeDeliveryId({ mailboxKey: claim.mailboxKey, batchId: claim.batchId })
		const owner = { mailboxKey: claim.mailboxKey, claimId: claim.claimId }

		const prepare = (prepared: PreparedDeliveryInvocation) =>
			backend.prepareDelivery({ ...owner, prepared }).pipe(
				Effect.tapError((error) =>
					Effect.logWarning('Delivery preparation failed', error).pipe(
						Effect.annotateLogs({ mailbox_key: claim.mailboxKey, claim_id: claim.claimId }),
					),
				),
				Effect.catchTags({
					MailboxProcessingUnavailable: ({ reason }) =>
						Effect.fail(new DeliveryPreparationUnavailable({ reason })),
					MailboxProcessingClaimLost: () => Effect.fail(new DeliveryPreparationConflict({ deliveryId })),
				}),
				Effect.withSpan('delivery.prepare'),
			)

		const handoff = (options?: HandoffOptions) =>
			Effect.gen(function* () {
				const handedOffAt = Timestamp.make(yield* Clock.currentTimeMillis)
				yield* backend.handOffDelivery({ ...owner, handedOffAt, links: options?.links ?? [] }).pipe(
					Effect.tapError((error) =>
						Effect.logWarning('Delivery handoff failed', error).pipe(
							Effect.annotateLogs({ mailbox_key: claim.mailboxKey, claim_id: claim.claimId }),
						),
					),
					Effect.catchTags({
						MailboxProcessingUnavailable: ({ reason }) =>
							Effect.fail(new DeliveryHandoffUnavailable({ reason })),
						MailboxProcessingClaimLost: () => Effect.fail(new DeliveryHandoffRejected({ deliveryId })),
					}),
				)
				yield* Ref.set(input.handedOff, true)
				return DeliveryHandoff.make({ deliveryId })
			}).pipe(Effect.withSpan('delivery.handoff'))

		return new ProviderDeliveryExecution({
			deliveryId,
			prepared: Option.fromUndefinedOr(claim.prepared),
			prepare,
			context: new DeliveryContext({
				deliveryId,
				conversationId: makeConversationId(claim.mailboxKey),
				accessToken: Redacted.make(claim.accessToken),
				handoff,
			}),
		})
	})

export const processClaim = Effect.fn('delivery.process_mailbox_claim')(function* (input: ProcessClaimInput) {
	const { claim, maxAttempts, leaseMs } = input
	const mailboxProcessingBackend = yield* MailboxProcessingBackend
	const providerEventDispatcher = yield* ProviderEventDispatcher
	const startedAt = yield* Clock.currentTimeMillis
	const claimAnnotations = {
		mailbox_key: claim.mailboxKey,
		batch_id: claim.batchId,
		claim_id: claim.claimId,
		attempt: claim.attempt,
		event_count: claim.admissions.length,
		provider: claim.admissions[0].provider,
	}

	yield* Effect.logInfo('Mailbox processing started').pipe(Effect.annotateLogs(claimAnnotations))

	const handedOff = yield* Ref.make(false)

	const runCallbackUnderLease = Effect.gen(function* () {
		const execution = yield* makeProviderDeliveryExecution({ claim, handedOff })
		return yield* Effect.raceFirst(
			providerEventDispatcher.process(claim.admissions, execution).pipe(
				Effect.match({
					onSuccess: providerSuccessToAttemptResult,
					onFailure: providerFailureToAttemptResult,
				}),
			),
			keepClaimLeaseAlive({ claim, leaseMs }),
		)
	}).pipe(
		Effect.tapError((error) =>
			Effect.logWarning('Mailbox claim was lost while its callback ran; callback interrupted', error).pipe(
				Effect.annotateLogs(claimAnnotations),
			),
		),
	)
	const providerResult =
		claim.attempt > maxAttempts
			? MailboxProcessingAttemptTerminalFailure.make({ safeCode: 'attempts_exhausted' })
			: !isRoutableMailboxKey(claim.mailboxKey)
				? MailboxProcessingAttemptTerminalFailure.make({ safeCode: 'mailbox_key_unroutable' })
				: yield* runCallbackUnderLease
	const processingResult = Match.value(providerResult).pipe(
		Match.tag('RetryableFailure', (result) =>
			claim.attempt >= maxAttempts
				? MailboxProcessingAttemptTerminalFailure.make({ safeCode: 'attempts_exhausted' })
				: result,
		),
		Match.orElse((result) => result),
	)
	const processingFinishedAt = yield* Clock.currentTimeMillis
	yield* mailboxProcessingBackend.recordProcessingAttemptResult({
		claim,
		result: processingResult,
		finishedAt: Timestamp.make(processingFinishedAt),
	})
	const recordedAt = yield* Clock.currentTimeMillis
	const recordedAnnotations = {
		...claimAnnotations,
		result: processingResult._tag,
		duration_ms: recordedAt - startedAt,
	}
	/** After a handoff the store waits for the remote worker, whatever the callback returned. */
	if (yield* Ref.get(handedOff)) {
		return yield* Effect.logInfo('Mailbox delivery handed off; waiting for its remote worker').pipe(
			Effect.annotateLogs({ ...recordedAnnotations, handed_off: true }),
		)
	}
	yield* Match.value(processingResult).pipe(
		Match.tagsExhaustive({
			Completed: ({ ignoredReason }) => {
				const log = Effect.logInfo('Mailbox processing completed')
				return Predicate.isUndefined(ignoredReason)
					? log
					: log.pipe(Effect.annotateLogs({ ignored_reason: ignoredReason }))
			},
			RetryableFailure: ({ retryAfterMs, safeCode }) => {
				const log = Effect.logWarning('Mailbox processing scheduled for retry').pipe(
					Effect.annotateLogs({ safe_code: safeCode }),
				)
				return Predicate.isUndefined(retryAfterMs)
					? log
					: log.pipe(Effect.annotateLogs({ retry_after_ms: retryAfterMs }))
			},
			TerminalFailure: ({ safeCode }) =>
				Effect.logWarning('Mailbox processing permanently failed').pipe(
					Effect.annotateLogs({ safe_code: safeCode }),
				),
		}),
		Effect.annotateLogs(recordedAnnotations),
	)
})

/** How many times one output operation is tried before it fails for good. */
export const DEFAULT_OUTPUT_MAX_ATTEMPTS = 8

/** The first wait before an output operation is tried again. Each later wait doubles, up to `OUTPUT_RETRY_MAX_MS`. */
export const OUTPUT_RETRY_INITIAL_MS = 1_000
export const OUTPUT_RETRY_MAX_MS = 5 * 60 * 1_000

/** The wait before retry `attempt + 1`: doubling from one second, capped, then spread over its upper half. */
const outputRetryDelayMs = (attempt: number) =>
	Effect.gen(function* () {
		const ceiling = Math.min(OUTPUT_RETRY_INITIAL_MS * 2 ** Math.max(0, attempt - 1), OUTPUT_RETRY_MAX_MS)
		return Math.round(ceiling / 2 + (ceiling / 2) * (yield* Random.next))
	})

/**
 * The operation as its provider receives it. An update or deletion takes the provider's reference to
 * its message; there is none when the message's create failed, so it cannot be sent.
 */
const providerOperation = (claim: ClaimedDeliveryOutput): Option.Option<ProviderOutputOperation> => {
	const reference = Option.fromUndefinedOr(claim.messageReference)
	return Match.value(claim.operation).pipe(
		Match.tagsExhaustive({
			PresentOutcome: (operation) =>
				Option.some<ProviderOutputOperation>(
					ProviderPresentOutcome.make({ ...operation, clearActivity: claim.clearActivity }),
				),
			SetActivity: (operation) => Option.some<ProviderOutputOperation>(operation),
			AddExternalLink: (operation) => Option.some<ProviderOutputOperation>(operation),
			CreateMessage: (operation) => Option.some<ProviderOutputOperation>(operation),
			UpdateMessage: ({ messageId, markdown }) =>
				Option.map(reference, (saved) => ProviderUpdateMessage.make({ messageId, markdown, reference: saved })),
			DeleteMessage: ({ messageId }) =>
				Option.map(reference, (saved) => ProviderDeleteMessage.make({ messageId, reference: saved })),
		}),
	)
}

export type ProcessOutputClaimInput = {
	readonly claim: ClaimedDeliveryOutput
	readonly maxAttempts: number
	readonly leaseMs: number
}

/** Tell the store "still sending" for as long as the provider call runs. See `keepClaimLeaseAlive`. */
const keepOutputLeaseAlive = (input: { readonly claim: ClaimedDeliveryOutput; readonly leaseMs: number }) =>
	Effect.gen(function* () {
		const mailboxProcessingBackend = yield* MailboxProcessingBackend
		return yield* Effect.sleep(Duration.millis(Math.max(1, Math.floor(input.leaseMs / 3)))).pipe(
			Effect.andThen(
				mailboxProcessingBackend.renewDeliveryOutput({
					mailboxKey: input.claim.mailboxKey,
					operationId: input.claim.operationId,
					claimId: input.claim.claimId,
					leaseMs: input.leaseMs,
				}),
			),
			Effect.catchTag('MailboxProcessingUnavailable', (error) =>
				Effect.logWarning('Delivery output lease renewal failed; will try again', error),
			),
			Effect.forever,
		)
	})

/**
 * Send one claimed output operation through its provider, then record how it went.
 *
 * The provider call and the lease renewal race, as for callbacks. A retryable failure is tried again
 * after a doubling wait, until `maxAttempts`; anything else fails the operation for good. Either way
 * the delivery's result stands: a failed output never changes `completed` to `failed`.
 */
export const processOutputClaim = Effect.fn('delivery.process_output_claim')(function* (input: ProcessOutputClaimInput) {
	const { claim, maxAttempts, leaseMs } = input
	const mailboxProcessingBackend = yield* MailboxProcessingBackend
	const providerOutputDispatcher = yield* ProviderOutputDispatcher
	const startedAt = yield* Clock.currentTimeMillis
	const deliveryId = makeDeliveryId({ mailboxKey: claim.mailboxKey, batchId: claim.batchId })
	const annotations = {
		mailbox_key: claim.mailboxKey,
		batch_id: claim.batchId,
		delivery_id: deliveryId,
		operation_id: claim.operationId,
		operation: claim.operation._tag,
		attempt: claim.attempt,
		had_ambiguous_attempt: claim.hadAmbiguousAttempt,
		provider: claim.provider,
	}
	const failed = (safeCode: string) => DeliveryOutputSettlement.cases.Failed.make({ safeCode })

	const sendUnderLease = (prepared: PreparedDeliveryInvocation, operation: ProviderOutputOperation) =>
		Effect.raceFirst(
			providerOutputDispatcher
				.process({
					namespace: claim.namespace,
					provider: claim.provider,
					attempt: ProviderOutputAttempt.make({
						deliveryId,
						operationId: claim.operationId,
						attempt: claim.attempt,
						hadAmbiguousAttempt: claim.hadAmbiguousAttempt,
						prepared,
						operation,
					}),
				})
				.pipe(
					Effect.map(({ receipt }) =>
						DeliveryOutputSettlement.cases.Applied.make(Predicate.isUndefined(receipt) ? {} : { receipt }),
					),
					Effect.catchTags({
						ProviderOutputProcessorNotFound: () => Effect.succeed(failed('output_processor_not_found')),
						DeliveryOutputFailed: ({ retryable, retryAfterMs, safeCode }) =>
							Effect.gen(function* () {
								if (!retryable) return failed(safeCode)
								if (claim.attempt >= maxAttempts) return failed('attempts_exhausted')
								const now = yield* Clock.currentTimeMillis
								const waitMs = retryAfterMs ?? (yield* outputRetryDelayMs(claim.attempt))
								return DeliveryOutputSettlement.cases.Retry.make({ readyAt: Timestamp.make(now + waitMs) })
							}),
					}),
				),
			keepOutputLeaseAlive({ claim, leaseMs }),
		).pipe(
			Effect.tapError((error) =>
				Effect.logWarning('Delivery output lease was lost while its provider call ran; call interrupted', error).pipe(
					Effect.annotateLogs(annotations),
				),
			),
		)

	yield* Effect.logInfo('Delivery output started').pipe(Effect.annotateLogs(annotations))
	const operation = providerOperation(claim)
	const settlement =
		claim.attempt > maxAttempts
			? failed('attempts_exhausted')
			: Predicate.isUndefined(claim.prepared)
				? failed('destination_missing')
				: Option.isNone(operation)
					? failed('message_not_created')
					: yield* sendUnderLease(claim.prepared, operation.value)
	const settledAt = Timestamp.make(yield* Clock.currentTimeMillis)
	yield* mailboxProcessingBackend.settleDeliveryOutput({
		mailboxKey: claim.mailboxKey,
		operationId: claim.operationId,
		claimId: claim.claimId,
		settlement,
		settledAt,
	})
	const settledAnnotations = { ...annotations, result: settlement._tag, duration_ms: settledAt - startedAt }
	yield* DeliveryOutputSettlement.match(settlement, {
		Applied: () => Effect.logInfo('Delivery output applied'),
		Retry: ({ readyAt }) =>
			Effect.logWarning('Delivery output scheduled for retry').pipe(
				Effect.annotateLogs({ retry_after_ms: readyAt - settledAt }),
			),
		Failed: ({ safeCode }) =>
			Effect.logWarning('Delivery output permanently failed').pipe(Effect.annotateLogs({ safe_code: safeCode })),
	}).pipe(Effect.annotateLogs(settledAnnotations))
})

export type MailboxProcessingOptions = {
	/** How many mailboxes one pass works on at the same time. */
	readonly concurrency: number
	readonly maxAttempts?: number
	/** How many times one output operation is tried. Defaults to `DEFAULT_OUTPUT_MAX_ATTEMPTS`. */
	readonly outputMaxAttempts?: number
	/**
	 * How long a claim stays ours without a renewal. The lease is renewed while the callback runs,
	 * so this bounds how long a crashed worker's batch waits, not how long a callback may take.
	 */
	readonly leaseMs: number
	/** The delivery mode for each provider's mailboxes. */
	readonly deliveryModeFor: (provider: string) => DeliveryMode
	/**
	 * What wakes processing. Stores with no wake-up of their own (SQL, Redis) poll on an interval.
	 * Hosts that wake processing themselves, such as a Durable Object alarm, disable polling.
	 */
	readonly polling: 'disabled' | { readonly intervalMs: number }
}

/** What one pass did with one ready mailbox. */
type ReadyMailboxOutcome = Data.TaggedEnum<{
	Claimed: { readonly claim: ClaimedMailboxBatch }
	Output: { readonly claim: ClaimedDeliveryOutput }
	Deferred: {}
	/** Another worker got there first, or the store could not be reached for this mailbox. */
	Skipped: {}
}>
const ReadyMailboxOutcome = Data.taggedEnum<ReadyMailboxOutcome>()

const claimedOrSkipped = (claimed: Option.Option<ClaimedMailboxBatch>) =>
	Option.match(claimed, {
		onNone: () => ReadyMailboxOutcome.Skipped(),
		onSome: (claim) => ReadyMailboxOutcome.Claimed({ claim }),
	})

/**
 * Look at one ready mailbox and either take a batch from it or put it off until later.
 * A frozen batch is always taken as it is. For waiting events the delivery mode decides.
 */
export const claimOrDeferReadyMailbox = Effect.fn('delivery.claim_or_defer_ready_mailbox')(function* (input: {
	readonly mailbox: ReadyMailbox
	readonly leaseMs: number
	readonly deliveryModeFor: (provider: string) => DeliveryMode
}) {
	const mailboxProcessingBackend = yield* MailboxProcessingBackend
	const { leaseMs } = input
	return yield* Match.value(input.mailbox).pipe(
		Match.tagsExhaustive({
			OutputReadyMailbox: ({ mailboxKey }) =>
				mailboxProcessingBackend.claimDeliveryOutput(ClaimDeliveryOutput.make({ mailboxKey, leaseMs })).pipe(
					Effect.map(
						Option.match({
							onNone: () => ReadyMailboxOutcome.Skipped(),
							onSome: (claim) => ReadyMailboxOutcome.Output({ claim }),
						}),
					),
				),
			RecoverableMailbox: ({ mailboxKey }) =>
				mailboxProcessingBackend
					.claimMailbox(ClaimFrozenBatch.make({ mailboxKey, leaseMs }))
					.pipe(Effect.map(claimedOrSkipped)),
			WaitingMailbox: ({ mailboxKey, provider, waiting }) =>
				Effect.gen(function* () {
					const now = Timestamp.make(yield* Clock.currentTimeMillis)
					const decision = decideMailboxClaim({ waiting, mode: input.deliveryModeFor(provider), now })
					return yield* MailboxClaimDecision.$match(decision, {
						ClaimUpTo: ({ upToSequence }) =>
							Effect.gen(function* () {
								const batchId = yield* makeBatchId
								const accessToken = yield* makeDeliveryAccessToken
								const claimed = yield* mailboxProcessingBackend.claimMailbox(
									ClaimWaitingEvents.make({ mailboxKey, upToSequence, leaseMs, batchId, accessToken }),
								)
								return claimedOrSkipped(claimed)
							}).pipe(
								Effect.catchTag('PlatformError', (error) =>
									Effect.logError('Random batch identity could not be made', error).pipe(
										Effect.andThen(
											Effect.fail(new MailboxProcessingUnavailable({ reason: 'random_unavailable' })),
										),
									),
								),
							),
						WaitUntil: ({ until }) =>
							mailboxProcessingBackend
								.deferMailbox({ mailboxKey, until, lastSequenceSeen: waiting.lastSequence })
								.pipe(Effect.as(ReadyMailboxOutcome.Deferred())),
					})
				}),
		}),
	)
})

/** Construct the storage-agnostic mailbox processing service. */
export const makeMailboxProcessing = (options: MailboxProcessingOptions) =>
	Effect.gen(function* () {
		const processingBackend = yield* MailboxProcessingBackend
		const providerEventDispatcher = yield* ProviderEventDispatcher
		const providerOutputDispatcher = yield* ProviderOutputDispatcher
		/** Makes each new batch's ID and remote-worker token. */
		const crypto = yield* Crypto.Crypto

		const runClaim = (claim: ClaimedMailboxBatch) =>
			processClaim({ claim, maxAttempts: options.maxAttempts ?? 5, leaseMs: options.leaseMs }).pipe(
				Effect.provideService(MailboxProcessingBackend, processingBackend),
				Effect.provideService(ProviderEventDispatcher, providerEventDispatcher),
			)

		const runOutput = (claim: ClaimedDeliveryOutput) =>
			processOutputClaim({
				claim,
				maxAttempts: options.outputMaxAttempts ?? DEFAULT_OUTPUT_MAX_ATTEMPTS,
				leaseMs: options.leaseMs,
			}).pipe(
				Effect.provideService(MailboxProcessingBackend, processingBackend),
				Effect.provideService(ProviderOutputDispatcher, providerOutputDispatcher),
			)

		/** Run one claimed piece of work. A failure is logged here so one mailbox cannot stop the pass. */
		const runLogged = <E, R>(
			work: Effect.Effect<void, E, R>,
			annotations: { readonly mailbox_key: string; readonly claim_id: string },
		) =>
			Effect.gen(function* () {
				const result = yield* work.pipe(Effect.exit)
				if (Exit.isSuccess(result)) return
				if (Cause.hasInterruptsOnly(result.cause)) return yield* Effect.failCause(result.cause)
				yield* Effect.logError('Mailbox claim processing failed', result.cause).pipe(
					Effect.annotateLogs(annotations),
				)
			})

		/**
		 * Each mailbox is claimed right before its callback runs, never ahead of time,
		 * so a claim does not sit unrenewed while it waits for a free concurrency slot.
		 */
		const processReadyMailbox = (mailbox: ReadyMailbox) =>
			Effect.gen(function* () {
				const outcome = yield* claimOrDeferReadyMailbox({
					mailbox,
					leaseMs: options.leaseMs,
					deliveryModeFor: options.deliveryModeFor,
				}).pipe(
					Effect.provideService(MailboxProcessingBackend, processingBackend),
					Effect.provideService(Crypto.Crypto, crypto),
					Effect.catchTag('MailboxProcessingUnavailable', (error) =>
						Effect.logError('Mailbox could not be claimed or deferred', error).pipe(
							Effect.annotateLogs({ mailbox_key: mailbox.mailboxKey }),
							Effect.as(ReadyMailboxOutcome.Skipped()),
						),
					),
				)
				yield* ReadyMailboxOutcome.$match(outcome, {
					Deferred: () => Effect.void,
					Skipped: () => Effect.void,
					Claimed: ({ claim }) =>
						runLogged(runClaim(claim), { mailbox_key: claim.mailboxKey, claim_id: claim.claimId }),
					Output: ({ claim }) =>
						runLogged(runOutput(claim), { mailbox_key: claim.mailboxKey, claim_id: claim.claimId }),
				})
				return outcome
			})

		return MailboxProcessing.of({
			processReady: Effect.gen(function* () {
				const ready = yield* processingBackend.findReadyMailboxes
				const outcomes = yield* Effect.forEach(ready, processReadyMailbox, {
					concurrency: options.concurrency,
				})
				const summary = MailboxProcessingSummary.make({
					claimed: outcomes.filter(ReadyMailboxOutcome.$is('Claimed')).length,
					deferred: outcomes.filter(ReadyMailboxOutcome.$is('Deferred')).length,
					output: outcomes.filter(ReadyMailboxOutcome.$is('Output')).length,
				})
				if (summary.claimed > 0 || summary.output > 0) {
					yield* Effect.logInfo('Mailbox processing claimed ready work').pipe(Effect.annotateLogs(summary))
				}
				return summary
			}).pipe(Effect.withSpan('delivery.process_ready')),
		})
	})

/**
 * Call `processReady` for as long as the enclosing scope lives.
 *
 * A pass that found work is followed by another at once, since more may be waiting behind
 * the store's claim limit. A pass that found nothing waits one interval. A failed pass is
 * logged and the loop carries on; only interruption stops it.
 */
const pollReadyMailboxes = (input: {
	readonly processing: typeof MailboxProcessing.Service
	readonly intervalMs: number
}) =>
	input.processing.processReady.pipe(
		Effect.map((summary) => summary.claimed > 0 || summary.output > 0),
		Effect.catchCauseIf(
			(cause) => !Cause.hasInterruptsOnly(cause),
			(cause) => Effect.logError('Mailbox polling pass failed', cause).pipe(Effect.as(false)),
		),
		Effect.flatMap((foundWork) => (foundWork ? Effect.void : Effect.sleep(Duration.millis(input.intervalMs)))),
		Effect.forever,
	)

/**
 * Constructor for the live layer that actually wires all mailbox processing.
 * With a polling interval it also owns the poll loop, which stops when the layer's scope closes.
 */
export const MailboxProcessingLive = (options: MailboxProcessingOptions) =>
	Layer.effect(
		MailboxProcessing,
		Effect.gen(function* () {
			const processing = yield* makeMailboxProcessing(options)
			if (options.polling !== 'disabled') {
				yield* Effect.logInfo('Mailbox polling started').pipe(
					Effect.annotateLogs({ interval_ms: options.polling.intervalMs }),
				)
				yield* pollReadyMailboxes({ processing, intervalMs: options.polling.intervalMs }).pipe(
					Effect.forkScoped,
				)
			}
			return processing
		}),
	)
