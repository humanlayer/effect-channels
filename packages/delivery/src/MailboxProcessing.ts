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
	DEFAULT_HANDOFF_FAIL_AFTER,
	DeliveryContext,
	DeliveryHandoff,
	DeliveryHandoffRejected,
	DeliveryHandoffUnavailable,
	DeliveryPreparationConflict,
	DeliveryPreparationUnavailable,
	PreparedDeliveryInvocation,
	PreparedDeliveryCallback,
	ProviderDeliveryExecution,
	type DeliveryHandoffUnsupported,
	type HandoffOptions,
} from './DeliveryContext'
import {
	activityToClear,
	currentPreparedCallback,
	reactionAddedReference,
	renderedDeliveryPlan,
	sentMessageReference,
	type ActiveDelivery,
} from './DeliveryLifecycle'
import { ExternalLink } from './DeliveryLink'
import { ProviderDeleteMessage, ProviderMessageReference, ProviderUpdateMessage } from './DeliveryMessage'
import {
	DeliveryOperationId,
	DeliveryOutputOperation,
	DeliveryOutputSettlement,
	operationMessageId,
	type DeliveryOperation,
} from './DeliveryOperation'
import { ProviderRenderPlan, RenderedDeliveryPlan } from './DeliveryPlan'
import { ProviderReactionTarget, ProviderSetMessageReaction } from './DeliveryReaction'
import {
	BatchId,
	DeliveryAccessToken,
	isRoutableMailboxKey,
	makeBatchId,
	makeConversationId,
	makeDeliveryAccessToken,
	makeDeliveryId,
	makeDeliveryIdempotencyKey,
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
	DeliveryAdmissionBatch,
	processProviderEvent,
	type ProviderEventProcessingError,
	type ProviderEventResult,
	type ProviderEventProcessor,
} from './ProviderEventProcessing'
import {
	processProviderOutput,
	ProviderOutputAttempt,
	ProviderPresentOutcome,
	type ProviderOutputOperation,
	type ProviderOutputProcessor,
} from './ProviderOutput'

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
	callbackIndex: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
	batchId: BatchId,
	claimId: Schema.NonEmptyString,
	attempt: Schema.Int.check(Schema.isGreaterThan(0)),
	accessToken: DeliveryAccessToken,
	admissions: DeliveryAdmissionBatch,
	prepared: Schema.optionalKey(PreparedDeliveryInvocation),
})
export type ClaimedMailboxBatch = typeof ClaimedMailboxBatch.Type

/** The claim a store returns for a batch the lifecycle has just claimed. */
export const toClaimedMailboxBatch = (input: {
	readonly mailboxKey: string
	readonly claimId: string
	readonly active: ActiveDelivery
}) => {
	const { active } = input
	const claimed = {
		mailboxKey: input.mailboxKey,
		batchId: active.batchId,
		callbackIndex: active.callbackIndex,
		claimId: input.claimId,
		attempt: active.attempt,
		accessToken: active.accessToken,
		admissions: active.admissions,
	}
	return ClaimedMailboxBatch.make(
		Predicate.isUndefined(active.prepared) ? claimed : { ...claimed, prepared: active.prepared },
	)
}

/** A handoff's `failAfter` was not a positive duration. A programming error, so it is a defect. */
class HandoffFailAfterInvalid extends Data.TaggedError('HandoffFailAfterInvalid')<{}> {}

/** A handoff's time limit in whole milliseconds; none when it has no limit. */
const handoffFailAfterMs = (failAfter: Duration.Input) =>
	Option.match(Duration.fromInput(failAfter), {
		onNone: () => Effect.die(new HandoffFailAfterInvalid()),
		onSome: (duration) => {
			if (!Duration.isFinite(duration)) return Effect.succeedNone
			const ms = Math.ceil(Duration.toMillis(duration))
			return ms > 0 ? Effect.succeedSome(ms) : Effect.die(new HandoffFailAfterInvalid())
		},
	})

/** Save the callback choice and destination for the batch this claim owns. */
export const PrepareMailboxDelivery = Schema.Struct({
	mailboxKey: Schema.NonEmptyString,
	claimId: Schema.NonEmptyString,
	prepared: PreparedDeliveryInvocation,
	callbackAccessTokens: Schema.NonEmptyArray(DeliveryAccessToken),
})
export type PrepareMailboxDelivery = typeof PrepareMailboxDelivery.Type

/** Hand the batch this claim owns to a remote worker. The claim stays as ownership of callback cleanup. */
export const HandOffMailboxDelivery = Schema.Struct({
	mailboxKey: Schema.NonEmptyString,
	claimId: Schema.NonEmptyString,
	handedOffAt: Timestamp,
	links: Schema.Array(ExternalLink),
	/** How long the remote worker may go without a request before the delivery fails on its own. Absent means no limit. */
	failAfterMs: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThan(0))),
})
export type HandOffMailboxDelivery = typeof HandOffMailboxDelivery.Type

/**
 * Claim the next due output operation of a mailbox's active delivery.
 *
 * @property idempotencyKey - a new random UUID, made by the caller so no store needs a random source.
 * The store gives it to the operation at its first claim, and keeps the operation's own after that.
 */
export const ClaimDeliveryOutput = Schema.Struct({
	mailboxKey: Schema.NonEmptyString,
	leaseMs: LeaseMilliseconds,
	idempotencyKey: Schema.NonEmptyString,
})
export type ClaimDeliveryOutput = typeof ClaimDeliveryOutput.Type

/**
 * One output operation claimed for one attempt.
 *
 * @property claimId - new on every attempt; proves this attempt still owns the operation
 * @property namespace - with `provider`, names the provider that sends the output
 * @property prepared - where the output goes; missing when the provider never prepared the delivery
 * @property clearActivity - the delivery's last activity was `Working`; its `PresentOutcome` must clear it
 * @property messageReference - for an update or deletion, or a reaction on a message, the provider's reference to the message; missing when its create failed
 * @property reactionAddedReference - for a reaction removal, the provider's receipt for the add it undoes, when this delivery made one
 * @property renderedPlan - for `RenderPlan`, the plan the provider last showed, when it has shown one
 * @property hadAmbiguousAttempt - an earlier attempt's lease ran out, so the provider may already have applied it
 * @property idempotencyKey - the operation's key, the same on every attempt at it
 */
export const ClaimedDeliveryOutput = Schema.Struct({
	mailboxKey: Schema.NonEmptyString,
	callbackIndex: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
	batchId: BatchId,
	claimId: Schema.NonEmptyString,
	namespace: Schema.NonEmptyString,
	provider: Schema.NonEmptyString,
	prepared: Schema.optionalKey(PreparedDeliveryCallback),
	operationId: DeliveryOperationId,
	operation: DeliveryOutputOperation,
	messageReference: Schema.optionalKey(ProviderMessageReference),
	reactionAddedReference: Schema.optionalKey(Schema.Json),
	renderedPlan: Schema.optionalKey(RenderedDeliveryPlan),
	clearActivity: Schema.Boolean,
	attempt: Schema.Int.check(Schema.isGreaterThan(0)),
	hadAmbiguousAttempt: Schema.Boolean,
	idempotencyKey: Schema.NonEmptyString,
})
export type ClaimedDeliveryOutput = typeof ClaimedDeliveryOutput.Type

/** The claim a store returns for an operation the lifecycle has just claimed. */
export const toClaimedDeliveryOutput = (input: {
	readonly mailboxKey: string
	readonly claimId: string
	readonly active: ActiveDelivery
	readonly operation: DeliveryOperation
	readonly idempotencyKey: string
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
		callbackIndex: active.callbackIndex,
		claimId: input.claimId,
		namespace: first.namespace,
		provider: first.provider,
		operationId: operation.operationId,
		operation: operation.operation,
		clearActivity: activityToClear(active),
		attempt: operation.attempt,
		hadAmbiguousAttempt: operation.hadAmbiguousAttempt,
		idempotencyKey: input.idempotencyKey,
	}
	const addedReference = reactionAddedReference(active, operation)
	const withMessage = Predicate.isUndefined(messageReference) ? claimed : { ...claimed, messageReference }
	const withReference = Predicate.isUndefined(addedReference)
		? withMessage
		: { ...withMessage, reactionAddedReference: addedReference }
	const renderedPlan = Predicate.isTagged(operation.operation, 'RenderPlan')
		? renderedDeliveryPlan(active)
		: undefined
	const withPlan = Predicate.isUndefined(renderedPlan) ? withReference : { ...withReference, renderedPlan }
	const prepared = currentPreparedCallback(active)
	return ClaimedDeliveryOutput.make(Predicate.isUndefined(prepared) ? withPlan : { ...withPlan, prepared })
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
		readonly settleDeliveryOutput: (
			input: SettleDeliveryOutput,
		) => Effect.Effect<void, MailboxProcessingBackendError>
	}
>()('@humanlayer/channels-delivery/MailboxProcessingBackend') {}

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
 * Mailbox processing is the high level service that sits on top of the mailbox processing backend
 * and the providers' processors.
 *
 * It depends on MailboxProcessingBackend (see above) for storage and claims, and hands each claimed
 * batch, and each output operation, to the processor of the provider that owns it.
 *
 * `R` is what the providers' processors need while they run.
 */
export type MailboxProcessingOperations<R = never> = {
	readonly processReady: Effect.Effect<MailboxProcessingSummary, MailboxProcessingBackendError, R>
}

/** The event and output processors of a bot's providers. `R` is what they need while they run. */
export type MailboxProcessors<R = never> = {
	readonly events: ReadonlyArray<ProviderEventProcessor<R>>
	readonly outputs: ReadonlyArray<ProviderOutputProcessor<R>>
}

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
 * Hand a claimed batch to the processor of the provider that owns it, then record the result.
 *
 * The callback and the lease renewal run side by side, and whichever finishes first stops the other:
 * a finished callback stops the renewal, and a lost claim interrupts the callback.
 *
 * A batch that keeps killing its worker never reports a failure; it only comes back through lease
 * recovery with a higher attempt. Once it is past `maxAttempts` it is failed without running again.
 */
export type ProcessClaimInput<R = never> = {
	readonly claim: ClaimedMailboxBatch
	readonly maxAttempts: number
	readonly leaseMs: number
	readonly processors: ReadonlyArray<ProviderEventProcessor<R>>
}

/**
 * Tell the store "still working" for as long as the callback runs.
 *
 * Renews three times per lease, so one or two renewals may fail before the lease lapses.
 * Never succeeds: it runs until it is interrupted, or fails once the claim belongs to someone else.
 */
const keepClaimLeaseAlive = (input: { readonly claim: ClaimedMailboxBatch; readonly leaseMs: number }) =>
	Effect.gen(function* () {
		const backend = yield* MailboxProcessingBackend
		return yield* Effect.sleep(Duration.millis(Math.max(1, Math.floor(input.leaseMs / 3)))).pipe(
			Effect.andThen(
				backend.renewClaim({
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
		const backend = yield* MailboxProcessingBackend
		const crypto = yield* Crypto.Crypto
		const { claim } = input
		const deliveryId = makeDeliveryId({
			mailboxKey: claim.mailboxKey,
			batchId: claim.batchId,
			callbackIndex: claim.callbackIndex,
		})
		const idempotencyKey = yield* makeDeliveryIdempotencyKey(deliveryId)
		const owner = { mailboxKey: claim.mailboxKey, claimId: claim.claimId }

		const prepare = (prepared: PreparedDeliveryInvocation) =>
			Effect.gen(function* () {
				const remaining = yield* Effect.forEach(prepared.callbacks.slice(1), () =>
					makeDeliveryAccessToken.pipe(Effect.provideService(Crypto.Crypto, crypto)),
				)
				return yield* backend.prepareDelivery({
					...owner,
					prepared,
					callbackAccessTokens: [claim.accessToken, ...remaining],
				})
			}).pipe(
				Effect.tapError((error) =>
					Effect.logWarning('Delivery preparation failed', error).pipe(
						Effect.annotateLogs({ mailbox_key: claim.mailboxKey, claim_id: claim.claimId }),
					),
				),
				Effect.catchTags({
					PlatformError: () =>
						Effect.fail(new DeliveryPreparationUnavailable({ reason: 'random_unavailable' })),
					MailboxProcessingUnavailable: ({ reason }) =>
						Effect.fail(new DeliveryPreparationUnavailable({ reason })),
					MailboxProcessingClaimLost: () => Effect.fail(new DeliveryPreparationConflict({ deliveryId })),
				}),
				Effect.withSpan('delivery.prepare'),
			)

		const handoff = (options?: HandoffOptions) =>
			Effect.gen(function* () {
				const handedOffAt = Timestamp.make(yield* Clock.currentTimeMillis)
				const failAfterMs = yield* handoffFailAfterMs(options?.failAfter ?? DEFAULT_HANDOFF_FAIL_AFTER)
				const request = { ...owner, handedOffAt, links: options?.links ?? [] }
				yield* backend
					.handOffDelivery(
						Option.match(failAfterMs, {
							onNone: () => request,
							onSome: (ms) => ({ ...request, failAfterMs: ms }),
						}),
					)
					.pipe(
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
			callbackIndex: claim.callbackIndex,
			idempotencyKey,
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

export const processClaim = <R>(input: ProcessClaimInput<R>) =>
	Effect.gen(function* () {
		const backend = yield* MailboxProcessingBackend
		const { claim, maxAttempts, leaseMs } = input
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
				processProviderEvent(input.processors)(claim.admissions, execution).pipe(
					Effect.match({
						onSuccess: providerSuccessToAttemptResult,
						onFailure: providerFailureToAttemptResult,
					}),
				),
				keepClaimLeaseAlive({ claim, leaseMs }),
			)
		}).pipe(
			Effect.catchTag('PlatformError', (error) =>
				Effect.logError('Delivery idempotency key could not be made', error).pipe(
					Effect.annotateLogs(claimAnnotations),
					Effect.as(
						MailboxProcessingAttemptRetryableFailure.make({ safeCode: 'idempotency_key_unavailable' }),
					),
				),
			),
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
		yield* backend.recordProcessingAttemptResult({
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
	}).pipe(Effect.withSpan('delivery.process_mailbox_claim'))

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

/** A saved operation that cannot be given to its provider; it fails with `safeCode` without a call. */
class OutputNotSendable extends Data.TaggedError('OutputNotSendable')<{ readonly safeCode: string }> {}

/**
 * The operation as its provider receives it. An update or deletion, or a reaction on a message, takes
 * the provider's reference to its message; there is none when the message's create failed, so it cannot
 * be sent. A reaction removal also takes the receipt of the add it undoes, when there is one. A plan
 * takes the plan the provider last showed.
 */
const providerOperation = (claim: ClaimedDeliveryOutput): Effect.Effect<ProviderOutputOperation, OutputNotSendable> => {
	const messageReference = Effect.fromOption(Option.fromUndefinedOr(claim.messageReference)).pipe(
		Effect.mapError(() => new OutputNotSendable({ safeCode: 'message_not_created' })),
	)
	return Match.value(claim.operation).pipe(
		Match.withReturnType<Effect.Effect<ProviderOutputOperation, OutputNotSendable>>(),
		Match.tagsExhaustive({
			PresentOutcome: (operation) =>
				Effect.succeed(ProviderPresentOutcome.make({ ...operation, clearActivity: claim.clearActivity })),
			RenderPlan: ({ revision, plan }) =>
				Effect.succeed(
					ProviderRenderPlan.make(
						Predicate.isUndefined(claim.renderedPlan)
							? { revision, plan }
							: { revision, plan, rendered: claim.renderedPlan },
					),
				),
			SetActivity: (operation) => Effect.succeed(operation),
			AddExternalLink: (operation) => Effect.succeed(operation),
			CreateMessage: (operation) => Effect.succeed(operation),
			UpdateMessage: ({ messageId, markdown }) =>
				Effect.map(messageReference, (reference) =>
					ProviderUpdateMessage.make({ messageId, markdown, reference }),
				),
			DeleteMessage: ({ messageId }) =>
				Effect.map(messageReference, (reference) => ProviderDeleteMessage.make({ messageId, reference })),
			SetMessageReaction: ({ target, reaction, active }) =>
				Effect.gen(function* () {
					const providerTarget = yield* Match.value(target).pipe(
						Match.withReturnType<Effect.Effect<ProviderReactionTarget, OutputNotSendable>>(),
						Match.tagsExhaustive({
							ActivationTarget: () =>
								Effect.succeed(ProviderReactionTarget.cases.ActivationTarget.make({})),
							MessageTarget: ({ messageId }) =>
								Effect.map(messageReference, (reference) =>
									ProviderReactionTarget.cases.MessageTarget.make({ messageId, reference }),
								),
							PlanTarget: () =>
								Effect.fail(new OutputNotSendable({ safeCode: 'reaction_target_unavailable' })),
						}),
					)
					const request = { target: providerTarget, reaction, active }
					return ProviderSetMessageReaction.make(
						Predicate.isUndefined(claim.reactionAddedReference)
							? request
							: { ...request, addedReference: claim.reactionAddedReference },
					)
				}),
		}),
	)
}

export type ProcessOutputClaimInput<R = never> = {
	readonly claim: ClaimedDeliveryOutput
	readonly maxAttempts: number
	readonly leaseMs: number
	readonly processors: ReadonlyArray<ProviderOutputProcessor<R>>
}

/** Tell the store "still sending" for as long as the provider call runs. See `keepClaimLeaseAlive`. */
const keepOutputLeaseAlive = (input: { readonly claim: ClaimedDeliveryOutput; readonly leaseMs: number }) =>
	Effect.gen(function* () {
		const backend = yield* MailboxProcessingBackend
		return yield* Effect.sleep(Duration.millis(Math.max(1, Math.floor(input.leaseMs / 3)))).pipe(
			Effect.andThen(
				backend.renewDeliveryOutput({
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
export const processOutputClaim = <R>(input: ProcessOutputClaimInput<R>) =>
	Effect.gen(function* () {
		const backend = yield* MailboxProcessingBackend
		const { claim, maxAttempts, leaseMs } = input
		const startedAt = yield* Clock.currentTimeMillis
		const deliveryId = makeDeliveryId({
			mailboxKey: claim.mailboxKey,
			batchId: claim.batchId,
			callbackIndex: claim.callbackIndex,
		})
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

		const sendUnderLease = (prepared: PreparedDeliveryCallback, operation: ProviderOutputOperation) =>
			Effect.raceFirst(
				processProviderOutput(input.processors)({
					namespace: claim.namespace,
					provider: claim.provider,
					attempt: ProviderOutputAttempt.make({
						deliveryId,
						operationId: claim.operationId,
						attempt: claim.attempt,
						hadAmbiguousAttempt: claim.hadAmbiguousAttempt,
						idempotencyKey: claim.idempotencyKey,
						prepared,
						operation,
					}),
				}).pipe(
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
								return DeliveryOutputSettlement.cases.Retry.make({
									readyAt: Timestamp.make(now + waitMs),
								})
							}),
					}),
				),
				keepOutputLeaseAlive({ claim, leaseMs }),
			).pipe(
				Effect.tapError((error) =>
					Effect.logWarning(
						'Delivery output lease was lost while its provider call ran; call interrupted',
						error,
					).pipe(Effect.annotateLogs(annotations)),
				),
			)

		yield* Effect.logInfo('Delivery output started').pipe(Effect.annotateLogs(annotations))
		if (
			Predicate.isTagged(claim.operation, 'PresentOutcome') &&
			Predicate.isTagged(claim.operation.outcome, 'Failed') &&
			claim.operation.outcome.reason === 'TimedOut' &&
			claim.attempt === 1
		) {
			yield* Effect.logWarning(
				'Delivery timed out: its remote worker sent no request within the handoff time limit',
			).pipe(Effect.annotateLogs(annotations))
		}
		const prepared = claim.prepared
		const settlement =
			claim.attempt > maxAttempts
				? failed('attempts_exhausted')
				: Predicate.isUndefined(prepared)
					? failed('destination_missing')
					: yield* providerOperation(claim).pipe(
							Effect.flatMap((operation) => sendUnderLease(prepared, operation)),
							Effect.catchTag('OutputNotSendable', ({ safeCode }) => Effect.succeed(failed(safeCode))),
						)
		const settledAt = Timestamp.make(yield* Clock.currentTimeMillis)
		yield* backend.settleDeliveryOutput({
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
				Effect.logWarning('Delivery output permanently failed').pipe(
					Effect.annotateLogs({ safe_code: safeCode }),
				),
		}).pipe(Effect.annotateLogs(settledAnnotations))
	}).pipe(Effect.withSpan('delivery.process_output_claim'))

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
export const claimOrDeferReadyMailbox = (input: {
	readonly mailbox: ReadyMailbox
	readonly leaseMs: number
	readonly deliveryModeFor: (provider: string) => DeliveryMode
}) =>
	Effect.gen(function* () {
		const mailboxProcessingBackend = yield* MailboxProcessingBackend
		const crypto = yield* Crypto.Crypto
		const { leaseMs } = input
		return yield* Match.value(input.mailbox).pipe(
			Match.tagsExhaustive({
				OutputReadyMailbox: ({ mailboxKey }) =>
					Effect.gen(function* () {
						const idempotencyKey = yield* crypto.randomUUIDv4
						const claimed = yield* mailboxProcessingBackend.claimDeliveryOutput(
							ClaimDeliveryOutput.make({ mailboxKey, leaseMs, idempotencyKey }),
						)
						return Option.match(claimed, {
							onNone: () => ReadyMailboxOutcome.Skipped(),
							onSome: (claim) => ReadyMailboxOutcome.Output({ claim }),
						})
					}).pipe(
						Effect.catchTag('PlatformError', (error) =>
							Effect.logError('Random output idempotency key could not be made', error).pipe(
								Effect.andThen(
									Effect.fail(new MailboxProcessingUnavailable({ reason: 'random_unavailable' })),
								),
							),
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
										ClaimWaitingEvents.make({
											mailboxKey,
											upToSequence,
											leaseMs,
											batchId,
											accessToken,
										}),
									)
									return claimedOrSkipped(claimed)
								}).pipe(
									Effect.catchTag('PlatformError', (error) =>
										Effect.logError('Random batch identity could not be made', error).pipe(
											Effect.andThen(
												Effect.fail(
													new MailboxProcessingUnavailable({ reason: 'random_unavailable' }),
												),
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
	}).pipe(Effect.withSpan('delivery.claim_or_defer_ready_mailbox'))

/** Run one claimed piece of work. A failure is logged here so one mailbox cannot stop the pass. */
const runLogged = <E, R>(
	work: Effect.Effect<void, E, R>,
	annotations: { readonly mailbox_key: string; readonly claim_id: string },
) =>
	Effect.gen(function* () {
		const result = yield* work.pipe(Effect.exit)
		if (Exit.isSuccess(result)) return
		if (Cause.hasInterruptsOnly(result.cause)) return yield* Effect.failCause(result.cause)
		yield* Effect.logError('Mailbox claim processing failed', result.cause).pipe(Effect.annotateLogs(annotations))
	})

/**
 * Each mailbox is claimed right before its callback runs, never ahead of time,
 * so a claim does not sit unrenewed while it waits for a free concurrency slot.
 */
const processReadyMailbox = <R>(
	options: MailboxProcessingOptions,
	processors: MailboxProcessors<R>,
	mailbox: ReadyMailbox,
) =>
	Effect.gen(function* () {
		const outcome = yield* claimOrDeferReadyMailbox({
			mailbox,
			leaseMs: options.leaseMs,
			deliveryModeFor: options.deliveryModeFor,
		}).pipe(
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
				runLogged(
					processClaim({
						claim,
						maxAttempts: options.maxAttempts ?? 5,
						leaseMs: options.leaseMs,
						processors: processors.events,
					}),
					{ mailbox_key: claim.mailboxKey, claim_id: claim.claimId },
				),
			Output: ({ claim }) =>
				runLogged(
					processOutputClaim({
						claim,
						maxAttempts: options.outputMaxAttempts ?? DEFAULT_OUTPUT_MAX_ATTEMPTS,
						leaseMs: options.leaseMs,
						processors: processors.outputs,
					}),
					{ mailbox_key: claim.mailboxKey, claim_id: claim.claimId },
				),
		})
		return outcome
	})

/** One pass: work on every mailbox the store reports as due. */
const processReady = <R>(options: MailboxProcessingOptions, processors: MailboxProcessors<R>) =>
	Effect.gen(function* () {
		const backend = yield* MailboxProcessingBackend
		const ready = yield* backend.findReadyMailboxes
		const outcomes = yield* Effect.forEach(ready, (mailbox) => processReadyMailbox(options, processors, mailbox), {
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
	}).pipe(Effect.withSpan('delivery.process_ready'))

/**
 * Call `processReady` for as long as the enclosing scope lives.
 *
 * A pass that found work is followed by another at once, since more may be waiting behind
 * the store's claim limit. A pass that found nothing waits one interval. A failed pass is
 * logged and the loop carries on; only interruption stops it.
 */
const pollReadyMailboxes = (intervalMs: number) =>
	Effect.gen(function* () {
		const processing = yield* MailboxProcessing
		return yield* processing.processReady.pipe(
			Effect.map((summary) => summary.claimed > 0 || summary.output > 0),
			Effect.catchCauseIf(
				(cause) => !Cause.hasInterruptsOnly(cause),
				(cause) => Effect.logError('Mailbox polling pass failed', cause).pipe(Effect.as(false)),
			),
			Effect.flatMap((foundWork) => (foundWork ? Effect.void : Effect.sleep(Duration.millis(intervalMs)))),
			Effect.forever,
		)
	})

export class MailboxProcessing extends Context.Service<MailboxProcessing, MailboxProcessingOperations>()(
	'@humanlayer/channels-delivery/MailboxProcessing',
) {
	/**
	 * Mailbox processing over the store's backend. What the processors need stays in `processReady`'s
	 * requirements, so a host that has it only while a call runs, such as a Durable Object alarm, can
	 * supply it then.
	 */
	static readonly make = <R>(options: MailboxProcessingOptions, processors: MailboxProcessors<R>) =>
		Effect.gen(function* () {
			const backend = yield* MailboxProcessingBackend
			const crypto = yield* Crypto.Crypto
			return {
				processReady: processReady(options, processors).pipe(
					Effect.provideService(MailboxProcessingBackend, backend),
					Effect.provideService(Crypto.Crypto, crypto),
				),
			} satisfies MailboxProcessingOperations<R>
		})

	/**
	 * Mailbox processing with what the processors need taken when the layer is built. With a polling
	 * interval it also owns the poll loop, which stops when the layer's scope closes.
	 */
	static readonly layer = <R>(options: MailboxProcessingOptions, processors: MailboxProcessors<R>) =>
		Layer.effect(
			MailboxProcessing,
			Effect.gen(function* () {
				const operations = yield* MailboxProcessing.make(options, processors)
				const context = yield* Effect.context<R>()
				const processing = MailboxProcessing.of({
					processReady: operations.processReady.pipe(Effect.provide(context)),
				})
				if (options.polling !== 'disabled') {
					yield* Effect.logInfo('Mailbox polling started').pipe(
						Effect.annotateLogs({ interval_ms: options.polling.intervalMs }),
					)
					yield* pollReadyMailboxes(options.polling.intervalMs).pipe(
						Effect.provideService(MailboxProcessing, processing),
						Effect.forkScoped,
					)
				}
				return processing
			}),
		)
}
