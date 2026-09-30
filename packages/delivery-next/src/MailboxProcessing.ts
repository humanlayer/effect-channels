/**
 * This file defines the mailbox processing service.
 *
 * It is responsible for processing provider events that have been saved in a mailbox
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
	ExternalLink,
	PreparedDeliveryInvocation,
	ProviderDeliveryExecution,
	type DeliveryHandoffUnsupported,
	type HandoffOptions,
} from './DeliveryContext'
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

/** One mailbox that is due now, as reported by `findReadyMailboxes`. */
export const ReadyMailbox = Schema.Union([WaitingMailbox, RecoverableMailbox])
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
	}
>()('@humanlayer/channels-delivery-next/MailboxProcessingBackend') {}

export const MailboxProcessingSummary = Schema.Struct({
	claimed: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
	deferred: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
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

export type MailboxProcessingOptions = {
	/** How many mailboxes one pass works on at the same time. */
	readonly concurrency: number
	readonly maxAttempts?: number
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
		/** Makes each new batch's ID and remote-worker token. */
		const crypto = yield* Crypto.Crypto

		const runClaim = (claim: ClaimedMailboxBatch) =>
			processClaim({ claim, maxAttempts: options.maxAttempts ?? 5, leaseMs: options.leaseMs }).pipe(
				Effect.provideService(MailboxProcessingBackend, processingBackend),
				Effect.provideService(ProviderEventDispatcher, providerEventDispatcher),
			)

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
						Effect.gen(function* () {
							const result = yield* runClaim(claim).pipe(Effect.exit)
							if (Exit.isSuccess(result)) return
							if (Cause.hasInterruptsOnly(result.cause)) return yield* Effect.failCause(result.cause)
							yield* Effect.logError('Mailbox claim processing failed', result.cause).pipe(
								Effect.annotateLogs({ mailbox_key: claim.mailboxKey, claim_id: claim.claimId }),
							)
						}),
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
				})
				if (summary.claimed > 0) {
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
		Effect.map((summary) => summary.claimed > 0),
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
