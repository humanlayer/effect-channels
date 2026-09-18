/**
 * This file defines the mailbox processing service.
 *
 * It is responsible for processing provider events that have been saved in a mailbox
 *
 */
import { Cause, Clock, Context, Data, Duration, Effect, Exit, Layer, Match, Option, Predicate, Schema } from 'effect'

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

/** Claim the waiting events of an idle mailbox, at or below `upToSequence`, as one new frozen batch. */
export const ClaimWaitingEvents = Schema.TaggedStruct('ClaimWaitingEvents', {
	mailboxKey: Schema.NonEmptyString,
	upToSequence: MailboxSequence,
	leaseMs: LeaseMilliseconds,
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

/** A batch of things from the mailbox that we successfully claimed for processing from a given mailbox */
export const ClaimedMailboxBatch = Schema.Struct({
	mailboxKey: Schema.NonEmptyString,
	claimId: Schema.NonEmptyString,
	attempt: Schema.Int.check(Schema.isGreaterThan(0)),
	admissions: DeliveryAdmissionBatch,
})
export type ClaimedMailboxBatch = typeof ClaimedMailboxBatch.Type

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
		 * preserving admissions that were received while it ran
		 */
		readonly recordProcessingAttemptResult: (
			input: RecordProcessingAttemptResult,
		) => Effect.Effect<void, MailboxProcessingBackendError>
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
		) => Effect.Effect<ProviderEventResult, ProviderEventProcessingError>
	}
>()('@humanlayer/channels-delivery-next/ProviderEventDispatcher') {}

/**
 * Constructor for ProviderEventDispatcher live layer which accepts a set of processors
 * BUT this lets the MailboxProcessing layer stub/mock this out and not have to worry about the providers
 */
export const ProviderEventDispatcherLive = <R>(processors: ReadonlyArray<ProviderEventProcessor<R>>) =>
	Layer.effect(
		ProviderEventDispatcher,
		Effect.gen(function* () {
			const processorContext = yield* Effect.context<R>()
			return ProviderEventDispatcher.of({
				process: (admissions) =>
					processProviderEvent(processors)(admissions).pipe(Effect.provide(processorContext)),
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

export const processClaim = Effect.fn('delivery.process_mailbox_claim')(function* (input: ProcessClaimInput) {
	const { claim, maxAttempts, leaseMs } = input
	const mailboxProcessingBackend = yield* MailboxProcessingBackend
	const providerEventDispatcher = yield* ProviderEventDispatcher
	const startedAt = yield* Clock.currentTimeMillis
	const claimAnnotations = {
		mailbox_key: claim.mailboxKey,
		claim_id: claim.claimId,
		attempt: claim.attempt,
		event_count: claim.admissions.length,
		provider: claim.admissions[0].provider,
	}

	yield* Effect.logInfo('Mailbox processing started').pipe(Effect.annotateLogs(claimAnnotations))

	const runCallbackUnderLease = Effect.raceFirst(
		providerEventDispatcher.process(claim.admissions).pipe(
			Effect.match({
				onSuccess: providerSuccessToAttemptResult,
				onFailure: providerFailureToAttemptResult,
			}),
		),
		keepClaimLeaseAlive({ claim, leaseMs }),
	).pipe(
		Effect.tapError((error) =>
			Effect.logWarning('Mailbox claim was lost while its callback ran; callback interrupted', error).pipe(
				Effect.annotateLogs(claimAnnotations),
			),
		),
	)
	const providerResult =
		claim.attempt > maxAttempts
			? MailboxProcessingAttemptTerminalFailure.make({ safeCode: 'attempts_exhausted' })
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
		Effect.annotateLogs({
			...claimAnnotations,
			result: processingResult._tag,
			duration_ms: recordedAt - startedAt,
		}),
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
export const claimOrDeferReadyMailbox = (input: {
	readonly mailbox: ReadyMailbox
	readonly leaseMs: number
	readonly deliveryModeFor: (provider: string) => DeliveryMode
}) =>
	Effect.gen(function* () {
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
								mailboxProcessingBackend
									.claimMailbox(ClaimWaitingEvents.make({ mailboxKey, upToSequence, leaseMs }))
									.pipe(Effect.map(claimedOrSkipped)),
							WaitUntil: ({ until }) =>
								mailboxProcessingBackend
									.deferMailbox({ mailboxKey, until, lastSequenceSeen: waiting.lastSequence })
									.pipe(Effect.as(ReadyMailboxOutcome.Deferred())),
						})
					}),
			}),
		)
	}).pipe(Effect.withSpan('delivery.claim_or_defer_ready_mailbox'))

/** Construct the storage-agnostic mailbox processing service. */
export const makeMailboxProcessing = (options: MailboxProcessingOptions) =>
	Effect.gen(function* () {
		const processingBackend = yield* MailboxProcessingBackend
		const providerEventDispatcher = yield* ProviderEventDispatcher

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
