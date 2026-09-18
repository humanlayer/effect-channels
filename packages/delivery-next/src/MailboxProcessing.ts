/**
 * This file defines the mailbox processing service.
 *
 * It is responsible for processing provider events that have been saved in a mailbox
 *
 */
import { Cause, Clock, Context, Effect, Exit, Layer, Match, Predicate, Schema } from 'effect'

import {
	DeliveryAdmissionBatch,
	processProviderEvent,
	type ProviderEventProcessingError,
	type ProviderEventResult,
	type ProviderEventProcessor,
} from './ProviderEventProcessing'

export const Timestamp = Schema.Finite.pipe(Schema.brand('Timestamp'))
export type Timestamp = typeof Timestamp.Type

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
 * that underlays storage-agnostic processing
 */
export class MailboxProcessingBackend extends Context.Service<
	MailboxProcessingBackend,
	{
		/**
		 * Discover and atomically freeze work that is ready now.
		 * SQL / Redis may return claims from several mailxboes, DO returns zero or one
		 * */
		readonly claimReadyMailboxes: Effect.Effect<ReadonlyArray<ClaimedMailboxBatch>, MailboxProcessingUnavailable>

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
 */
export type ProcessClaimInput = {
	readonly claim: ClaimedMailboxBatch
	readonly maxAttempts: number
}

export const processClaim = Effect.fn('delivery.process_mailbox_claim')(function* (input: ProcessClaimInput) {
	const { claim, maxAttempts } = input
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

	const providerResult = yield* providerEventDispatcher.process(claim.admissions).pipe(
		Effect.match({
			onSuccess: providerSuccessToAttemptResult,
			onFailure: providerFailureToAttemptResult,
		}),
	)
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

/** Construct the storage-agnostic mailbox processing service. */
export const makeMailboxProcessing = (options: { concurrency: number; maxAttempts?: number }) =>
	Effect.gen(function* () {
		const processingBackend = yield* MailboxProcessingBackend
		const providerEventDispatcher = yield* ProviderEventDispatcher

		const runClaim = (claim: ClaimedMailboxBatch) =>
			processClaim({ claim, maxAttempts: options.maxAttempts ?? 5 }).pipe(
				Effect.provideService(MailboxProcessingBackend, processingBackend),
				Effect.provideService(ProviderEventDispatcher, providerEventDispatcher),
			)

		return MailboxProcessing.of({
			processReady: Effect.gen(function* () {
				const claims = yield* processingBackend.claimReadyMailboxes
				if (claims.length > 0) {
					yield* Effect.logInfo('Mailbox processing claimed ready work').pipe(
						Effect.annotateLogs({ claimed: claims.length }),
					)
				}
				yield* Effect.forEach(
					claims,
					(claim) =>
						Effect.gen(function* () {
							const result = yield* runClaim(claim).pipe(Effect.exit)
							if (Exit.isSuccess(result)) return
							if (Cause.hasInterruptsOnly(result.cause)) return yield* Effect.failCause(result.cause)
							yield* Effect.logError('Mailbox claim processing failed', result.cause).pipe(
								Effect.annotateLogs({ mailbox_key: claim.mailboxKey, claim_id: claim.claimId }),
							)
						}),
					{ concurrency: options.concurrency, discard: true },
				)
				return MailboxProcessingSummary.make({ claimed: claims.length })
			}).pipe(Effect.withSpan('delivery.process_ready')),
		})
	})

/** Constructor for the live layer that actually wires all mailbox processing. */
export const MailboxProcessingLive = (options: { concurrency: number; maxAttempts?: number }) =>
	Layer.effect(MailboxProcessing, makeMailboxProcessing(options))
