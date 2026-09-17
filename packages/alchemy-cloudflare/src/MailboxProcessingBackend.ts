import {
	ClaimedMailboxBatch,
	DeliveryAdmissionBatch,
	MailboxProcessingBackend,
	MailboxProcessingClaimLost,
	MailboxProcessingUnavailable,
	Timestamp,
	type RecordProcessingAttemptResult,
} from '@humanlayer/channels-delivery-next'
import * as Cloudflare from 'alchemy/Cloudflare'
import { RuntimeContext } from 'alchemy/RuntimeContext'
import { Clock, Effect, Exit, Layer, Match, Predicate, Random, Schema } from 'effect'

import { DurableMailboxState, mailboxStateKey } from './mailboxState'

const makeClaimId = Effect.gen(function* () {
	const now = yield* Clock.currentTimeMillis
	return `${now}-${Math.abs(yield* Random.nextInt)}`
})

const processingUnavailable = (reason: string) => new MailboxProcessingUnavailable({ reason })

export type MailboxProcessingBackendFromDurableObjectStorageOptions = {
	readonly recoveryAfterMs: number
}

/** Builds a mailbox-processing backend over the current Durable Object's persistent storage. */
export const makeMailboxProcessingBackendFromDurableObjectStorage = (
	options: MailboxProcessingBackendFromDurableObjectStorageOptions,
) =>
	Effect.gen(function* () {
		const durableObject = yield* Cloudflare.DurableObjectState
		const runtimeContext = yield* RuntimeContext

		const claimReadyMailboxes = Effect.gen(function* () {
			const now = Timestamp.make(yield* Clock.currentTimeMillis)
			const claimId = yield* makeClaimId
			const result = yield* durableObject.storage.transaction((transaction) =>
				Effect.gen(function* () {
					const stored = yield* transaction.get(mailboxStateKey)
					if (Predicate.isUndefined(stored)) return Exit.succeed([])
					const decoded = yield* Schema.decodeUnknownEffect(DurableMailboxState)(stored).pipe(Effect.exit)
					if (Exit.isFailure(decoded)) return Exit.failCause(decoded.cause)
					const current = decoded.value
					if (Predicate.isNull(current.readyAt) || current.readyAt > now) return Exit.succeed([])
					const pendingFirst = current.pending[0]
					const admissions =
						current.status !== 'idle'
							? current.activeBatch
							: Predicate.isUndefined(pendingFirst)
								? null
								: DeliveryAdmissionBatch.make([pendingFirst, ...current.pending.slice(1)])
					if (Predicate.isNull(admissions)) return Exit.succeed([])
					const attempt = current.status !== 'idle' ? current.attempt + 1 : 1
					const recoveryAt = Timestamp.make(now + options.recoveryAfterMs)
					const next = DurableMailboxState.make({
						...current,
						status: 'active',
						pending: current.status !== 'idle' ? current.pending : [],
						activeBatch: admissions,
						claimId,
						attempt,
						readyAt: recoveryAt,
					})
					yield* transaction.put(mailboxStateKey, next)
					yield* transaction.setAlarm(recoveryAt)
					return Exit.succeed([
						ClaimedMailboxBatch.make({ mailboxKey: current.mailboxKey, claimId, attempt, admissions }),
					])
				}),
			)
			if (Exit.isFailure(result)) return yield* Effect.failCause(result.cause)
			return result.value
		}).pipe(
			Effect.provideService(RuntimeContext, runtimeContext),
			Effect.tapError((error) => Effect.logError('Cloudflare mailbox claim failed', error)),
			Effect.mapError(() => processingUnavailable('cloudflare_unavailable')),
			Effect.catchDefect((defect) =>
				Effect.logError('Cloudflare mailbox claim failed', defect).pipe(
					Effect.andThen(Effect.fail(processingUnavailable('cloudflare_unavailable'))),
				),
			),
			Effect.withSpan('delivery.cloudflare.claim_ready_mailboxes'),
		)

		const recordProcessingAttemptResult = (input: RecordProcessingAttemptResult) =>
			Effect.gen(function* () {
				const result = yield* durableObject.storage.transaction((transaction) =>
					Effect.gen(function* () {
						const stored = yield* transaction.get(mailboxStateKey)
						if (Predicate.isUndefined(stored)) return Exit.succeed(false)
						const decoded = yield* Schema.decodeUnknownEffect(DurableMailboxState)(stored).pipe(Effect.exit)
						if (Exit.isFailure(decoded)) return Exit.failCause(decoded.cause)
						const current = decoded.value
						if (current.status !== 'active' || current.claimId !== input.claim.claimId) {
							return Exit.succeed(false)
						}
						const settlement = Match.value(input.result).pipe(
							Match.tag('RetryableFailure', (attemptResult) => ({
								status: 'retry' as const,
								activeBatch: current.activeBatch,
								attempt: current.attempt,
								readyAt: Timestamp.make(input.finishedAt + (attemptResult.retryAfterMs ?? 1_000)),
							})),
							Match.orElse(() => ({
								status: 'idle' as const,
								activeBatch: null,
								attempt: 0,
								readyAt: current.pending.length > 0 ? input.finishedAt : null,
							})),
						)
						const next = DurableMailboxState.make({
							...current,
							...settlement,
							lastResult: input.result,
							claimId: null,
						})
						yield* transaction.put(mailboxStateKey, next)
						if (Predicate.isNull(settlement.readyAt)) yield* transaction.deleteAlarm()
						else yield* transaction.setAlarm(settlement.readyAt)
						return Exit.succeed(true)
					}),
				)
				if (Exit.isFailure(result)) return yield* Effect.failCause(result.cause)
				const recorded = result.value
				if (!recorded) {
					return yield* new MailboxProcessingClaimLost({
						mailboxKey: input.claim.mailboxKey,
						claimId: input.claim.claimId,
					})
				}
			}).pipe(
				Effect.provideService(RuntimeContext, runtimeContext),
				Effect.tapErrorTag('SchemaError', (error) =>
					Effect.logError('Cloudflare mailbox state decode failed', error),
				),
				Effect.catchTag('SchemaError', () => Effect.fail(processingUnavailable('cloudflare_state_invalid'))),
				Effect.catchDefect((defect) =>
					Effect.logError('Cloudflare mailbox result recording failed', defect).pipe(
						Effect.andThen(Effect.fail(processingUnavailable('cloudflare_unavailable'))),
					),
				),
				Effect.withSpan('delivery.cloudflare.record_processing_attempt_result'),
			)

		return MailboxProcessingBackend.of({ claimReadyMailboxes, recordProcessingAttemptResult })
	})

export const MailboxProcessingBackendFromDurableObjectStorage = (
	options: MailboxProcessingBackendFromDurableObjectStorageOptions,
) => Layer.effect(MailboxProcessingBackend, makeMailboxProcessingBackendFromDurableObjectStorage(options))
