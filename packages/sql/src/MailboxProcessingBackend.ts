import {
	ClaimedMailboxBatch,
	DeliveryAdmission,
	DeliveryAdmissionBatch,
	MailboxProcessingAttemptResult,
	MailboxProcessingBackend,
	MailboxProcessingClaimLost,
	MailboxProcessingUnavailable,
	type RecordProcessingAttemptResult,
} from '@humanlayer/channels-delivery-next'
import { Clock, Effect, Layer, Match, Predicate, Random, Schema } from 'effect'
import * as SqlClient from 'effect/unstable/sql/SqlClient'
import * as SqlError from 'effect/unstable/sql/SqlError'

import { migrate } from './MailboxDelivery'

const batchCodec = Schema.fromJsonString(DeliveryAdmissionBatch)
const resultCodec = Schema.fromJsonString(MailboxProcessingAttemptResult)
const readyMailboxRows = Schema.Array(
	Schema.Struct({
		mailbox_key: Schema.NonEmptyString,
		status: Schema.Literals(['idle', 'active', 'retry']),
		attempt: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
		active_batch_json: Schema.NullOr(batchCodec),
	}),
)
const pendingRows = Schema.Array(Schema.Struct({ admission_json: Schema.fromJsonString(DeliveryAdmission) }))

const unavailable = <A, R>(
	effect: Effect.Effect<A, MailboxProcessingUnavailable | Schema.SchemaError | SqlError.SqlError, R>,
) =>
	effect.pipe(
		Effect.tapError((error) => Effect.logError('SQL mailbox processing failed', error)),
		Effect.catchTags({
			SchemaError: () => Effect.fail(new MailboxProcessingUnavailable({ reason: 'sql_codec_unavailable' })),
			SqlError: () => Effect.fail(new MailboxProcessingUnavailable({ reason: 'sql_unavailable' })),
		}),
	)

const makeClaimId = Effect.gen(function* () {
	const now = yield* Clock.currentTimeMillis
	const random = Math.abs(yield* Random.nextInt)
	return `${now}-${random}`
})

const claimReadyMailboxes = (claimLimit: number, recoveryAfterMs: number) =>
	Effect.gen(function* () {
		const now = yield* Clock.currentTimeMillis
		const sql = (yield* SqlClient.SqlClient).withoutTransforms()
		return yield* sql.withTransaction(
			Effect.gen(function* () {
				const rows = yield* Schema.decodeUnknownEffect(readyMailboxRows)(
					yield* sql`SELECT mailbox_key, status, attempt::double precision AS attempt, active_batch_json
					FROM delivery_next_mailboxes
					WHERE ready_at <= ${now} AND status IN ('idle', 'active', 'retry') AND (
						status IN ('active', 'retry') OR EXISTS (
							SELECT 1 FROM delivery_next_admissions admission
							WHERE admission.mailbox_key = delivery_next_mailboxes.mailbox_key AND admission.consumed = false
						)
					)
					ORDER BY ready_at, mailbox_key LIMIT ${claimLimit} FOR UPDATE SKIP LOCKED`,
				)
				return yield* Effect.forEach(rows, (row) =>
					Effect.gen(function* () {
						const claimId = yield* makeClaimId
						const pending =
							row.status !== 'idle'
								? null
								: yield* Schema.decodeUnknownEffect(pendingRows)(
										yield* sql`SELECT admission_json FROM delivery_next_admissions
											WHERE mailbox_key = ${row.mailbox_key} AND consumed = false ORDER BY sequence_id`,
									)
						const pendingFirst = pending?.[0]
						const admissions =
							row.status !== 'idle'
								? row.active_batch_json
								: Predicate.isUndefined(pendingFirst)
									? null
									: DeliveryAdmissionBatch.make([
											pendingFirst.admission_json,
											...(pending ?? []).slice(1).map(({ admission_json }) => admission_json),
										])
						if (Predicate.isNull(admissions)) {
							return yield* new MailboxProcessingUnavailable({ reason: 'sql_retry_batch_missing' })
						}
						if (row.status === 'idle') {
							yield* sql`UPDATE delivery_next_admissions SET consumed = true
							WHERE mailbox_key = ${row.mailbox_key} AND consumed = false`
						}
						const batchJson = yield* Schema.encodeEffect(batchCodec)(admissions)
						const attempt = row.status !== 'idle' ? row.attempt + 1 : 1
						yield* sql`UPDATE delivery_next_mailboxes SET status = 'active', claim_id = ${claimId},
							attempt = ${attempt}, active_batch_json = ${batchJson}, ready_at = ${now + recoveryAfterMs}
						WHERE mailbox_key = ${row.mailbox_key}`
						return ClaimedMailboxBatch.make({ mailboxKey: row.mailbox_key, claimId, attempt, admissions })
					}),
				)
			}),
		)
	}).pipe(unavailable, Effect.withSpan('delivery.sql.claim_ready_mailboxes'))

const recordProcessingAttemptResult = (input: RecordProcessingAttemptResult) =>
	Effect.gen(function* () {
		const sql = (yield* SqlClient.SqlClient).withoutTransforms()
		const decoded = yield* Effect.gen(function* () {
			const resultJson = yield* Schema.encodeEffect(resultCodec)(input.result)
			const changed = yield* sql.withTransaction(
				Match.value(input.result).pipe(
					Match.tag('RetryableFailure', (result) => {
						const readyAt = input.finishedAt + (result.retryAfterMs ?? 1_000)
						return sql`UPDATE delivery_next_mailboxes SET status = 'retry', claim_id = NULL,
						last_result_json = ${resultJson}, ready_at = ${readyAt}
						WHERE mailbox_key = ${input.claim.mailboxKey} AND status = 'active'
						AND claim_id = ${input.claim.claimId} RETURNING mailbox_key`
					}),
					Match.orElse(
						() => sql`UPDATE delivery_next_mailboxes SET status = 'idle', claim_id = NULL,
					attempt = 0, active_batch_json = NULL, last_result_json = ${resultJson},
					ready_at = CASE WHEN EXISTS (
						SELECT 1 FROM delivery_next_admissions WHERE mailbox_key = ${input.claim.mailboxKey} AND consumed = false
					) THEN ${input.finishedAt} ELSE NULL END
					WHERE mailbox_key = ${input.claim.mailboxKey} AND status = 'active'
					AND claim_id = ${input.claim.claimId} RETURNING mailbox_key`,
					),
				),
			)
			return yield* Schema.decodeUnknownEffect(
				Schema.Array(Schema.Struct({ mailbox_key: Schema.NonEmptyString })).check(Schema.isMaxLength(1)),
			)(changed)
		}).pipe(unavailable)
		if (decoded.length === 0) {
			return yield* new MailboxProcessingClaimLost({
				mailboxKey: input.claim.mailboxKey,
				claimId: input.claim.claimId,
			})
		}
	}).pipe(Effect.withSpan('delivery.sql.record_processing_attempt_result'))

export type MailboxProcessingBackendSqlOptions = {
	readonly claimLimit: number
	readonly recoveryAfterMs: number
	readonly runMigrations: boolean
}

export const MailboxProcessingBackendSql = (options: MailboxProcessingBackendSqlOptions) =>
	Layer.effect(
		MailboxProcessingBackend,
		Effect.gen(function* () {
			const sql = yield* SqlClient.SqlClient
			if (options.runMigrations) yield* migrate
			return MailboxProcessingBackend.of({
				claimReadyMailboxes: claimReadyMailboxes(options.claimLimit, options.recoveryAfterMs).pipe(
					Effect.provideService(SqlClient.SqlClient, sql),
				),
				recordProcessingAttemptResult: (input) =>
					recordProcessingAttemptResult(input).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
			})
		}),
	)
