/**
 * The SQL store behind delivery-next's MailboxProcessingBackend.
 *
 * Every write that reads waiting admissions first takes the mailbox row `FOR UPDATE` in its own statement.
 * `deliver` takes the same lock, so under READ COMMITTED the statements that follow see every admission
 * committed before the lock was granted. Folding the lock and the read into one statement would not:
 * a subquery keeps the snapshot from before the lock wait.
 *
 * All times come from Effect's Clock and reach SQL as parameters.
 */
import {
	BatchId,
	ClaimedMailboxBatch,
	DeliveryAccessToken,
	DeliveryAdmission,
	DeliveryAdmissionBatch,
	DeliveryHandoffUnsupported,
	DeliveryPreparationConflict,
	MailboxProcessingAttemptResult,
	MailboxProcessingBackend,
	MailboxProcessingClaimLost,
	MailboxProcessingUnavailable,
	MailboxSequence,
	PreparedDeliveryInvocation,
	RecoverableMailbox,
	Timestamp,
	WaitingMailbox,
	makeDeliveryId,
	type ClaimMailbox,
	type DeferMailbox,
	type HandOffMailboxDelivery,
	type PrepareMailboxDelivery,
	type RecordProcessingAttemptResult,
	type RenewMailboxClaim,
} from '@humanlayer/channels-delivery-next'
import { Array as Arr, Clock, Effect, Layer, Match, Option, Predicate, Random, Result, Schema } from 'effect'
import * as SqlClient from 'effect/unstable/sql/SqlClient'
import * as SqlError from 'effect/unstable/sql/SqlError'

import { migrate } from './MailboxDelivery'

const resultCodec = Schema.fromJsonString(MailboxProcessingAttemptResult)
const preparedCodec = Schema.fromJsonString(PreparedDeliveryInvocation)
const samePreparation = Schema.toEquivalence(PreparedDeliveryInvocation)

const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0))

const readyMailboxRows = Schema.Array(
	Schema.Union([
		Schema.Struct({
			status: Schema.Literal('idle'),
			mailbox_key: Schema.NonEmptyString,
			provider: Schema.NonEmptyString,
			waiting_count: PositiveInt,
			first_sequence: MailboxSequence,
			first_arrived_at: Timestamp,
			last_sequence: MailboxSequence,
			last_arrived_at: Timestamp,
		}),
		Schema.Struct({
			status: Schema.Literals(['active', 'retry']),
			mailbox_key: Schema.NonEmptyString,
		}),
	]),
)

const lockedMailboxRows = Schema.Array(Schema.Struct({ status: Schema.Literals(['idle', 'active', 'retry']) })).check(
	Schema.isMaxLength(1),
)

/**
 * The one claim of a mailbox that is running or waiting for its retry, with its batch.
 * The claims table allows at most one.
 */
const liveClaimRows = Schema.Array(
	Schema.Struct({
		claim_id: Schema.NonEmptyString,
		attempt: PositiveInt,
		batch_id: BatchId,
		access_token: DeliveryAccessToken,
		prepared_json: Schema.NullOr(preparedCodec),
	}),
).check(Schema.isMaxLength(1))

/** The batch a running claim owns. */
const ownedBatchRows = Schema.Array(
	Schema.Struct({ batch_id: BatchId, prepared_json: Schema.NullOr(preparedCodec) }),
).check(Schema.isMaxLength(1))

const admissionRows = Schema.Array(Schema.Struct({ admission_json: Schema.fromJsonString(DeliveryAdmission) }))

const mailboxKeyRows = Schema.Array(Schema.Struct({ mailbox_key: Schema.NonEmptyString })).check(Schema.isMaxLength(1))

const claimIdRows = Schema.Array(Schema.Struct({ claim_id: Schema.NonEmptyString })).check(Schema.isMaxLength(1))

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

const toBatch = (rows: typeof admissionRows.Type) => {
	const [first, ...rest] = rows.map(({ admission_json }) => admission_json)
	return Predicate.isUndefined(first) ? Option.none() : Option.some(DeliveryAdmissionBatch.make([first, ...rest]))
}

const findReadyMailboxes = Effect.fn('delivery.sql.find_ready_mailboxes')(function* (claimLimit: number) {
	const now = yield* Clock.currentTimeMillis
	const sql = (yield* SqlClient.SqlClient).withoutTransforms()
	const rows = yield* Schema.decodeUnknownEffect(readyMailboxRows)(
		yield* sql`SELECT mailbox.status, mailbox.mailbox_key, mailbox.provider,
				waiting.waiting_count, waiting.first_sequence, waiting.first_arrived_at,
				waiting.last_sequence, waiting.last_arrived_at
			FROM delivery_next_mailboxes mailbox
			LEFT JOIN LATERAL (
				SELECT count(*)::double precision AS waiting_count,
					min(sequence_id)::double precision AS first_sequence,
					(array_agg(arrived_at ORDER BY sequence_id ASC))[1] AS first_arrived_at,
					max(sequence_id)::double precision AS last_sequence,
					(array_agg(arrived_at ORDER BY sequence_id DESC))[1] AS last_arrived_at
				FROM delivery_next_admissions admission
				WHERE mailbox.status = 'idle' AND admission.mailbox_key = mailbox.mailbox_key
					AND admission.claim_id IS NULL
			) waiting ON true
			WHERE mailbox.ready_at <= ${now} AND (mailbox.status IN ('active', 'retry') OR waiting.waiting_count > 0)
			ORDER BY mailbox.ready_at, mailbox.mailbox_key LIMIT ${claimLimit}`,
	)
	return rows.map((row) =>
		Match.value(row).pipe(
			Match.discriminatorsExhaustive('status')({
				idle: (idle) =>
					WaitingMailbox.make({
						mailboxKey: idle.mailbox_key,
						provider: idle.provider,
						waiting: {
							count: idle.waiting_count,
							firstSequence: idle.first_sequence,
							firstArrivedAt: idle.first_arrived_at,
							lastSequence: idle.last_sequence,
							lastArrivedAt: idle.last_arrived_at,
						},
					}),
				active: ({ mailbox_key }) => RecoverableMailbox.make({ mailboxKey: mailbox_key }),
				retry: ({ mailbox_key }) => RecoverableMailbox.make({ mailboxKey: mailbox_key }),
			}),
		),
	)
}, unavailable)

type StartClaim = {
	readonly mailboxKey: string
	readonly batchId: BatchId
	readonly claimId: string
	readonly attempt: number
	readonly now: number
	readonly leaseExpiresAt: number
}

/** Insert the claim row. Admissions point at it, so it must exist before they are moved onto it. */
const insertClaim = (input: StartClaim) =>
	Effect.gen(function* () {
		const sql = (yield* SqlClient.SqlClient).withoutTransforms()
		yield* sql`INSERT INTO delivery_next_claims (
				claim_id, mailbox_key, batch_id, attempt, status, lease_expires_at, claimed_at
			) VALUES (
				${input.claimId}, ${input.mailboxKey}, ${input.batchId}, ${input.attempt}, 'active',
				${input.leaseExpiresAt}, ${input.now}
			)`
	})

const activateMailbox = (input: StartClaim) =>
	Effect.gen(function* () {
		const sql = (yield* SqlClient.SqlClient).withoutTransforms()
		yield* sql`UPDATE delivery_next_mailboxes
			SET status = 'active', ready_at = ${input.leaseExpiresAt}
			WHERE mailbox_key = ${input.mailboxKey}`
	})

/**
 * Freeze the waiting admissions at or below `upToSequence` as a new batch, and claim it for attempt 1.
 * Runs under the mailbox lock.
 */
const claimWaitingEvents = (
	input: StartClaim & { readonly upToSequence: number; readonly accessToken: DeliveryAccessToken },
) =>
	Effect.gen(function* () {
		const sql = (yield* SqlClient.SqlClient).withoutTransforms()
		const admissions = toBatch(
			yield* Schema.decodeUnknownEffect(admissionRows)(
				yield* sql`SELECT admission_json FROM delivery_next_admissions
					WHERE mailbox_key = ${input.mailboxKey} AND claim_id IS NULL AND sequence_id <= ${input.upToSequence}
					ORDER BY sequence_id`,
			),
		)
		if (Option.isNone(admissions)) return Option.none()
		yield* sql`INSERT INTO delivery_next_batches (batch_id, mailbox_key, access_token, created_at)
			VALUES (${input.batchId}, ${input.mailboxKey}, ${input.accessToken}, ${input.now})`
		yield* insertClaim(input)
		yield* sql`UPDATE delivery_next_admissions SET claim_id = ${input.claimId}
			WHERE mailbox_key = ${input.mailboxKey} AND claim_id IS NULL AND sequence_id <= ${input.upToSequence}`
		yield* activateMailbox(input)
		return Option.some(
			ClaimedMailboxBatch.make({
				mailboxKey: input.mailboxKey,
				batchId: input.batchId,
				claimId: input.claimId,
				attempt: input.attempt,
				accessToken: input.accessToken,
				admissions: admissions.value,
			}),
		)
	})

/**
 * Open a new claim for the batch frozen on `previousClaimId` and close the old claim.
 * A claim that was waiting for its retry becomes 'retried'; one whose lease ran out becomes 'abandoned'.
 * The old claim is closed first, because a mailbox may have only one live claim.
 * Runs under the mailbox lock.
 */
const claimFrozenBatch = (
	input: StartClaim & {
		readonly previousClaimId: string
		readonly accessToken: DeliveryAccessToken
		readonly prepared: PreparedDeliveryInvocation | null
	},
) =>
	Effect.gen(function* () {
		const sql = (yield* SqlClient.SqlClient).withoutTransforms()
		yield* sql`UPDATE delivery_next_claims
			SET status = CASE status WHEN 'active' THEN 'abandoned' ELSE 'retried' END,
				finished_at = COALESCE(finished_at, ${input.now}::double precision)
			WHERE claim_id = ${input.previousClaimId}`
		yield* insertClaim(input)
		yield* sql`UPDATE delivery_next_admissions SET claim_id = ${input.claimId}
			WHERE claim_id = ${input.previousClaimId}`
		const admissions = toBatch(
			yield* Schema.decodeUnknownEffect(admissionRows)(
				yield* sql`SELECT admission_json FROM delivery_next_admissions
					WHERE claim_id = ${input.claimId} ORDER BY sequence_id`,
			),
		)
		if (Option.isNone(admissions)) {
			return yield* new MailboxProcessingUnavailable({ reason: 'sql_frozen_batch_missing' })
		}
		yield* activateMailbox(input)
		const batch = {
			mailboxKey: input.mailboxKey,
			batchId: input.batchId,
			claimId: input.claimId,
			attempt: input.attempt,
			accessToken: input.accessToken,
			admissions: admissions.value,
		}
		return Option.some(
			Predicate.isNull(input.prepared)
				? ClaimedMailboxBatch.make(batch)
				: ClaimedMailboxBatch.make({ ...batch, prepared: input.prepared }),
		)
	})

const claimMailbox = (input: ClaimMailbox) =>
	Effect.gen(function* () {
		const now = yield* Clock.currentTimeMillis
		const claimId = yield* makeClaimId
		const sql = (yield* SqlClient.SqlClient).withoutTransforms()
		const { mailboxKey } = input
		const leaseExpiresAt = now + input.leaseMs
		return yield* sql.withTransaction(
			Effect.gen(function* () {
				const [mailbox] = yield* Schema.decodeUnknownEffect(lockedMailboxRows)(
					yield* sql`SELECT status FROM delivery_next_mailboxes
					WHERE mailbox_key = ${mailboxKey} AND ready_at <= ${now} FOR UPDATE`,
				)
				if (Predicate.isUndefined(mailbox)) return Option.none()
				const [liveClaim] = yield* Schema.decodeUnknownEffect(liveClaimRows)(
					yield* sql`SELECT claim.claim_id, claim.attempt::double precision AS attempt, claim.batch_id,
						batch.access_token, batch.prepared_json
					FROM delivery_next_claims claim
					LEFT JOIN delivery_next_batches batch ON batch.batch_id = claim.batch_id
					WHERE claim.mailbox_key = ${mailboxKey} AND claim.status IN ('active', 'retry')`,
				)
				return yield* Match.value(input).pipe(
					Match.tagsExhaustive({
						ClaimWaitingEvents: ({ upToSequence, batchId, accessToken }) =>
							mailbox.status === 'idle'
								? claimWaitingEvents({
										mailboxKey,
										batchId,
										accessToken,
										claimId,
										attempt: 1,
										now,
										leaseExpiresAt,
										upToSequence,
									})
								: Effect.succeedNone,
						ClaimFrozenBatch: () =>
							mailbox.status === 'idle' || Predicate.isUndefined(liveClaim)
								? Effect.succeedNone
								: claimFrozenBatch({
										mailboxKey,
										batchId: liveClaim.batch_id,
										accessToken: liveClaim.access_token,
										prepared: liveClaim.prepared_json,
										claimId,
										attempt: liveClaim.attempt + 1,
										now,
										leaseExpiresAt,
										previousClaimId: liveClaim.claim_id,
									}),
					}),
				)
			}),
		)
	}).pipe(
		unavailable,
		Effect.withSpan('delivery.sql.claim_mailbox', {
			attributes: { mailbox_key: input.mailboxKey, claim_kind: input._tag },
		}),
	)

/** Put an idle mailbox off until later. A newer waiting event means delivery already woke it, so the deferral is dropped. */
const deferMailbox = (input: DeferMailbox) =>
	Effect.gen(function* () {
		const sql = (yield* SqlClient.SqlClient).withoutTransforms()
		yield* sql.withTransaction(
			Effect.gen(function* () {
				const locked = yield* Schema.decodeUnknownEffect(mailboxKeyRows)(
					yield* sql`SELECT mailbox_key FROM delivery_next_mailboxes
						WHERE mailbox_key = ${input.mailboxKey} AND status = 'idle' FOR UPDATE`,
				)
				if (Arr.isReadonlyArrayEmpty(locked)) return
				yield* sql`UPDATE delivery_next_mailboxes SET ready_at = ${input.until}
					WHERE mailbox_key = ${input.mailboxKey} AND (
						SELECT max(sequence_id) FROM delivery_next_admissions
						WHERE mailbox_key = ${input.mailboxKey} AND claim_id IS NULL
					) = ${input.lastSequenceSeen}`
			}),
		)
	}).pipe(
		unavailable,
		Effect.withSpan('delivery.sql.defer_mailbox', { attributes: { mailbox_key: input.mailboxKey } }),
	)

const claimLostUnless = (owned: boolean, claim: { readonly mailboxKey: string; readonly claimId: string }) =>
	owned
		? Effect.void
		: Effect.fail(new MailboxProcessingClaimLost({ mailboxKey: claim.mailboxKey, claimId: claim.claimId }))

const renewClaim = (input: RenewMailboxClaim) =>
	Effect.gen(function* () {
		const owned = yield* Effect.gen(function* () {
			const now = yield* Clock.currentTimeMillis
			const sql = (yield* SqlClient.SqlClient).withoutTransforms()
			const leaseExpiresAt = now + input.leaseMs
			return yield* sql.withTransaction(
				Effect.gen(function* () {
					yield* sql`SELECT mailbox_key FROM delivery_next_mailboxes
						WHERE mailbox_key = ${input.mailboxKey} FOR UPDATE`
					const renewed = yield* Schema.decodeUnknownEffect(claimIdRows)(
						yield* sql`UPDATE delivery_next_claims SET lease_expires_at = ${leaseExpiresAt}
							WHERE claim_id = ${input.claimId} AND mailbox_key = ${input.mailboxKey} AND status = 'active'
							RETURNING claim_id`,
					)
					if (Arr.isReadonlyArrayEmpty(renewed)) return false
					yield* sql`UPDATE delivery_next_mailboxes SET ready_at = ${leaseExpiresAt}
						WHERE mailbox_key = ${input.mailboxKey}`
					return true
				}),
			)
		}).pipe(unavailable)
		yield* claimLostUnless(owned, input)
	}).pipe(
		Effect.withSpan('delivery.sql.renew_claim', {
			attributes: { mailbox_key: input.mailboxKey, claim_id: input.claimId },
		}),
	)

/**
 * Close the claim with its result.
 * A retryable failure leaves the claim live as 'retry', so the retry finds the frozen batch through it.
 * Any other result releases the mailbox, and wakes it at once if events arrived while the batch ran.
 */
const recordProcessingAttemptResult = (input: RecordProcessingAttemptResult) =>
	Effect.gen(function* () {
		const { mailboxKey, claimId } = input.claim
		const owned = yield* Effect.gen(function* () {
			const sql = (yield* SqlClient.SqlClient).withoutTransforms()
			const resultJson = yield* Schema.encodeEffect(resultCodec)(input.result)
			const closeClaim = (status: 'retry' | 'completed' | 'failed') =>
				sql`UPDATE delivery_next_claims
					SET status = ${status}, result_json = ${resultJson}, finished_at = ${input.finishedAt}
					WHERE claim_id = ${claimId}`
			const ownedClaim = sql`SELECT claim_id FROM delivery_next_claims
				WHERE claim_id = ${claimId} AND mailbox_key = ${mailboxKey} AND status = 'active'`
			const releaseMailbox = sql`UPDATE delivery_next_mailboxes SET status = 'idle',
				ready_at = CASE WHEN EXISTS (
					SELECT 1 FROM delivery_next_admissions WHERE mailbox_key = ${mailboxKey} AND claim_id IS NULL
				) THEN ${input.finishedAt}::double precision ELSE NULL END
				WHERE mailbox_key = ${mailboxKey}`
			return yield* sql.withTransaction(
				Effect.gen(function* () {
					yield* sql`SELECT mailbox_key FROM delivery_next_mailboxes WHERE mailbox_key = ${mailboxKey} FOR UPDATE`
					const owned = yield* Schema.decodeUnknownEffect(claimIdRows)(yield* ownedClaim)
					if (Arr.isReadonlyArrayEmpty(owned)) return false
					yield* Match.value(input.result).pipe(
						Match.tagsExhaustive({
							RetryableFailure: ({ retryAfterMs }) =>
								closeClaim('retry').pipe(
									Effect.andThen(
										sql`UPDATE delivery_next_mailboxes
											SET status = 'retry', ready_at = ${input.finishedAt + (retryAfterMs ?? 1_000)}
											WHERE mailbox_key = ${mailboxKey}`,
									),
								),
							Completed: () => closeClaim('completed').pipe(Effect.andThen(releaseMailbox)),
							TerminalFailure: () => closeClaim('failed').pipe(Effect.andThen(releaseMailbox)),
						}),
					)
					return true
				}),
			)
		}).pipe(unavailable)
		yield* claimLostUnless(owned, input.claim)
	}).pipe(
		Effect.withSpan('delivery.sql.record_processing_attempt_result', {
			attributes: {
				mailbox_key: input.claim.mailboxKey,
				claim_id: input.claim.claimId,
				result: input.result._tag,
			},
		}),
	)

/**
 * What a prepare does with the batch its claim owns: nothing owned is a lost claim, a saved choice must
 * match, and otherwise the new choice is saved.
 */
const decidePreparation = (
	input: PrepareMailboxDelivery & { readonly owned: (typeof ownedBatchRows.Type)[number] | undefined },
): Result.Result<PreparedDeliveryInvocation, MailboxProcessingClaimLost | DeliveryPreparationConflict> => {
	const { mailboxKey, claimId, owned } = input
	if (Predicate.isUndefined(owned)) return Result.fail(new MailboxProcessingClaimLost({ mailboxKey, claimId }))
	if (Predicate.isNull(owned.prepared_json)) return Result.succeed(input.prepared)
	return samePreparation(owned.prepared_json, input.prepared)
		? Result.succeed(owned.prepared_json)
		: Result.fail(
				new DeliveryPreparationConflict({ deliveryId: makeDeliveryId({ mailboxKey, batchId: owned.batch_id }) }),
			)
}

/**
 * Save the callback choice on the batch the running claim owns, once. The same choice again returns
 * the saved one; a different choice is a conflict and changes nothing.
 */
const prepareDelivery = (input: PrepareMailboxDelivery) =>
	Effect.gen(function* () {
		const { mailboxKey, claimId } = input
		const outcome = yield* Effect.gen(function* () {
			const now = yield* Clock.currentTimeMillis
			const sql = (yield* SqlClient.SqlClient).withoutTransforms()
			const preparedJson = yield* Schema.encodeEffect(preparedCodec)(input.prepared)
			return yield* sql.withTransaction(
				Effect.gen(function* () {
					yield* sql`SELECT mailbox_key FROM delivery_next_mailboxes WHERE mailbox_key = ${mailboxKey} FOR UPDATE`
					const [owned] = yield* Schema.decodeUnknownEffect(ownedBatchRows)(
						yield* sql`SELECT batch.batch_id, batch.prepared_json
							FROM delivery_next_claims claim
							JOIN delivery_next_batches batch ON batch.batch_id = claim.batch_id
							WHERE claim.claim_id = ${claimId} AND claim.mailbox_key = ${mailboxKey}
								AND claim.status = 'active'`,
					)
					const outcome = decidePreparation({ ...input, owned })
					if (Predicate.isUndefined(owned) || Predicate.isNotNull(owned.prepared_json)) return outcome
					yield* sql`UPDATE delivery_next_batches SET prepared_json = ${preparedJson}, prepared_at = ${now}
						WHERE batch_id = ${owned.batch_id}`
					return outcome
				}),
			)
		}).pipe(unavailable)
		return yield* Effect.fromResult(outcome)
	}).pipe(
		Effect.withSpan('delivery.sql.prepare_delivery', {
			attributes: { mailbox_key: input.mailboxKey, claim_id: input.claimId },
		}),
	)

/** Postgres has no remote control yet, so it refuses every handoff. */
const handOffDelivery = (input: HandOffMailboxDelivery) =>
	Effect.fail(new DeliveryHandoffUnsupported()).pipe(
		Effect.withSpan('delivery.sql.hand_off_delivery', {
			attributes: { mailbox_key: input.mailboxKey, claim_id: input.claimId },
		}),
	)

export type MailboxProcessingBackendSqlOptions = {
	/** The most mailboxes one `findReadyMailboxes` reports. */
	readonly claimLimit: number
	readonly runMigrations: boolean
}

export const MailboxProcessingBackendSql = (options: MailboxProcessingBackendSqlOptions) =>
	Layer.effect(
		MailboxProcessingBackend,
		Effect.gen(function* () {
			const sql = yield* SqlClient.SqlClient
			if (options.runMigrations) yield* migrate
			return MailboxProcessingBackend.of({
				findReadyMailboxes: findReadyMailboxes(options.claimLimit).pipe(
					Effect.provideService(SqlClient.SqlClient, sql),
				),
				claimMailbox: (input) => claimMailbox(input).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
				deferMailbox: (input) => deferMailbox(input).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
				renewClaim: (input) => renewClaim(input).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
				recordProcessingAttemptResult: (input) =>
					recordProcessingAttemptResult(input).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
				prepareDelivery: (input) => prepareDelivery(input).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
				handOffDelivery,
			})
		}),
	)
