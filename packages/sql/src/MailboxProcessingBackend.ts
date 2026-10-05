/**
 * The SQL store behind delivery's MailboxProcessingBackend.
 *
 * Every change runs the shared `DeliveryLifecycle` transition inside one transaction, on the mailbox
 * row it locked; see `DeliverySlot.ts` for the pattern and the layout. The claims pollers make take
 * the lock with `SKIP LOCKED`, so two pollers never take the same batch or output operation: the
 * second finds the row busy and moves on. Every other change waits for the lock.
 *
 * All times come from Effect's Clock and reach SQL as parameters.
 */
import {
	DEFAULT_RETRY_AFTER_MS,
	DeliveryAdmissionJson,
	DeliveryPreparationConflict,
	MailboxProcessingAttemptResult,
	MailboxProcessingBackend,
	MailboxProcessingClaimLost,
	MailboxProcessingUnavailable,
	MailboxSequence,
	OutputReadyMailbox,
	RecoverableMailbox,
	Timestamp,
	WaitingEvents,
	WaitingMailbox,
	activeDeliveryWork,
	ActiveDeliveryStage,
	claimDeliveryOutput as claimDeliveryOutputSlot,
	claimFrozenBatch as claimFrozenBatchSlot,
	handOffDeliverySlot,
	makeDeliveryId,
	prepareDeliverySlot,
	recordDeliveryAttempt,
	renewDeliveryClaim,
	renewDeliveryOutput as renewDeliveryOutputSlot,
	settleDeliveryOutput as settleDeliveryOutputSlot,
	startDeliveryBatch,
	toClaimedDeliveryOutput,
	toClaimedMailboxBatch,
	type ClaimDeliveryOutput,
	type ClaimMailbox,
	type ClaimWaitingEvents,
	type DeferMailbox,
	type DeliverySlot,
	type HandOffMailboxDelivery,
	type PrepareMailboxDelivery,
	type RecordProcessingAttemptResult,
	type RenewDeliveryOutput,
	type RenewMailboxClaim,
	type SettleDeliveryOutput,
} from '@humanlayer/channels-delivery'
import { Array as Arr, Clock, Effect, Layer, Match, Option, Predicate, Random, Schema } from 'effect'
import * as SqlClient from 'effect/sql/SqlClient'
import * as SqlError from 'effect/sql/SqlError'

import {
	changeDeliverySlot,
	lockDeliverySlot,
	writeDeliverySlot,
	type LoadedDeliverySlot,
	type NarrowSqlFailure,
} from './DeliverySlot'

const resultCodec = Schema.fromJsonString(MailboxProcessingAttemptResult)

const readyMailboxRows = Schema.Array(
	Schema.Union([
		Schema.Struct({
			status: Schema.Literal('idle'),
			mailbox_key: Schema.NonEmptyString,
			provider: Schema.NonEmptyString,
			waiting_count: WaitingEvents.fields.count,
			first_sequence: MailboxSequence,
			first_arrived_at: Timestamp,
			last_sequence: MailboxSequence,
			last_arrived_at: Timestamp,
		}),
		Schema.Struct({
			status: Schema.Literals(['active', 'retry']),
			mailbox_key: Schema.NonEmptyString,
			stage: ActiveDeliveryStage,
		}),
	]),
)

const mailboxKeyRows = Schema.Array(Schema.Struct({ mailbox_key: Schema.NonEmptyString })).check(Schema.isMaxLength(1))

const waitingAdmissionRows = Schema.Array(Schema.Struct({ admission_json: DeliveryAdmissionJson }))

/** Log a database or codec failure where it happens, then narrow it to `MailboxProcessingUnavailable`. */
const narrowSqlFailure: NarrowSqlFailure<MailboxProcessingUnavailable> = (reason) => (error) =>
	Effect.logError('SQL mailbox processing failed', error).pipe(
		Effect.andThen(Effect.fail(new MailboxProcessingUnavailable({ reason }))),
	)

/** Narrow the failures of an operation that only reads and writes rows. */
const unavailable = <A, R>(effect: Effect.Effect<A, SqlError.SqlError | Schema.SchemaError, R>) =>
	effect.pipe(
		Effect.catchTags({
			SqlError: narrowSqlFailure('sql_unavailable'),
			SchemaError: narrowSqlFailure('sql_codec_unavailable'),
		}),
	)

const makeClaimId = Effect.gen(function* () {
	const now = yield* Clock.currentTimeMillis
	const random = Math.abs(yield* Random.nextInt)
	return `${now}-${random}`
})

const claimLost = (claim: { readonly mailboxKey: string; readonly claimId: string }) =>
	new MailboxProcessingClaimLost({ mailboxKey: claim.mailboxKey, claimId: claim.claimId })

/**
 * Look: every due mailbox. An idle one reports its waiting events; one with an active delivery
 * reports the work its stage leaves: its callback to run again, or output to send.
 */
const findReadyMailboxes = Effect.fn('delivery.sql.find_ready_mailboxes')(function* (claimLimit: number) {
	const now = yield* Clock.currentTimeMillis
	const sql = (yield* SqlClient.SqlClient).withoutTransforms()
	const rows = yield* Schema.decodeUnknownEffect(readyMailboxRows)(
		yield* sql`SELECT mailbox.status, mailbox.mailbox_key, mailbox.provider, batch.stage,
				waiting.waiting_count, waiting.first_sequence, waiting.first_arrived_at,
				waiting.last_sequence, waiting.last_arrived_at
			FROM delivery_next_mailboxes mailbox
			LEFT JOIN delivery_next_batches batch ON batch.batch_id = mailbox.active_batch_id
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
				active: (busy) => busyMailbox(busy),
				retry: (busy) => busyMailbox(busy),
			}),
		),
	)
}, unavailable)

const busyMailbox = (row: { readonly mailbox_key: string; readonly stage: ActiveDeliveryStage }) =>
	activeDeliveryWork(row) === 'Output'
		? OutputReadyMailbox.make({ mailboxKey: row.mailbox_key })
		: RecoverableMailbox.make({ mailboxKey: row.mailbox_key })

/**
 * Freeze the waiting admissions at or below `upToSequence` as a new batch, claimed for attempt 1.
 * Runs under the mailbox lock. The admissions are marked with the batch for good, and with the
 * claim that first took them, which is what tells them apart from waiting ones.
 */
const claimWaitingEvents = (input: {
	readonly loaded: LoadedDeliverySlot
	readonly claim: typeof ClaimWaitingEvents.Type
	readonly claimId: string
	readonly now: number
}) =>
	Effect.gen(function* () {
		const sql = (yield* SqlClient.SqlClient).withoutTransforms()
		const { loaded, claim, claimId, now } = input
		const waiting = yield* Schema.decodeUnknownEffect(waitingAdmissionRows)(
			yield* sql`SELECT admission_json FROM delivery_next_admissions
				WHERE mailbox_key = ${loaded.mailboxKey} AND claim_id IS NULL AND sequence_id <= ${claim.upToSequence}
				ORDER BY sequence_id`,
		)
		if (!Arr.isReadonlyArrayNonEmpty(waiting)) return Option.none()
		const started = startDeliveryBatch(loaded.slot, {
			batchId: claim.batchId,
			accessToken: claim.accessToken,
			admissions: Arr.map(waiting, ({ admission_json }) => admission_json),
			claimId,
			leaseMs: claim.leaseMs,
			now,
		})
		if (Predicate.isNull(started)) return Option.none()
		yield* writeDeliverySlot({ loaded, slot: started.slot, now })
		yield* sql`UPDATE delivery_next_admissions SET claim_id = ${claimId}, batch_id = ${claim.batchId}
			WHERE mailbox_key = ${loaded.mailboxKey} AND claim_id IS NULL AND sequence_id <= ${claim.upToSequence}`
		return Option.some(toClaimedMailboxBatch({ mailboxKey: loaded.mailboxKey, active: started.claimed, claimId }))
	})

/**
 * Take the frozen batch again. The lifecycle may settle the delivery instead of claiming it, such as
 * one whose lease ran out after handoff, so its change is written even when nothing is claimed.
 */
const claimFrozenBatch = (input: {
	readonly loaded: LoadedDeliverySlot
	readonly leaseMs: number
	readonly claimId: string
	readonly now: number
}) =>
	Effect.gen(function* () {
		const { loaded, claimId, now } = input
		const { slot, claimed } = claimFrozenBatchSlot(loaded.slot, {
			claimId,
			leaseMs: input.leaseMs,
			now,
			hasWaiting: loaded.hasWaiting,
		})
		if (slot !== loaded.slot) yield* writeDeliverySlot({ loaded, slot, now })
		return Predicate.isNull(claimed)
			? Option.none()
			: Option.some(toClaimedMailboxBatch({ mailboxKey: loaded.mailboxKey, active: claimed, claimId }))
	})

const claimMailbox = (input: ClaimMailbox) =>
	Effect.gen(function* () {
		const now = yield* Clock.currentTimeMillis
		const claimId = yield* makeClaimId
		const sql = yield* SqlClient.SqlClient
		return yield* sql.withTransaction(
			Effect.gen(function* () {
				const loaded = yield* lockDeliverySlot({
					mailboxKey: input.mailboxKey,
					now,
					lock: 'SkipLocked',
					withRetained: true,
				})
				if (Option.isNone(loaded)) return Option.none()
				const { readyAt } = loaded.value.slot
				if (Predicate.isNull(readyAt) || readyAt > now) return Option.none()
				return yield* Match.value(input).pipe(
					Match.tagsExhaustive({
						ClaimWaitingEvents: (claim) =>
							claimWaitingEvents({ loaded: loaded.value, claim, claimId, now }),
						ClaimFrozenBatch: ({ leaseMs }) =>
							claimFrozenBatch({ loaded: loaded.value, leaseMs, claimId, now }),
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

/**
 * Apply a lifecycle change on behalf of a claim. A missing mailbox, or a change the lifecycle
 * refuses because the claim no longer owns the work, is a lost claim.
 */
const changeClaimedSlot = <A, E = never>(
	claim: { readonly mailboxKey: string; readonly claimId: string },
	input: {
		readonly withRetained: boolean
		readonly change: (
			loaded: LoadedDeliverySlot,
		) => Effect.Effect<{ readonly slot: DeliverySlot; readonly value: A }, E | MailboxProcessingClaimLost>
		readonly beforeWrite?: (
			slot: DeliverySlot,
		) => Effect.Effect<void, SqlError.SqlError | Schema.SchemaError, SqlClient.SqlClient>
	},
) =>
	changeDeliverySlot({
		mailboxKey: claim.mailboxKey,
		withRetained: input.withRetained,
		onMissing: claimLost(claim),
		change: input.change,
		beforeWrite: input.beforeWrite,
		narrowSqlFailure,
	})

const renewClaim = (input: RenewMailboxClaim) =>
	Effect.gen(function* () {
		const now = yield* Clock.currentTimeMillis
		yield* changeClaimedSlot(input, {
			withRetained: false,
			change: ({ slot }) =>
				renewDeliveryClaim(slot, { ...input, now }).pipe(
					Effect.map((renewed) => ({ slot: renewed, value: undefined })),
					Effect.catchTag('ClaimNotOwned', () => Effect.fail(claimLost(input))),
				),
		})
	}).pipe(
		Effect.withSpan('delivery.sql.renew_claim', {
			attributes: { mailbox_key: input.mailboxKey, claim_id: input.claimId },
		}),
	)

/**
 * Record how an attempt ended. The lifecycle decides what follows from the stage, not only the
 * result; the claim row keeps the result as history. An attempt that will be retried stays the
 * mailbox's live claim row until the retry takes over.
 */
const recordProcessingAttemptResult = (input: RecordProcessingAttemptResult) =>
	Effect.gen(function* () {
		const { claim } = input
		const retryAfterMs = Match.value(input.result).pipe(
			Match.tag('RetryableFailure', ({ retryAfterMs }) => retryAfterMs ?? DEFAULT_RETRY_AFTER_MS),
			Match.orElse(() => null),
		)
		const closeClaim = (slot: DeliverySlot) =>
			Effect.gen(function* () {
				const sql = (yield* SqlClient.SqlClient).withoutTransforms()
				const resultJson = yield* Schema.encodeEffect(resultCodec)(input.result)
				const status =
					slot.active?.stage === 'Retry'
						? 'retry'
						: Predicate.isTagged(input.result, 'Completed')
							? 'completed'
							: 'failed'
				yield* sql`UPDATE delivery_next_claims
					SET status = ${status}, result_json = ${resultJson}, finished_at = ${input.finishedAt}
					WHERE claim_id = ${claim.claimId}`
			})
		yield* changeClaimedSlot(claim, {
			withRetained: true,
			change: (loaded) =>
				recordDeliveryAttempt(loaded.slot, {
					claimId: claim.claimId,
					retryAfterMs,
					now: input.finishedAt,
					hasWaiting: loaded.hasWaiting,
				}).pipe(
					Effect.map((slot) => ({ slot, value: undefined })),
					Effect.catchTag('ClaimNotOwned', () => Effect.fail(claimLost(claim))),
				),
			beforeWrite: closeClaim,
		})
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
 * Save the callback choice on the batch the running claim owns, once. The same choice again returns
 * the saved one; a different choice is a conflict and changes nothing.
 */
const prepareDelivery = (input: PrepareMailboxDelivery) =>
	changeClaimedSlot<PrepareMailboxDelivery['prepared'], DeliveryPreparationConflict>(input, {
		withRetained: false,
		change: ({ slot }) =>
			prepareDeliverySlot(slot, input).pipe(
				Effect.map(({ slot: prepared, prepared: saved }) => ({ slot: prepared, value: saved })),
				Effect.catchTags({
					ClaimNotOwned: () => Effect.fail(claimLost(input)),
					PreparationMismatch: ({ batchId }) =>
						Effect.fail(
							new DeliveryPreparationConflict({
								deliveryId: makeDeliveryId({ mailboxKey: input.mailboxKey, batchId }),
							}),
						),
				}),
			),
	}).pipe(
		Effect.withSpan('delivery.sql.prepare_delivery', {
			attributes: { mailbox_key: input.mailboxKey, claim_id: input.claimId },
		}),
	)

/** Hand the batch off. The claim stays as ownership of callback cleanup until its result is recorded. */
const handOffDelivery = (input: HandOffMailboxDelivery) =>
	changeClaimedSlot(input, {
		withRetained: false,
		change: ({ slot }) =>
			handOffDeliverySlot(slot, input).pipe(
				Effect.map((handedOff) => ({ slot: handedOff, value: undefined })),
				Effect.catchTag('ClaimNotOwned', () => Effect.fail(claimLost(input))),
			),
	}).pipe(
		Effect.withSpan('delivery.sql.hand_off_delivery', {
			attributes: { mailbox_key: input.mailboxKey, claim_id: input.claimId },
		}),
	)

/** Take the next due output operation under a new lease. A busy mailbox is skipped, as for batches. */
const claimDeliveryOutput = (input: ClaimDeliveryOutput) =>
	Effect.gen(function* () {
		const now = yield* Clock.currentTimeMillis
		const claimId = yield* makeClaimId
		const sql = yield* SqlClient.SqlClient
		return yield* sql.withTransaction(
			Effect.gen(function* () {
				const loaded = yield* lockDeliverySlot({
					mailboxKey: input.mailboxKey,
					now,
					lock: 'SkipLocked',
					withRetained: false,
				})
				if (Option.isNone(loaded)) return Option.none()
				const { slot, claimed } = claimDeliveryOutputSlot(loaded.value.slot, {
					claimId,
					leaseMs: input.leaseMs,
					now,
					idempotencyKey: input.idempotencyKey,
				})
				if (Predicate.isNull(claimed)) return Option.none()
				yield* writeDeliverySlot({ loaded: loaded.value, slot, now })
				return Option.some(toClaimedDeliveryOutput({ mailboxKey: input.mailboxKey, claimId, ...claimed }))
			}),
		)
	}).pipe(
		unavailable,
		Effect.withSpan('delivery.sql.claim_delivery_output', { attributes: { mailbox_key: input.mailboxKey } }),
	)

const renewDeliveryOutput = (input: RenewDeliveryOutput) =>
	Effect.gen(function* () {
		const now = yield* Clock.currentTimeMillis
		yield* changeClaimedSlot(input, {
			withRetained: false,
			change: ({ slot }) =>
				renewDeliveryOutputSlot(slot, { ...input, now }).pipe(
					Effect.map((renewed) => ({ slot: renewed, value: undefined })),
					Effect.catchTag('ClaimNotOwned', () => Effect.fail(claimLost(input))),
				),
		})
	}).pipe(
		Effect.withSpan('delivery.sql.renew_delivery_output', {
			attributes: { mailbox_key: input.mailboxKey, operation_id: input.operationId, claim_id: input.claimId },
		}),
	)

/** Record how an output attempt ended. A finishing delivery whose output is all settled retires. */
const settleDeliveryOutput = (input: SettleDeliveryOutput) =>
	changeClaimedSlot(input, {
		withRetained: true,
		change: (loaded) =>
			settleDeliveryOutputSlot(loaded.slot, {
				...input,
				now: input.settledAt,
				hasWaiting: loaded.hasWaiting,
			}).pipe(
				Effect.map((settled) => ({ slot: settled, value: undefined })),
				Effect.catchTag('ClaimNotOwned', () => Effect.fail(claimLost(input))),
			),
	}).pipe(
		Effect.withSpan('delivery.sql.settle_delivery_output', {
			attributes: {
				mailbox_key: input.mailboxKey,
				operation_id: input.operationId,
				claim_id: input.claimId,
				settlement: input.settlement._tag,
			},
		}),
	)

export type MailboxProcessingBackendSqlOptions = {
	/** The most mailboxes one `findReadyMailboxes` reports. */
	readonly claimLimit: number
}

/** Mailbox processing over the application's `SqlClient`. It creates no tables: see `MigrationsSql`. */
export const MailboxProcessingBackendSql = (options: MailboxProcessingBackendSqlOptions) =>
	Layer.effect(
		MailboxProcessingBackend,
		Effect.gen(function* () {
			const sql = yield* SqlClient.SqlClient
			return MailboxProcessingBackend.of({
				findReadyMailboxes: findReadyMailboxes(options.claimLimit).pipe(
					Effect.provideService(SqlClient.SqlClient, sql),
				),
				claimMailbox: (input) => claimMailbox(input).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
				deferMailbox: (input) => deferMailbox(input).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
				renewClaim: (input) => renewClaim(input).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
				recordProcessingAttemptResult: (input) =>
					recordProcessingAttemptResult(input).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
				prepareDelivery: (input) =>
					prepareDelivery(input).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
				handOffDelivery: (input) =>
					handOffDelivery(input).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
				claimDeliveryOutput: (input) =>
					claimDeliveryOutput(input).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
				renewDeliveryOutput: (input) =>
					renewDeliveryOutput(input).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
				settleDeliveryOutput: (input) =>
					settleDeliveryOutput(input).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
			})
		}),
	)
