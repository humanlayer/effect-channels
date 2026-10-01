/**
 * This file keeps one mailbox's delivery slot in Postgres, so the SQL store runs the shared
 * `DeliveryLifecycle` transitions: the same rules memory and the Durable Object run.
 *
 * Every change to a mailbox's deliveries follows one pattern, inside one transaction:
 *
 * 1. lock the mailbox row with `lockMailboxRow`, in a statement of its own;
 * 2. read the slot with `loadDeliverySlot`. Under READ COMMITTED these later statements see every
 *    change committed before the lock was granted; a subquery in the locking statement would not;
 * 3. run the pure transition;
 * 4. write back what changed with `writeDeliverySlot`.
 *
 * No provider is called inside: output is claimed in one transaction and settled in a later one.
 *
 * How the slot maps onto rows:
 *
 * - `readyAt` is the mailbox's `ready_at`; the active delivery is the batch named by `active_batch_id`;
 * - the active batch keeps its stage in `stage`, its callback choice in `prepared_json`, its token in
 *   `access_token`, and the rest of `ActiveDelivery` in `delivery_json`. Its admissions are the
 *   admission rows with its `batch_id`;
 * - a retained delivery is a batch row in stage `Retired`, with its `RetainedDelivery` in
 *   `delivery_json` and its end in `retain_until`. One the lifecycle drops becomes `Pruned`;
 * - claim rows are the attempt history. A mailbox has at most one live claim row: the claim that
 *   owns the batch, or the attempt waiting to retry.
 */
import {
	ActiveDelivery,
	ActiveDeliveryStage,
	BatchId,
	DeliveryAccessToken,
	DeliveryAdmissionJson,
	DeliverySlot,
	MAX_RETAINED_DELIVERIES,
	PreparedDeliveryInvocationJson,
	RetainedDelivery,
	StoredActiveDelivery,
	Timestamp,
	mailboxSchedulerStatus,
} from '@humanlayer/channels-delivery-next'
import { Array as Arr, Clock, Effect, Match, Option, Predicate, Result, Schema, Struct } from 'effect'
import * as SqlClient from 'effect/unstable/sql/SqlClient'

/** What `delivery_json` holds for the active batch: the fields no column holds. */
const storedActiveCodec = Schema.fromJsonString(StoredActiveDelivery)

/** What `delivery_json` holds for a retired batch: the fields no column holds. */
const StoredRetainedDelivery = RetainedDelivery.mapFields(Struct.omit(['batchId', 'accessToken', 'retainUntil']))
const storedRetainedCodec = Schema.fromJsonString(StoredRetainedDelivery)

const lockedRows = Schema.Array(Schema.Struct({ mailbox_key: Schema.NonEmptyString })).check(Schema.isMaxLength(1))

const mailboxRows = Schema.Array(
	Schema.Struct({
		provider: Schema.NonEmptyString,
		ready_at: Schema.NullOr(Timestamp),
		active_batch_id: Schema.NullOr(BatchId),
		has_waiting: Schema.Boolean,
	}),
).check(Schema.isMaxLength(1))

const activeBatchRows = Schema.Tuple([
	Schema.Struct({
		access_token: DeliveryAccessToken,
		prepared_json: Schema.NullOr(PreparedDeliveryInvocationJson),
		stage: ActiveDeliveryStage,
		delivery_json: storedActiveCodec,
	}),
])

/** A batch's admissions, in arrival order. A batch always has at least one. */
const batchAdmissionRows = Schema.NonEmptyArray(Schema.Struct({ admission_json: DeliveryAdmissionJson }))

const retainedRows = Schema.Array(
	Schema.Struct({
		batch_id: BatchId,
		access_token: DeliveryAccessToken,
		retain_until: Timestamp,
		delivery_json: storedRetainedCodec,
	}),
)

/**
 * How a change locks the mailbox row.
 *
 * - `Wait`: wait for the lock. For changes that must happen, such as recording a result.
 * - `SkipLocked`: give up at once when another transaction holds the row. For pollers' claims: the
 *   mailbox is busy, so another poller is working on it, and the next pass will look again.
 * - `Share`: a read that waits for writers but lets other reads run.
 */
export const MailboxLock = Schema.Literals(['Wait', 'SkipLocked', 'Share'])
export type MailboxLock = typeof MailboxLock.Type

/** Lock the mailbox row. False when there is no such mailbox, or it is busy and the lock is `SkipLocked`. */
export const lockMailboxRow = (mailboxKey: string, lock: MailboxLock) =>
	Effect.gen(function* () {
		const sql = (yield* SqlClient.SqlClient).withoutTransforms()
		const rows = yield* Match.value(lock).pipe(
			Match.when(
				'Wait',
				() => sql`SELECT mailbox_key FROM delivery_next_mailboxes
				WHERE mailbox_key = ${mailboxKey} FOR UPDATE`,
			),
			Match.when(
				'SkipLocked',
				() => sql`SELECT mailbox_key FROM delivery_next_mailboxes
				WHERE mailbox_key = ${mailboxKey} FOR UPDATE SKIP LOCKED`,
			),
			Match.when(
				'Share',
				() => sql`SELECT mailbox_key FROM delivery_next_mailboxes
				WHERE mailbox_key = ${mailboxKey} FOR SHARE`,
			),
			Match.exhaustive,
		)
		return Arr.isReadonlyArrayNonEmpty(yield* Schema.decodeUnknownEffect(lockedRows)(rows))
	})

/**
 * One mailbox's deliveries as the lifecycle sees them, with the facts its transitions need.
 *
 * @property hasWaiting - events wait behind the active delivery
 */
export const LoadedDeliverySlot = Schema.Struct({
	mailboxKey: Schema.NonEmptyString,
	provider: Schema.NonEmptyString,
	slot: DeliverySlot,
	hasWaiting: Schema.Boolean,
})
export interface LoadedDeliverySlot extends Schema.Schema.Type<typeof LoadedDeliverySlot> {}

const loadActiveDelivery = (batchId: BatchId) =>
	Effect.gen(function* () {
		const sql = (yield* SqlClient.SqlClient).withoutTransforms()
		const [batch] = yield* Schema.decodeUnknownEffect(activeBatchRows)(
			yield* sql`SELECT access_token, prepared_json, stage, delivery_json
				FROM delivery_next_batches WHERE batch_id = ${batchId}`,
		)
		const admissions = yield* Schema.decodeUnknownEffect(batchAdmissionRows)(
			yield* sql`SELECT admission_json FROM delivery_next_admissions
				WHERE batch_id = ${batchId} ORDER BY sequence_id`,
		)
		const active = {
			...batch.delivery_json,
			batchId,
			accessToken: batch.access_token,
			admissions: Arr.map(admissions, ({ admission_json }) => admission_json),
			stage: batch.stage,
		}
		return ActiveDelivery.make(
			Predicate.isNull(batch.prepared_json) ? active : { ...active, prepared: batch.prepared_json },
		)
	})

/** The mailbox's finished deliveries still in their retention period, oldest first. */
const loadRetainedDeliveries = (input: { readonly mailboxKey: string; readonly now: number }) =>
	Effect.gen(function* () {
		const sql = (yield* SqlClient.SqlClient).withoutTransforms()
		const rows = yield* Schema.decodeUnknownEffect(retainedRows)(
			yield* sql`SELECT batch_id, access_token, retain_until, delivery_json FROM delivery_next_batches
				WHERE mailbox_key = ${input.mailboxKey} AND stage = 'Retired' AND retain_until > ${input.now}
				ORDER BY retired_order DESC LIMIT ${MAX_RETAINED_DELIVERIES}`,
		)
		return rows.toReversed().map((row) =>
			RetainedDelivery.make({
				...row.delivery_json,
				batchId: row.batch_id,
				accessToken: row.access_token,
				retainUntil: row.retain_until,
			}),
		)
	})

/**
 * Read the mailbox's deliveries. Call it after `lockMailboxRow`, in the same transaction.
 * None when there is no such mailbox.
 *
 * @param withRetained - read the finished deliveries too. A change that cannot retire a delivery or
 * read a finished one, such as a lease renewal, leaves them out, and writing back leaves them alone.
 */
export const loadDeliverySlot = (input: {
	readonly mailboxKey: string
	readonly now: number
	readonly withRetained: boolean
}) =>
	Effect.gen(function* () {
		const sql = (yield* SqlClient.SqlClient).withoutTransforms()
		const [mailbox] = yield* Schema.decodeUnknownEffect(mailboxRows)(
			yield* sql`SELECT provider, ready_at, active_batch_id, EXISTS (
					SELECT 1 FROM delivery_next_admissions
					WHERE mailbox_key = ${input.mailboxKey} AND claim_id IS NULL
				) AS has_waiting
				FROM delivery_next_mailboxes WHERE mailbox_key = ${input.mailboxKey}`,
		)
		if (Predicate.isUndefined(mailbox)) return Option.none<LoadedDeliverySlot>()
		const active = Predicate.isNull(mailbox.active_batch_id)
			? null
			: yield* loadActiveDelivery(mailbox.active_batch_id)
		const retained = input.withRetained ? yield* loadRetainedDeliveries(input) : []
		return Option.some<LoadedDeliverySlot>({
			mailboxKey: input.mailboxKey,
			provider: mailbox.provider,
			hasWaiting: mailbox.has_waiting,
			slot: DeliverySlot.make({ active, readyAt: mailbox.ready_at, retained }),
		})
	})

/** Lock the mailbox row and read its deliveries. None when there is no such mailbox, or it is busy and the lock is `SkipLocked`. */
export const lockDeliverySlot = (input: {
	readonly mailboxKey: string
	readonly now: number
	readonly lock: MailboxLock
	readonly withRetained: boolean
}) =>
	Effect.gen(function* () {
		if (!(yield* lockMailboxRow(input.mailboxKey, input.lock))) return Option.none<LoadedDeliverySlot>()
		return yield* loadDeliverySlot(input)
	})

/**
 * Close claim rows that no longer own the batch. A claim whose lease ran out is abandoned; an
 * attempt that waited to retry has been retried. The attempt waiting to retry stays live.
 */
const closeReplacedClaims = (input: {
	readonly mailboxKey: string
	readonly slot: DeliverySlot
	readonly now: number
}) =>
	Effect.gen(function* () {
		const sql = (yield* SqlClient.SqlClient).withoutTransforms()
		const { active } = input.slot
		if (Predicate.isNotNull(active) && active.stage === 'Retry') return
		const owner = active?.claimId ?? null
		yield* sql`UPDATE delivery_next_claims
			SET status = CASE status WHEN 'active' THEN 'abandoned' ELSE 'retried' END,
				finished_at = COALESCE(finished_at, ${input.now}::double precision)
			WHERE mailbox_key = ${input.mailboxKey} AND status IN ('active', 'retry')
				AND claim_id IS DISTINCT FROM ${owner}::text`
	})

/** Save the active batch: a new row for a new batch, or its changed stage and state. */
const writeActiveBatch = (input: {
	readonly mailboxKey: string
	readonly before: ActiveDelivery | null
	readonly active: ActiveDelivery
	readonly now: number
}) =>
	Effect.gen(function* () {
		const sql = (yield* SqlClient.SqlClient).withoutTransforms()
		const { active, now } = input
		const deliveryJson = yield* Schema.encodeEffect(storedActiveCodec)(active)
		const preparedJson = Predicate.isUndefined(active.prepared)
			? null
			: yield* Schema.encodeEffect(PreparedDeliveryInvocationJson)(active.prepared)
		if (input.before?.batchId !== active.batchId) {
			yield* sql`INSERT INTO delivery_next_batches (
					batch_id, mailbox_key, access_token, created_at, stage, delivery_json, prepared_json, prepared_at
				) VALUES (
					${active.batchId}, ${input.mailboxKey}, ${active.accessToken}, ${now}, ${active.stage},
					${deliveryJson}, ${preparedJson}, ${Predicate.isNull(preparedJson) ? null : now}
				)`
			return
		}
		yield* sql`UPDATE delivery_next_batches
			SET stage = ${active.stage}, delivery_json = ${deliveryJson}, prepared_json = ${preparedJson}::text,
				prepared_at = CASE WHEN prepared_at IS NULL AND ${preparedJson}::text IS NOT NULL
					THEN ${now}::double precision ELSE prepared_at END
			WHERE batch_id = ${active.batchId}`
	})

/** Record the claim that now owns the batch: a new claim row, or the lease it has moved forward. */
const writeOwningClaim = (input: {
	readonly mailboxKey: string
	readonly before: ActiveDelivery | null
	readonly slot: DeliverySlot
	readonly now: number
}) =>
	Effect.gen(function* () {
		const sql = (yield* SqlClient.SqlClient).withoutTransforms()
		const { active, readyAt } = input.slot
		if (Predicate.isNull(active) || Predicate.isNull(active.claimId)) return
		const leaseExpiresAt = readyAt ?? input.now
		if (input.before?.claimId === active.claimId) {
			yield* sql`UPDATE delivery_next_claims SET lease_expires_at = ${leaseExpiresAt}
				WHERE claim_id = ${active.claimId} AND status = 'active'`
			return
		}
		yield* sql`INSERT INTO delivery_next_claims (
				claim_id, mailbox_key, batch_id, attempt, status, lease_expires_at, claimed_at
			) VALUES (
				${active.claimId}, ${input.mailboxKey}, ${active.batchId}, ${active.attempt}, 'active',
				${leaseExpiresAt}, ${input.now}
			)`
	})

/** Save deliveries that just retired, and mark those the lifecycle no longer keeps as pruned. */
const writeRetainedDeliveries = (input: { readonly before: DeliverySlot; readonly slot: DeliverySlot }) =>
	Effect.gen(function* () {
		const sql = (yield* SqlClient.SqlClient).withoutTransforms()
		const keptBefore = new Set(input.before.retained.map(({ batchId }) => batchId))
		const keptNow = new Set(input.slot.retained.map(({ batchId }) => batchId))
		const retired = input.slot.retained.filter(({ batchId }) => !keptBefore.has(batchId))
		yield* Effect.forEach(retired, (delivery) =>
			Effect.gen(function* () {
				const deliveryJson = yield* Schema.encodeEffect(storedRetainedCodec)(delivery)
				yield* sql`UPDATE delivery_next_batches
					SET stage = 'Retired', delivery_json = ${deliveryJson}, retain_until = ${delivery.retainUntil},
						retired_order = nextval('delivery_next_batches_retired_order')
					WHERE batch_id = ${delivery.batchId}`
			}),
		)
		const pruned = input.before.retained.filter(({ batchId }) => !keptNow.has(batchId))
		yield* Effect.forEach(
			pruned,
			({ batchId }) => sql`UPDATE delivery_next_batches SET stage = 'Pruned' WHERE batch_id = ${batchId}`,
		)
	})

/**
 * Apply one lifecycle change to a mailbox's deliveries in one transaction, waiting for the lock.
 * A missing mailbox or a refused change writes nothing and comes back as a failed `Result`; the
 * effect itself fails only when the database or a row's decoding does.
 *
 * @param beforeWrite - rows to write before the slot, such as the result of the claim that just ended
 */
export const changeDeliverySlot = <A, E, E2 = never, R = never>(input: {
	readonly mailboxKey: string
	readonly withRetained: boolean
	readonly onMissing: E
	readonly change: (
		loaded: LoadedDeliverySlot,
	) => Result.Result<{ readonly slot: DeliverySlot; readonly value: A }, E>
	readonly beforeWrite?: (slot: DeliverySlot) => Effect.Effect<void, E2, R>
}) =>
	Effect.gen(function* () {
		const sql = yield* SqlClient.SqlClient
		const now = yield* Clock.currentTimeMillis
		return yield* sql.withTransaction(
			Effect.gen(function* () {
				const loaded = yield* lockDeliverySlot({
					mailboxKey: input.mailboxKey,
					now,
					lock: 'Wait',
					withRetained: input.withRetained,
				})
				if (Option.isNone(loaded)) return Result.fail(input.onMissing)
				const changed = input.change(loaded.value)
				if (Result.isFailure(changed)) return Result.fail(changed.failure)
				if (changed.success.slot !== loaded.value.slot) {
					if (Predicate.isNotUndefined(input.beforeWrite)) yield* input.beforeWrite(changed.success.slot)
					yield* writeDeliverySlot({ loaded: loaded.value, slot: changed.success.slot, now })
				}
				return Result.succeed(changed.success.value)
			}),
		)
	})

/**
 * Write the slot a transition returned, under the lock `loadDeliverySlot` was read under.
 * Rows are written in the order their references need: batch, then claim, then mailbox.
 */
export const writeDeliverySlot = (input: {
	readonly loaded: LoadedDeliverySlot
	readonly slot: DeliverySlot
	readonly now: number
}) =>
	Effect.gen(function* () {
		const sql = (yield* SqlClient.SqlClient).withoutTransforms()
		const { mailboxKey } = input.loaded
		const before = input.loaded.slot
		const { active, readyAt } = input.slot
		yield* closeReplacedClaims({ mailboxKey, slot: input.slot, now: input.now })
		if (Predicate.isNotNull(active)) {
			yield* writeActiveBatch({ mailboxKey, before: before.active, active, now: input.now })
		}
		yield* writeOwningClaim({ mailboxKey, before: before.active, slot: input.slot, now: input.now })
		yield* writeRetainedDeliveries({ before, slot: input.slot })
		yield* sql`UPDATE delivery_next_mailboxes
			SET status = ${mailboxSchedulerStatus(input.slot)}, ready_at = ${readyAt}::double precision,
				active_batch_id = ${active?.batchId ?? null}::text
			WHERE mailbox_key = ${mailboxKey}`
	})
