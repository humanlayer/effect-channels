/**
 * This file keeps one mailbox's delivery slot in Redis, so the Redis store runs the shared
 * `DeliveryLifecycle` transitions: the same rules memory, the Durable Object, and Postgres run.
 *
 * Every change follows one pattern, with optimistic concurrency instead of a lock:
 *
 * 1. read the mailbox in one step with the `load` script, which returns its `version`;
 * 2. run the pure transition in TypeScript;
 * 3. write what changed with the `commit` script, which writes only if the version is unchanged,
 *    and moves it on;
 * 4. if another poller, request, or admission changed the mailbox in between, start again from 1.
 *
 * No provider is called inside a change: output is claimed in one change and settled in a later one.
 *
 * How the slot maps onto Redis (see `scripts.ts` for the keys):
 *
 * - `readyAt` is the state hash's `ready_at`, and the mailbox's score in the shared ready set;
 * - the active delivery keeps its admissions in `batch`, its ID and token in `batch_id` and
 *   `access_token`, its callback choice in `prepared`, its owner in `claim_id`, and its `attempt` and
 *   `stage` in fields of their own, as the store kept them before handoff existed. The rest of
 *   `ActiveDelivery` is the JSON in `delivery`. A batch saved before then has no `stage` or `delivery`:
 *   it reads as `Local` or `Retry`, with no links or output;
 * - the finished deliveries are one JSON list under the mailbox's retained key. The key expires when
 *   the last of them leaves its retention period; deleting them earlier is left to the lifecycle's limit.
 */
import {
	ActiveDelivery,
	ActiveDeliveryStage,
	BatchId,
	DeliveryAccessToken,
	DeliveryAdmission,
	DeliveryAdmissionBatch,
	DeliveryAdmissionJson,
	DeliverySlot,
	MailboxSchedulerStatus,
	PreparedDeliveryInvocationJson,
	RetainedDelivery,
	StoredActiveDelivery,
	Timestamp,
	emptyDeliverySlot,
	mailboxSchedulerStatus,
} from '@humanlayer/channels-delivery-next'
import { Array as Arr, Clock, Data, Effect, Option, Predicate, Random, Result, Schedule, Schema, Struct } from 'effect'
import * as Redis from 'effect/unstable/persistence/Redis'

import * as Scripts from './scripts'

const batchCodec = Schema.fromJsonString(DeliveryAdmissionBatch)

/**
 * What `delivery` holds for the active batch: the fields no other field holds. Redis also keeps the
 * claim and attempt in fields of their own, as it did before handoff existed.
 */
const storedActiveCodec = Schema.fromJsonString(StoredActiveDelivery.mapFields(Struct.omit(['claimId', 'attempt'])))
const retainedCodec = Schema.fromJsonString(Schema.Array(RetainedDelivery))

const Counter = Schema.FiniteFromString.pipe(Schema.decodeTo(Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))))

/** The state fields `load` reads, in order. */
const stateFields = [
	'version',
	'provider',
	'claims_made',
	'ready_at',
	'status',
	'batch',
	'batch_id',
	'access_token',
	'prepared',
	'claim_id',
	'attempt',
	'stage',
	'delivery',
] as const

const StoredState = Schema.Tuple([
	Schema.NullOr(Counter),
	Schema.NullOr(Schema.NonEmptyString),
	Schema.NullOr(Counter),
	Schema.NullOr(Schema.FiniteFromString.pipe(Schema.decodeTo(Timestamp))),
	Schema.NullOr(MailboxSchedulerStatus),
	Schema.NullOr(batchCodec),
	Schema.NullOr(BatchId),
	Schema.NullOr(DeliveryAccessToken),
	Schema.NullOr(PreparedDeliveryInvocationJson),
	Schema.NullOr(Schema.NonEmptyString),
	Schema.NullOr(Counter),
	Schema.NullOr(ActiveDeliveryStage),
	Schema.NullOr(storedActiveCodec),
])

const Loaded = Schema.NullOr(
	Schema.Tuple([
		StoredState,
		Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
		Schema.NullOr(retainedCodec),
		Schema.Array(DeliveryAdmissionJson),
	]),
)

const CommitResult = Schema.Literals(['ok', 'conflict', 'duplicate'])

/** Another change reached the mailbox between the read and the write; the change is decided again. */
export class SlotChanged extends Data.TaggedError('SlotChanged')<{}> {}

/** How many times a change is decided again before the store reports itself too busy. */
const MAX_CHANGE_ATTEMPTS = 50

/**
 * Decide again at once, up to the limit. The other change has already been written by the time
 * `commit` refuses, so waiting would not help.
 */
const decideAgain = Schedule.recurs(MAX_CHANGE_ATTEMPTS - 1)

/**
 * One mailbox's deliveries as the lifecycle sees them, with the facts its transitions need.
 *
 * @property version - what `commit` compares; 0 for a mailbox that does not exist yet
 * @property hasWaiting - events wait behind the active delivery
 * @property waiting - the waiting admissions `load` was asked for, oldest first
 * @property claimsMade - claims made on the mailbox so far, which makes each claim ID unique
 */
export const LoadedDeliverySlot = Schema.Struct({
	mailboxKey: Schema.NonEmptyString,
	provider: Schema.NullOr(Schema.NonEmptyString),
	version: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
	slot: DeliverySlot,
	hasWaiting: Schema.Boolean,
	waiting: Schema.Array(DeliveryAdmission),
	claimsMade: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
})
export interface LoadedDeliverySlot extends Schema.Schema.Type<typeof LoadedDeliverySlot> {}

/** The stage of a batch saved before stages were stored: its scheduler status says whether it waits to retry. */
export const legacyStage = (status: MailboxSchedulerStatus): ActiveDeliveryStage =>
	status === 'retry' ? 'Retry' : 'Local'

const toActiveDelivery = (state: typeof StoredState.Type) => {
	const [, , , , status, batch, batchId, accessToken, prepared, claimId, attempt, stage, delivery] = state
	if (status === null || status === 'idle' || batch === null) return Effect.succeed(null)
	const active = {
		links: [],
		operations: [],
		...delivery,
		batchId,
		accessToken,
		admissions: batch,
		attempt,
		claimId,
		stage: stage ?? legacyStage(status),
	}
	/** A batch always has its ID and token by now: `load` gives an older batch its own. */
	return Schema.decodeUnknownEffect(Schema.toType(ActiveDelivery))(
		Predicate.isNull(prepared) ? active : { ...active, prepared },
	)
}

/**
 * Read a mailbox's deliveries in one step. None when there is no such mailbox.
 *
 * @param waitingUpTo - also read the waiting admissions at or below this sequence
 */
export const loadDeliverySlot = (input: { readonly mailboxKey: string; readonly waitingUpTo?: number }) =>
	Effect.gen(function* () {
		const redis = yield* Redis.Redis
		const now = yield* Clock.currentTimeMillis
		const loaded = yield* Schema.decodeUnknownEffect(Loaded)(
			yield* redis.eval(Scripts.load)({
				mailboxKey: input.mailboxKey,
				pendingUpTo: input.waitingUpTo ?? null,
				legacySeed: `${now}-${Math.abs(yield* Random.nextInt)}`,
				now,
				fields: stateFields,
			}),
		)
		if (Predicate.isNull(loaded)) return Option.none<LoadedDeliverySlot>()
		const [state, waitingCount, retained, waiting] = loaded
		const [version, provider, claimsMade, readyAt] = state
		return Option.some<LoadedDeliverySlot>({
			mailboxKey: input.mailboxKey,
			provider,
			version: version ?? 0,
			hasWaiting: waitingCount > 0,
			waiting,
			claimsMade: claimsMade ?? 0,
			slot: DeliverySlot.make({ active: yield* toActiveDelivery(state), readyAt, retained: retained ?? [] }),
		})
	})

/** The slot of a mailbox no event has reached yet. */
export const emptyLoadedSlot = (mailboxKey: string): LoadedDeliverySlot => ({
	mailboxKey,
	provider: null,
	version: 0,
	slot: emptyDeliverySlot,
	hasWaiting: false,
	waiting: [],
	claimsMade: 0,
})

/** The fields that hold the active delivery, which an idle mailbox has none of. */
const activeFields = ['batch', 'batch_id', 'access_token', 'prepared', 'claim_id', 'stage', 'delivery', 'last_result']

/** The state fields to set and remove so the hash holds `slot`. The batch itself is written only when it is new. */
const stateWrites = (loaded: LoadedDeliverySlot, slot: DeliverySlot) =>
	Effect.gen(function* () {
		const { active } = slot
		const set: Array<readonly [string, string]> = [['status', mailboxSchedulerStatus(slot)]]
		if (Predicate.isNull(active)) {
			set.push(['attempt', '0'])
			return { set, remove: activeFields }
		}
		const remove: Array<string> = []
		if (loaded.slot.active?.batchId !== active.batchId) {
			set.push(['batch', yield* Schema.encodeEffect(batchCodec)(active.admissions)])
			set.push(['batch_id', active.batchId], ['access_token', active.accessToken])
		}
		if (Predicate.isUndefined(active.prepared)) remove.push('prepared')
		else set.push(['prepared', yield* Schema.encodeEffect(PreparedDeliveryInvocationJson)(active.prepared)])
		if (Predicate.isNull(active.claimId)) remove.push('claim_id')
		else set.push(['claim_id', active.claimId])
		set.push(
			['attempt', String(active.attempt)],
			['stage', active.stage],
			['delivery', yield* Schema.encodeEffect(storedActiveCodec)(active)],
		)
		return { set, remove }
	})

/** The finished deliveries to write: none when they did not change, and a lifetime that ends with the last one's retention. */
const retainedWrite = (before: DeliverySlot, slot: DeliverySlot, now: number) =>
	Effect.gen(function* () {
		if (slot.retained === before.retained) return Scripts.RetainedWrite.Keep()
		if (!Arr.isReadonlyArrayNonEmpty(slot.retained)) return Scripts.RetainedWrite.Remove()
		const lastRetained = Math.max(...slot.retained.map(({ retainUntil }) => retainUntil))
		if (lastRetained <= now) return Scripts.RetainedWrite.Remove()
		return Scripts.RetainedWrite.Replace({
			json: yield* Schema.encodeEffect(retainedCodec)(slot.retained),
			ttlMs: Math.ceil(lastRetained - now),
		})
	})

/**
 * A decided change: the slot to write and the answer to return.
 *
 * @property tookWaiting - how many waiting admissions a new batch took
 * @property madeClaim - the change used `nextClaimId`, so the mailbox's claim count moves on
 */
export type SlotChange<A> = {
	readonly slot: DeliverySlot
	readonly value: A
	readonly tookWaiting?: number
	readonly madeClaim?: boolean
}

/**
 * Write a decided change if the mailbox has not changed since `loaded` was read.
 * Fails with `SlotChanged` when it has; a repeated admission comes back as `duplicate`.
 */
export const commitDeliverySlot = (input: {
	readonly loaded: LoadedDeliverySlot
	readonly provider: string
	readonly slot: DeliverySlot
	readonly tookWaiting?: number
	readonly madeClaim?: boolean
	readonly admission?: { readonly admission: DeliveryAdmission; readonly arrivedAt: number }
}) =>
	Effect.gen(function* () {
		const redis = yield* Redis.Redis
		const now = yield* Clock.currentTimeMillis
		const { loaded, slot } = input
		const writes = yield* stateWrites(loaded, slot)
		const claims = input.madeClaim === true ? [['claims_made', String(loaded.claimsMade + 1)] as const] : []
		const admission = Predicate.isUndefined(input.admission)
			? null
			: {
					eventId: input.admission.admission.eventId,
					arrivedAt: input.admission.arrivedAt,
					json: yield* Schema.encodeEffect(DeliveryAdmissionJson)(input.admission.admission),
				}
		const result = yield* Schema.decodeUnknownEffect(CommitResult)(
			yield* redis.eval(Scripts.commit)({
				mailboxKey: loaded.mailboxKey,
				provider: input.provider,
				expectedVersion: loaded.version,
				readyAt: slot.readyAt,
				popWaiting: input.tookWaiting ?? 0,
				retained: yield* retainedWrite(loaded.slot, slot, now),
				admission,
				set: [...writes.set, ...claims],
				remove: writes.remove,
			}),
		)
		if (result === 'conflict') return yield* new SlotChanged()
		return result
	})

/** Decide a change again while another change keeps reaching the mailbox first, up to a limit. */
export const retryWhenChanged = <A, E, R>(effect: Effect.Effect<A, E | SlotChanged, R>) =>
	effect.pipe(Effect.retry({ schedule: decideAgain, while: (error) => Predicate.isTagged(error, 'SlotChanged') }))

/**
 * Apply one lifecycle change to a mailbox's deliveries, deciding it again whenever another change
 * reached the mailbox first. A missing mailbox or a refused change writes nothing and comes back as a
 * failed `Result`; the effect itself fails only when Redis, a stored value's decoding, or contention does.
 */
export const changeDeliverySlot = <A, E>(input: {
	readonly mailboxKey: string
	readonly waitingUpTo?: number
	readonly onMissing: E
	readonly change: (loaded: LoadedDeliverySlot) => Result.Result<SlotChange<A>, E>
}) =>
	Effect.gen(function* () {
		const loaded = yield* loadDeliverySlot(input)
		if (Option.isNone(loaded)) return Result.fail(input.onMissing)
		const changed = input.change(loaded.value)
		if (Result.isFailure(changed)) return Result.fail(changed.failure)
		const { slot, value } = changed.success
		if (slot !== loaded.value.slot) {
			yield* commitDeliverySlot({
				...changed.success,
				loaded: loaded.value,
				provider: loaded.value.provider ?? '',
			})
		}
		return Result.succeed(value)
	}).pipe(retryWhenChanged)

/** A claim ID no other claim on the mailbox has had: the caller's nonce and the mailbox's claim count. */
export const nextClaimId = (loaded: LoadedDeliverySlot, nonce: string) => `${nonce}-${loaded.claimsMade + 1}`
