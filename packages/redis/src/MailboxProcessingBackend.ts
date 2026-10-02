/**
 * The Redis store behind delivery-next's MailboxProcessingBackend.
 *
 * Every change runs the shared `DeliveryLifecycle` transition in TypeScript and writes it only if the
 * mailbox did not change since it was read; see `DeliverySlot.ts` for the pattern and the layout. Two
 * pollers never take the same batch or output operation: the second one's write finds the version
 * moved, decides again, and finds nothing left to take.
 *
 * All times come from Effect's Clock and reach Redis as arguments.
 */
import {
	ClaimedMailboxBatch,
	DEFAULT_RETRY_AFTER_MS,
	DeliveryPreparationConflict,
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
	type ClaimedDeliveryOutput,
	type ClaimMailbox,
	type DeferMailbox,
	type DeliverySlot,
	type HandOffMailboxDelivery,
	type PrepareMailboxDelivery,
	type ReadyMailbox,
	type RecordProcessingAttemptResult,
	type RenewDeliveryOutput,
	type RenewMailboxClaim,
	type SettleDeliveryOutput,
} from '@humanlayer/channels-delivery-next'
import { Array as Arr, Clock, Effect, Layer, Match, Option, Predicate, Random, Schema } from 'effect'
import * as Redis from 'effect/unstable/persistence/Redis'

import {
	changeDeliverySlot,
	legacyStage,
	nextClaimId,
	type LoadedDeliverySlot,
	type NarrowRedisFailure,
	type SlotChange,
} from './DeliverySlot'
import { readyMailboxesKey } from './Keys'
import * as Scripts from './scripts'

/** What the look script reports about an idle mailbox with events waiting. */
const IdleLook = Schema.Tuple([
	Schema.Literal('idle'),
	Schema.NonEmptyString,
	Schema.FiniteFromString.pipe(Schema.decodeTo(WaitingEvents.fields.count)),
	Schema.FiniteFromString.pipe(Schema.decodeTo(MailboxSequence)),
	Schema.FiniteFromString.pipe(Schema.decodeTo(Timestamp)),
	Schema.FiniteFromString.pipe(Schema.decodeTo(MailboxSequence)),
	Schema.FiniteFromString.pipe(Schema.decodeTo(Timestamp)),
	Schema.Literal(''),
])
/** What the look script reports about a mailbox with an active delivery. A batch saved before stages were stored has none. */
const BusyLook = Schema.Tuple([
	Schema.Literals(['active', 'retry']),
	Schema.String,
	Schema.String,
	Schema.String,
	Schema.String,
	Schema.String,
	Schema.String,
	Schema.Union([ActiveDeliveryStage, Schema.Literal('')]),
])
const Look = Schema.NullOr(Schema.Union([IdleLook, BusyLook]))

/** Log a Redis, codec, or contention failure where it happens, then narrow it to `MailboxProcessingUnavailable`. */
const narrowRedisFailure: NarrowRedisFailure<MailboxProcessingUnavailable> = (reason) => (error) =>
	Effect.logError('Redis mailbox processing failed', error).pipe(
		Effect.andThen(Effect.fail(new MailboxProcessingUnavailable({ reason }))),
	)

/** Narrow the failures of an operation that only reads and writes keys. */
const unavailable = <A, R>(effect: Effect.Effect<A, Redis.RedisError | Schema.SchemaError, R>) =>
	effect.pipe(
		Effect.catchTags({
			RedisError: narrowRedisFailure('redis_unavailable'),
			SchemaError: narrowRedisFailure('redis_codec_unavailable'),
		}),
	)

/** The first part of a claim ID; `nextClaimId` adds the mailbox's claim count, so two claims never share one. */
const makeClaimNonce = Effect.gen(function* () {
	const now = yield* Clock.currentTimeMillis
	return `${now}-${Math.abs(yield* Random.nextInt)}`
})

const claimLost = (claim: { readonly mailboxKey: string; readonly claimId: string }) =>
	new MailboxProcessingClaimLost({ mailboxKey: claim.mailboxKey, claimId: claim.claimId })

/**
 * An idle mailbox reports its waiting events; one with an active delivery reports the work its
 * stage leaves: its callback to run again, or output to send.
 */
const lookAtMailbox = (mailboxKey: string, now: number) =>
	Effect.gen(function* () {
		const redis = yield* Redis.Redis
		const look = yield* Schema.decodeUnknownEffect(Look)(yield* redis.eval(Scripts.look)({ mailboxKey, now }))
		if (Predicate.isNull(look)) return Option.none<ReadyMailbox>()
		if (look[0] !== 'idle') {
			const [status, , , , , , , storedStage] = look
			const stage = storedStage === '' ? legacyStage(status) : storedStage
			return Option.some<ReadyMailbox>(
				activeDeliveryWork({ stage }) === 'Output'
					? OutputReadyMailbox.make({ mailboxKey })
					: RecoverableMailbox.make({ mailboxKey }),
			)
		}
		const [, provider, count, firstSequence, firstArrivedAt, lastSequence, lastArrivedAt] = look
		return Option.some<ReadyMailbox>(
			WaitingMailbox.make({
				mailboxKey,
				provider,
				waiting: { count, firstSequence, firstArrivedAt, lastSequence, lastArrivedAt },
			}),
		)
	})

const findReadyMailboxes = Effect.fn('delivery.redis.find_ready_mailboxes')(function* (claimLimit: number) {
	const redis = yield* Redis.Redis
	const now = yield* Clock.currentTimeMillis
	const mailboxKeys = yield* Schema.decodeUnknownEffect(Schema.Array(Schema.NonEmptyString))(
		yield* redis.send('ZRANGEBYSCORE', readyMailboxesKey, '-inf', String(now), 'LIMIT', '0', String(claimLimit)),
	)
	const looks = yield* Effect.forEach(mailboxKeys, (mailboxKey) => lookAtMailbox(mailboxKey, now), {
		concurrency: 'unbounded',
	})
	return Arr.getSomes(looks)
}, unavailable)

/** A decided change that writes nothing. */
const unchanged = <A>(loaded: LoadedDeliverySlot, value: A): SlotChange<A> => ({ slot: loaded.slot, value })

/**
 * Freeze the waiting admissions at or below `upToSequence` as a new batch, or take the frozen batch
 * again. Taking a frozen batch may settle the delivery instead of claiming it, such as one whose lease
 * ran out after handoff, so that change is written even when nothing is claimed.
 */
const claimMailbox = (input: ClaimMailbox) =>
	Effect.gen(function* () {
		const nonce = yield* makeClaimNonce
		const now = yield* Clock.currentTimeMillis
		return yield* changeDeliverySlot({
			mailboxKey: input.mailboxKey,
			waitingUpTo: Match.value(input).pipe(
				Match.tagsExhaustive({
					ClaimWaitingEvents: ({ upToSequence }) => upToSequence,
					ClaimFrozenBatch: () => undefined,
				}),
			),
			onMissing: Effect.succeedNone,
			change: (loaded) =>
				Effect.sync((): SlotChange<Option.Option<ClaimedMailboxBatch>> => {
					const { readyAt } = loaded.slot
					if (Predicate.isNull(readyAt) || readyAt > now) return unchanged(loaded, Option.none())
					const claimId = nextClaimId(loaded, nonce)
					return Match.value(input).pipe(
						Match.tagsExhaustive({
							ClaimWaitingEvents: ({ batchId, accessToken, leaseMs }) => {
								const [first, ...rest] = loaded.waiting
								if (Predicate.isUndefined(first)) return unchanged(loaded, Option.none())
								const started = startDeliveryBatch(loaded.slot, {
									batchId,
									accessToken,
									admissions: [first, ...rest],
									claimId,
									leaseMs,
									now,
								})
								if (Predicate.isNull(started)) return unchanged(loaded, Option.none())
								return {
									slot: started.slot,
									value: Option.some(
										toClaimedMailboxBatch({
											mailboxKey: input.mailboxKey,
											active: started.claimed,
											claimId,
										}),
									),
									tookWaiting: loaded.waiting.length,
									madeClaim: true,
								}
							},
							ClaimFrozenBatch: ({ leaseMs }) => {
								const { slot, claimed } = claimFrozenBatchSlot(loaded.slot, {
									claimId,
									leaseMs,
									now,
									hasWaiting: loaded.hasWaiting,
								})
								return {
									slot,
									value: Predicate.isNull(claimed)
										? Option.none()
										: Option.some(
												toClaimedMailboxBatch({
													mailboxKey: input.mailboxKey,
													active: claimed,
													claimId,
												}),
											),
									madeClaim: Predicate.isNotNull(claimed),
								}
							},
						}),
					)
				}),
			narrowRedisFailure,
		})
	}).pipe(
		Effect.withSpan('delivery.redis.claim_mailbox', {
			attributes: { mailbox_key: input.mailboxKey, claim_kind: input._tag },
		}),
	)

/** Put an idle mailbox off until later. A newer waiting event means delivery already woke it, so the deferral is dropped. */
const deferMailbox = Effect.fn('delivery.redis.defer_mailbox')(function* (input: DeferMailbox) {
	const redis = yield* Redis.Redis
	yield* redis.eval(Scripts.defer)(input)
}, unavailable)

/**
 * Apply a lifecycle change on behalf of a claim. A missing mailbox, or a change the lifecycle
 * refuses because the claim no longer owns the work, is a lost claim.
 */
const changeClaimedSlot = <A, E = never>(
	claim: { readonly mailboxKey: string; readonly claimId: string },
	change: (
		loaded: LoadedDeliverySlot,
	) => Effect.Effect<{ readonly slot: DeliverySlot; readonly value: A }, E | MailboxProcessingClaimLost>,
) =>
	changeDeliverySlot({
		mailboxKey: claim.mailboxKey,
		onMissing: Effect.fail(claimLost(claim)),
		change,
		narrowRedisFailure,
	})

const renewClaim = (input: RenewMailboxClaim) =>
	Effect.gen(function* () {
		const now = yield* Clock.currentTimeMillis
		yield* changeClaimedSlot(input, ({ slot }) =>
			renewDeliveryClaim(slot, { ...input, now }).pipe(
				Effect.map((renewed) => ({ slot: renewed, value: undefined })),
				Effect.catchTag('ClaimNotOwned', () => Effect.fail(claimLost(input))),
			),
		)
	}).pipe(
		Effect.withSpan('delivery.redis.renew_claim', {
			attributes: { mailbox_key: input.mailboxKey, claim_id: input.claimId },
		}),
	)

/** Record how an attempt ended. The lifecycle decides what follows from the stage, not only the result. */
const recordProcessingAttemptResult = (input: RecordProcessingAttemptResult) =>
	Effect.gen(function* () {
		const { claim } = input
		const retryAfterMs = Match.value(input.result).pipe(
			Match.tag('RetryableFailure', ({ retryAfterMs }) => retryAfterMs ?? DEFAULT_RETRY_AFTER_MS),
			Match.orElse(() => null),
		)
		yield* changeClaimedSlot(claim, (loaded) =>
			recordDeliveryAttempt(loaded.slot, {
				claimId: claim.claimId,
				retryAfterMs,
				now: input.finishedAt,
				hasWaiting: loaded.hasWaiting,
			}).pipe(
				Effect.map((slot) => ({ slot, value: undefined })),
				Effect.catchTag('ClaimNotOwned', () => Effect.fail(claimLost(claim))),
			),
		)
	}).pipe(
		Effect.withSpan('delivery.redis.record_processing_attempt_result', {
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
	changeClaimedSlot<PrepareMailboxDelivery['prepared'], DeliveryPreparationConflict>(input, ({ slot }) =>
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
	).pipe(
		Effect.withSpan('delivery.redis.prepare_delivery', {
			attributes: { mailbox_key: input.mailboxKey, claim_id: input.claimId },
		}),
	)

/** Hand the batch off. The claim stays as ownership of callback cleanup until its result is recorded. */
const handOffDelivery = (input: HandOffMailboxDelivery) =>
	changeClaimedSlot(input, ({ slot }) =>
		handOffDeliverySlot(slot, input).pipe(
			Effect.map((handedOff) => ({ slot: handedOff, value: undefined })),
			Effect.catchTag('ClaimNotOwned', () => Effect.fail(claimLost(input))),
		),
	).pipe(
		Effect.withSpan('delivery.redis.hand_off_delivery', {
			attributes: { mailbox_key: input.mailboxKey, claim_id: input.claimId },
		}),
	)

/** Take the next due output operation under a new lease. */
const claimDeliveryOutput = (input: ClaimDeliveryOutput) =>
	Effect.gen(function* () {
		const nonce = yield* makeClaimNonce
		const now = yield* Clock.currentTimeMillis
		return yield* changeDeliverySlot({
			mailboxKey: input.mailboxKey,
			onMissing: Effect.succeedNone,
			change: (loaded) =>
				Effect.sync((): SlotChange<Option.Option<ClaimedDeliveryOutput>> => {
					const claimId = nextClaimId(loaded, nonce)
					const { slot, claimed } = claimDeliveryOutputSlot(loaded.slot, {
						claimId,
						leaseMs: input.leaseMs,
						now,
						idempotencyKey: input.idempotencyKey,
					})
					if (Predicate.isNull(claimed)) return unchanged(loaded, Option.none())
					return {
						slot,
						value: Option.some(
							toClaimedDeliveryOutput({ mailboxKey: input.mailboxKey, claimId, ...claimed }),
						),
						madeClaim: true,
					}
				}),
			narrowRedisFailure,
		})
	}).pipe(Effect.withSpan('delivery.redis.claim_delivery_output', { attributes: { mailbox_key: input.mailboxKey } }))

const renewDeliveryOutput = (input: RenewDeliveryOutput) =>
	Effect.gen(function* () {
		const now = yield* Clock.currentTimeMillis
		yield* changeClaimedSlot(input, ({ slot }) =>
			renewDeliveryOutputSlot(slot, { ...input, now }).pipe(
				Effect.map((renewed) => ({ slot: renewed, value: undefined })),
				Effect.catchTag('ClaimNotOwned', () => Effect.fail(claimLost(input))),
			),
		)
	}).pipe(
		Effect.withSpan('delivery.redis.renew_delivery_output', {
			attributes: { mailbox_key: input.mailboxKey, operation_id: input.operationId, claim_id: input.claimId },
		}),
	)

/** Record how an output attempt ended. A finishing delivery whose output is all settled retires. */
const settleDeliveryOutput = (input: SettleDeliveryOutput) =>
	changeClaimedSlot(input, (loaded) =>
		settleDeliveryOutputSlot(loaded.slot, {
			...input,
			now: input.settledAt,
			hasWaiting: loaded.hasWaiting,
		}).pipe(
			Effect.map((settled) => ({ slot: settled, value: undefined })),
			Effect.catchTag('ClaimNotOwned', () => Effect.fail(claimLost(input))),
		),
	).pipe(
		Effect.withSpan('delivery.redis.settle_delivery_output', {
			attributes: {
				mailbox_key: input.mailboxKey,
				operation_id: input.operationId,
				claim_id: input.claimId,
				settlement: input.settlement._tag,
			},
		}),
	)

export type MailboxProcessingBackendRedisOptions = {
	/** The most due mailboxes one look reports. */
	readonly claimLimit: number
}

export const MailboxProcessingBackendRedis = (options: MailboxProcessingBackendRedisOptions) =>
	Layer.effect(
		MailboxProcessingBackend,
		Effect.gen(function* () {
			const redis = yield* Redis.Redis
			const withRedis = Effect.provideService(Redis.Redis, redis)
			return MailboxProcessingBackend.of({
				findReadyMailboxes: findReadyMailboxes(options.claimLimit).pipe(withRedis),
				claimMailbox: (input) => claimMailbox(input).pipe(withRedis),
				deferMailbox: (input) => deferMailbox(input).pipe(withRedis),
				renewClaim: (input) => renewClaim(input).pipe(withRedis),
				recordProcessingAttemptResult: (input) => recordProcessingAttemptResult(input).pipe(withRedis),
				prepareDelivery: (input) => prepareDelivery(input).pipe(withRedis),
				handOffDelivery: (input) => handOffDelivery(input).pipe(withRedis),
				claimDeliveryOutput: (input) => claimDeliveryOutput(input).pipe(withRedis),
				renewDeliveryOutput: (input) => renewDeliveryOutput(input).pipe(withRedis),
				settleDeliveryOutput: (input) => settleDeliveryOutput(input).pipe(withRedis),
			})
		}),
	)
