import {
	BatchId,
	ClaimedMailboxBatch,
	DeliveryAccessToken,
	DeliveryAdmissionBatch,
	DeliveryHandoffUnsupported,
	DeliveryPreparationConflict,
	MailboxProcessingBackend,
	MailboxProcessingClaimLost,
	MailboxProcessingUnavailable,
	MailboxSequence,
	PreparedDeliveryInvocation,
	RecordProcessingAttemptResult,
	RecoverableMailbox,
	Timestamp,
	WaitingEvents,
	WaitingMailbox,
	makeDeliveryId,
	type ClaimDeliveryOutput,
	type ClaimedDeliveryOutput,
	type ClaimMailbox,
	type DeferMailbox,
	type HandOffMailboxDelivery,
	type PrepareMailboxDelivery,
	type ReadyMailbox,
	type RecordProcessingAttemptResult as RecordProcessingAttemptResultType,
	type RenewMailboxClaim,
} from '@humanlayer/channels-delivery-next'
import { Array as Arr, Clock, Effect, Layer, Match, Option, Predicate, Random, Schema } from 'effect'
import * as Redis from 'effect/unstable/persistence/Redis'

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
])
/** What the look script reports about a mailbox that holds a frozen batch. */
const FrozenLook = Schema.Tuple([
	Schema.Literals(['active', 'retry']),
	Schema.String,
	Schema.String,
	Schema.String,
	Schema.String,
	Schema.String,
	Schema.String,
])
const Look = Schema.NullOr(Schema.Union([IdleLook, FrozenLook]))

const preparedCodec = Schema.fromJsonString(PreparedDeliveryInvocation)
const samePreparation = Schema.toEquivalence(PreparedDeliveryInvocation)

/** What the claim script reports: the claim, its frozen batch, and the batch's saved preparation, if any. */
const Claimed = Schema.NullOr(
	Schema.Tuple([
		Schema.NonEmptyString,
		ClaimedMailboxBatch.fields.attempt,
		Schema.fromJsonString(DeliveryAdmissionBatch),
		BatchId,
		DeliveryAccessToken,
		Schema.Union([Schema.Literal(''), preparedCodec]),
	]),
)
/** What the prepare script reports: the batch's saved preparation and its ID, or null when the claim is lost. */
const Prepared = Schema.NullOr(Schema.Tuple([preparedCodec, BatchId]))
const Changed = Schema.Literals([0, 1])
const resultCodec = Schema.fromJsonString(RecordProcessingAttemptResult.fields.result)

const unavailable = <A, R>(effect: Effect.Effect<A, Redis.RedisError | Schema.SchemaError, R>) =>
	effect.pipe(
		Effect.tapError((error) => Effect.logError('Redis mailbox processing failed', error)),
		Effect.catchTags({
			RedisError: () => Effect.fail(new MailboxProcessingUnavailable({ reason: 'redis_unavailable' })),
			SchemaError: () => Effect.fail(new MailboxProcessingUnavailable({ reason: 'redis_codec_unavailable' })),
		}),
	)

/** The script appends a per-mailbox counter, so two claims of one mailbox never share an id. */
const makeClaimNonce = Effect.gen(function* () {
	const now = yield* Clock.currentTimeMillis
	return `${now}-${Math.abs(yield* Random.nextInt)}`
})

const lookAtMailbox = (mailboxKey: string, now: number) =>
	Effect.gen(function* () {
		const redis = yield* Redis.Redis
		const look = yield* Schema.decodeUnknownEffect(Look)(yield* redis.eval(Scripts.look)({ mailboxKey, now }))
		if (Predicate.isNull(look)) return Option.none<ReadyMailbox>()
		if (look[0] !== 'idle') return Option.some<ReadyMailbox>(RecoverableMailbox.make({ mailboxKey }))
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

const claimMailbox = (input: ClaimMailbox) =>
	Effect.gen(function* () {
		const redis = yield* Redis.Redis
		const now = yield* Clock.currentTimeMillis
		const result = yield* redis.eval(Scripts.claim)({
			mailboxKey: input.mailboxKey,
			claimNonce: yield* makeClaimNonce,
			now,
			leaseUntil: now + input.leaseMs,
			newBatch: Match.value(input).pipe(
				Match.tagsExhaustive({
					ClaimWaitingEvents: ({ upToSequence, batchId, accessToken }) => ({
						upToSequence,
						batchId,
						accessToken,
					}),
					ClaimFrozenBatch: () => null,
				}),
			),
		})
		const claimed = yield* Schema.decodeUnknownEffect(Claimed)(result)
		if (Predicate.isNull(claimed)) return Option.none<ClaimedMailboxBatch>()
		const [claimId, attempt, admissions, batchId, accessToken, prepared] = claimed
		const batch = { mailboxKey: input.mailboxKey, batchId, claimId, attempt, accessToken, admissions }
		return Option.some(
			prepared === '' ? ClaimedMailboxBatch.make(batch) : ClaimedMailboxBatch.make({ ...batch, prepared }),
		)
	}).pipe(unavailable, Effect.withSpan('delivery.redis.claim_mailbox', { attributes: { claim_kind: input._tag } }))

const deferMailbox = Effect.fn('delivery.redis.defer_mailbox')(function* (input: DeferMailbox) {
	const redis = yield* Redis.Redis
	yield* redis.eval(Scripts.defer)(input)
}, unavailable)

const renewClaim = Effect.fn('delivery.redis.renew_claim')(function* (input: RenewMailboxClaim) {
	const redis = yield* Redis.Redis
	const now = yield* Clock.currentTimeMillis
	const changed = yield* redis
		.eval(Scripts.renew)({
			mailboxKey: input.mailboxKey,
			claimId: input.claimId,
			leaseUntil: now + input.leaseMs,
		})
		.pipe(Effect.flatMap(Schema.decodeUnknownEffect(Changed)), unavailable)
	if (changed === 0) {
		return yield* new MailboxProcessingClaimLost({ mailboxKey: input.mailboxKey, claimId: input.claimId })
	}
})

const recordProcessingAttemptResult = Effect.fn('delivery.redis.record_processing_attempt_result')(function* (
	input: RecordProcessingAttemptResultType,
) {
	const redis = yield* Redis.Redis
	const changed = yield* Effect.gen(function* () {
		const resultJson = yield* Schema.encodeEffect(resultCodec)(input.result)
		const retryAt = Match.value(input.result).pipe(
			Match.tag('RetryableFailure', (result) => input.finishedAt + (result.retryAfterMs ?? 1_000)),
			Match.orElse(() => null),
		)
		const result = yield* redis.eval(Scripts.recordResult)({
			mailboxKey: input.claim.mailboxKey,
			claimId: input.claim.claimId,
			resultJson,
			retryAt,
			finishedAt: input.finishedAt,
		})
		return yield* Schema.decodeUnknownEffect(Changed)(result)
	}).pipe(unavailable)
	if (changed === 0) {
		return yield* new MailboxProcessingClaimLost({
			mailboxKey: input.claim.mailboxKey,
			claimId: input.claim.claimId,
		})
	}
})

/**
 * Save the callback choice on the batch the running claim owns, once. The script saves it only when none is
 * saved; a different saved choice is a conflict.
 */
const prepareDelivery = Effect.fn('delivery.redis.prepare_delivery')(function* (input: PrepareMailboxDelivery) {
	const redis = yield* Redis.Redis
	const { mailboxKey, claimId } = input
	const saved = yield* Effect.gen(function* () {
		const preparedJson = yield* Schema.encodeEffect(preparedCodec)(input.prepared)
		const result = yield* redis.eval(Scripts.prepare)({ mailboxKey, claimId, preparedJson })
		return yield* Schema.decodeUnknownEffect(Prepared)(result)
	}).pipe(unavailable)
	if (Predicate.isNull(saved)) return yield* new MailboxProcessingClaimLost({ mailboxKey, claimId })
	const [prepared, batchId] = saved
	if (!samePreparation(prepared, input.prepared)) {
		return yield* new DeliveryPreparationConflict({ deliveryId: makeDeliveryId({ mailboxKey, batchId }) })
	}
	return prepared
})

/** Redis has no remote control yet, so it refuses every handoff. */
const handOffDelivery = Effect.fn('delivery.redis.hand_off_delivery')(function* (_input: HandOffMailboxDelivery) {
	return yield* new DeliveryHandoffUnsupported()
})

/** Without handoff there is no output to send, so nothing is ever claimed or settled. */
const claimDeliveryOutput = Effect.fn('delivery.redis.claim_delivery_output')(function* (_input: ClaimDeliveryOutput) {
	return Option.none<ClaimedDeliveryOutput>()
})

const noOutputClaim = (input: { readonly mailboxKey: string; readonly claimId: string }) =>
	Effect.fail(new MailboxProcessingClaimLost({ mailboxKey: input.mailboxKey, claimId: input.claimId }))

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
				handOffDelivery,
				claimDeliveryOutput,
				renewDeliveryOutput: noOutputClaim,
				settleDeliveryOutput: noOutputClaim,
			})
		}),
	)
