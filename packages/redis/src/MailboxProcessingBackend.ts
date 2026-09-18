import {
	ClaimedMailboxBatch,
	DeliveryAdmissionBatch,
	MailboxProcessingBackend,
	MailboxProcessingClaimLost,
	MailboxProcessingUnavailable,
	MailboxSequence,
	RecordProcessingAttemptResult,
	RecoverableMailbox,
	Timestamp,
	WaitingEvents,
	WaitingMailbox,
	type ClaimMailbox,
	type DeferMailbox,
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

const Claimed = Schema.NullOr(
	Schema.Tuple([
		Schema.NonEmptyString,
		ClaimedMailboxBatch.fields.attempt,
		Schema.fromJsonString(DeliveryAdmissionBatch),
	]),
)
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
			upToSequence: Match.value(input).pipe(
				Match.tagsExhaustive({
					ClaimWaitingEvents: ({ upToSequence }) => upToSequence,
					ClaimFrozenBatch: () => null,
				}),
			),
		})
		const claimed = yield* Schema.decodeUnknownEffect(Claimed)(result)
		if (Predicate.isNull(claimed)) return Option.none<ClaimedMailboxBatch>()
		const [claimId, attempt, admissions] = claimed
		return Option.some(ClaimedMailboxBatch.make({ mailboxKey: input.mailboxKey, claimId, attempt, admissions }))
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
			})
		}),
	)
