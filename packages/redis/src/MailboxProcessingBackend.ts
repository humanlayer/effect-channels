import {
	ClaimedMailboxBatch,
	DeliveryAdmissionBatch,
	MailboxProcessingBackend,
	MailboxProcessingClaimLost,
	MailboxProcessingUnavailable,
	RecordProcessingAttemptResult,
	type RecordProcessingAttemptResult as RecordProcessingAttemptResultType,
} from '@humanlayer/channels-delivery-next'
import { Clock, Effect, Layer, Match, Predicate, Random, Schema } from 'effect'
import * as Redis from 'effect/unstable/persistence/Redis'

import { readyMailboxesKey } from './keys'
import * as Scripts from './scripts'

const claimed = Schema.fromJsonString(
	Schema.Struct({
		attempt: Schema.Int.check(Schema.isGreaterThan(0)),
		admissions: DeliveryAdmissionBatch,
	}),
)
const resultCodec = Schema.fromJsonString(RecordProcessingAttemptResult.fields.result)

const unavailable = <A, R>(effect: Effect.Effect<A, Redis.RedisError | Schema.SchemaError, R>) =>
	effect.pipe(
		Effect.tapError((error) => Effect.logError('Redis mailbox processing failed', error)),
		Effect.catchTags({
			RedisError: () => Effect.fail(new MailboxProcessingUnavailable({ reason: 'redis_unavailable' })),
			SchemaError: () => Effect.fail(new MailboxProcessingUnavailable({ reason: 'redis_codec_unavailable' })),
		}),
	)

const makeClaimId = Effect.gen(function* () {
	const now = yield* Clock.currentTimeMillis
	return `${now}-${Math.abs(yield* Random.nextInt)}`
})

const claimReadyMailboxes = (claimLimit: number, recoveryAfterMs: number) =>
	Effect.gen(function* () {
		const redis = yield* Redis.Redis
		const now = yield* Clock.currentTimeMillis
		const mailboxKeys = yield* Schema.decodeUnknownEffect(Schema.Array(Schema.NonEmptyString))(
			yield* redis.send(
				'ZRANGEBYSCORE',
				readyMailboxesKey,
				'-inf',
				String(now),
				'LIMIT',
				'0',
				String(claimLimit),
			),
		)
		return (yield* Effect.forEach(mailboxKeys, (mailboxKey) =>
			Effect.gen(function* () {
				const claimId = yield* makeClaimId
				const result = yield* redis.eval(Scripts.claim)({
					mailboxKey,
					claimId,
					now,
					recoveryAt: now + recoveryAfterMs,
				})
				if (Predicate.isNull(result) || result === false) return null
				const decoded = yield* Schema.decodeUnknownEffect(claimed)(result)
				return ClaimedMailboxBatch.make({ mailboxKey, claimId, ...decoded })
			}),
		)).filter(Predicate.isNotNull)
	}).pipe(unavailable, Effect.withSpan('delivery.redis.claim_ready_mailboxes'))

const recordProcessingAttemptResult = (input: RecordProcessingAttemptResultType) =>
	Effect.gen(function* () {
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
			return yield* Schema.decodeUnknownEffect(Schema.Literals([0, 1]))(result)
		}).pipe(unavailable)
		if (changed === 0) {
			return yield* new MailboxProcessingClaimLost({
				mailboxKey: input.claim.mailboxKey,
				claimId: input.claim.claimId,
			})
		}
	}).pipe(Effect.withSpan('delivery.redis.record_processing_attempt_result'))

export type MailboxProcessingBackendRedisOptions = {
	readonly claimLimit: number
	readonly recoveryAfterMs: number
}

export const MailboxProcessingBackendRedis = (options: MailboxProcessingBackendRedisOptions) =>
	Layer.effect(
		MailboxProcessingBackend,
		Effect.gen(function* () {
			const redis = yield* Redis.Redis
			return MailboxProcessingBackend.of({
				claimReadyMailboxes: claimReadyMailboxes(options.claimLimit, options.recoveryAfterMs).pipe(
					Effect.provideService(Redis.Redis, redis),
				),
				recordProcessingAttemptResult: (input) =>
					recordProcessingAttemptResult(input).pipe(Effect.provideService(Redis.Redis, redis)),
			})
		}),
	)
