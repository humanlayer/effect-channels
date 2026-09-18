import {
	deliveryMailboxKey,
	DeliveryAdmission,
	DeliveryReceipt,
	MailboxDelivery,
	MailboxDeliveryUnavailable,
} from '@humanlayer/channels-delivery-next'
import { Clock, Effect, Layer, Schema } from 'effect'
import * as Redis from 'effect/unstable/persistence/Redis'

import * as Scripts from './scripts'

const admissionCodec = Schema.fromJsonString(DeliveryAdmission)

const unavailable = <A, R>(effect: Effect.Effect<A, Redis.RedisError | Schema.SchemaError, R>) =>
	effect.pipe(
		Effect.tapError((error) => Effect.logError('Redis mailbox admission failed', error)),
		Effect.catchTags({
			RedisError: () => Effect.fail(new MailboxDeliveryUnavailable({ reason: 'redis_unavailable' })),
			SchemaError: () => Effect.fail(new MailboxDeliveryUnavailable({ reason: 'redis_codec_unavailable' })),
		}),
	)

const deliver = Effect.fn('delivery.redis.deliver')(function* (admission: DeliveryAdmission) {
	const mailboxKey = deliveryMailboxKey(admission)
	const admissionJson = yield* Schema.encodeEffect(admissionCodec)(admission)
	const redis = yield* Redis.Redis
	const now = yield* Clock.currentTimeMillis
	const result = yield* redis.eval(Scripts.admit)({
		mailboxKey,
		provider: admission.provider,
		eventId: admission.eventId,
		admissionJson,
		now,
	})
	const accepted = yield* Schema.decodeUnknownEffect(Schema.Literals([0, 1]))(result)
	return DeliveryReceipt.make({ mailboxKey, accepted: accepted === 1 })
}, unavailable)

export const MailboxDeliveryRedis = Layer.effect(
	MailboxDelivery,
	Effect.gen(function* () {
		const redis = yield* Redis.Redis
		return MailboxDelivery.of({
			deliver: (admission) => deliver(admission).pipe(Effect.provideService(Redis.Redis, redis)),
		})
	}),
)

export const layer = MailboxDeliveryRedis
