import {
	deliveryMailboxKey,
	DeliveryAdmissionJson,
	type DeliveryAdmission,
	DeliveryReceipt,
	DeliverySlot,
	MailboxDelivery,
	MailboxDeliveryUnavailable,
	Timestamp,
	requestDeliveryInterrupt,
} from '@humanlayer/channels-delivery-next'
import { Clock, Effect, Layer, Option, Predicate, Schema } from 'effect'
import * as Redis from 'effect/unstable/persistence/Redis'

import {
	commitDeliverySlot,
	emptyLoadedSlot,
	loadDeliverySlot,
	retryWhenChanged,
	type SlotChanged,
} from './DeliverySlot'
import * as Scripts from './scripts'

const unavailable = <A, R>(effect: Effect.Effect<A, Redis.RedisError | Schema.SchemaError | SlotChanged, R>) =>
	effect.pipe(
		Effect.tapError((error) => Effect.logError('Redis mailbox admission failed', error)),
		Effect.catchTags({
			RedisError: () => Effect.fail(new MailboxDeliveryUnavailable({ reason: 'redis_unavailable' })),
			SchemaError: () => Effect.fail(new MailboxDeliveryUnavailable({ reason: 'redis_codec_unavailable' })),
			SlotChanged: () => Effect.fail(new MailboxDeliveryUnavailable({ reason: 'redis_contention' })),
		}),
	)

/** Accept an ordinary event once. An arrival wakes an idle mailbox now, even a deferred one. */
const admit = (admission: DeliveryAdmission, mailboxKey: string) =>
	Effect.gen(function* () {
		const admissionJson = yield* Schema.encodeEffect(DeliveryAdmissionJson)(admission)
		const redis = yield* Redis.Redis
		const now = yield* Clock.currentTimeMillis
		const result = yield* redis.eval(Scripts.admit)({
			mailboxKey,
			provider: admission.provider,
			eventId: admission.eventId,
			admissionJson,
			now,
		})
		return (yield* Schema.decodeUnknownEffect(Schema.Literals([0, 1]))(result)) === 1
	})

/**
 * Accept an interrupting event once, and mark the mailbox's active delivery through the shared
 * lifecycle in the same write. The event itself still waits its turn.
 */
const admitInterrupting = (admission: DeliveryAdmission, mailboxKey: string) =>
	Effect.gen(function* () {
		const now = yield* Clock.currentTimeMillis
		const loaded = Option.getOrElse(yield* loadDeliverySlot({ mailboxKey }), () => emptyLoadedSlot(mailboxKey))
		const interrupted = requestDeliveryInterrupt(loaded.slot, now)
		const slot = Predicate.isNull(interrupted.active)
			? DeliverySlot.make({ ...interrupted, readyAt: Timestamp.make(now) })
			: interrupted
		const result = yield* commitDeliverySlot({
			loaded,
			provider: admission.provider,
			slot,
			admission: { admission, arrivedAt: now },
		})
		return result === 'ok'
	}).pipe(retryWhenChanged)

const deliver = Effect.fn('delivery.redis.deliver')(function* (admission: DeliveryAdmission) {
	const mailboxKey = deliveryMailboxKey(admission)
	const accepted =
		admission.interrupt === true
			? yield* admitInterrupting(admission, mailboxKey)
			: yield* admit(admission, mailboxKey)
	return DeliveryReceipt.make({ mailboxKey, accepted })
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
