import {
	MailboxSubscriptionCreatedResult,
	MailboxSubscriptionError,
	MailboxSubscriptionAlreadyExistsResult,
	type MailboxSubscriptionOperation,
	MailboxSubscriptions,
} from '@humanlayer/channels-delivery-next'
import { Effect, Layer, Schema } from 'effect'
import * as Redis from 'effect/unstable/persistence/Redis'

import { mailboxSubscriptionsKey } from './Keys'

const changed = Schema.Literals([0, 1])

const unavailable = <A, R>(
	operation: MailboxSubscriptionOperation,
	effect: Effect.Effect<A, Redis.RedisError | Schema.SchemaError, R>,
) =>
	effect.pipe(
		Effect.tapError((error) =>
			Effect.logError('Redis mailbox subscription operation failed', error).pipe(
				Effect.annotateLogs({ operation }),
			),
		),
		Effect.catchTags({
			RedisError: () =>
				Effect.fail(
					new MailboxSubscriptionError({
						operation,
						reason: 'redis_unavailable',
					}),
				),

			SchemaError: () =>
				Effect.fail(
					new MailboxSubscriptionError({
						operation,
						reason: 'redis_codec_unavailable',
					}),
				),
		}),
	)

const subscribe = Effect.fn('delivery.redis.subscriptions.subscribe')(
	function* ({ mailboxKey }: { readonly mailboxKey: string }) {
		const redis = yield* Redis.Redis

		const result = yield* redis.send('SADD', mailboxSubscriptionsKey, mailboxKey)

		const created = yield* Schema.decodeUnknownEffect(changed)(result)

		return created === 1
			? MailboxSubscriptionCreatedResult.make({})
			: MailboxSubscriptionAlreadyExistsResult.make({})
	},
	(effect) => unavailable('subscribe', effect),
)

const isSubscribed = Effect.fn('delivery.redis.subscriptions.is_subscribed')(
	function* ({ mailboxKey }: { readonly mailboxKey: string }) {
		const redis = yield* Redis.Redis

		const result = yield* redis.send('SISMEMBER', mailboxSubscriptionsKey, mailboxKey)

		return (yield* Schema.decodeUnknownEffect(changed)(result)) === 1
	},
	(effect) => unavailable('is_subscribed', effect),
)

const unsubscribe = Effect.fn('delivery.redis.subscriptions.unsubscribe')(
	function* ({ mailboxKey }: { readonly mailboxKey: string }) {
		const redis = yield* Redis.Redis

		const result = yield* redis.send('SREM', mailboxSubscriptionsKey, mailboxKey)

		yield* Schema.decodeUnknownEffect(changed)(result)
	},
	(effect) => unavailable('unsubscribe', effect),
)

export const MailboxSubscriptionsRedis = Layer.effect(
	MailboxSubscriptions,
	Effect.gen(function* () {
		const redis = yield* Redis.Redis

		return MailboxSubscriptions.of({
			subscribe: (input) => subscribe(input).pipe(Effect.provideService(Redis.Redis, redis)),

			isSubscribed: (input) => isSubscribed(input).pipe(Effect.provideService(Redis.Redis, redis)),

			unsubscribe: (input) => unsubscribe(input).pipe(Effect.provideService(Redis.Redis, redis)),
		})
	}),
)
