import { Effect, Layer, Schema } from 'effect'
import * as Redis from 'effect/unstable/persistence/Redis'

import { SubscriptionInput } from '../Operations'
import { SubscriptionCreated, SubscriptionExisting } from '../SlackEvents'
import { ResolveSlackDirectMessageRoute, SlackDirectMessageRoute, SlackSubscriptions } from '../SlackSubscriptions'
import { subscriptionErrors } from './errors'
import { subscriptionKey } from './keys'
import * as Scripts from './scripts'

const routeJson = Schema.fromJsonString(SlackDirectMessageRoute)

const isSubscribed = Effect.fn('slack.redis.subscriptions.is_subscribed')(
	function* (input: SubscriptionInput) {
		yield* Schema.decodeEffect(SubscriptionInput)(input)
		const redis = yield* Redis.Redis
		const value = yield* Schema.decodeUnknownEffect(Schema.NullOr(Schema.Literal('1')))(
			yield* redis.send('GET', subscriptionKey(input)),
		)
		return value === '1'
	},
	(effect, input) => effect.pipe(subscriptionErrors({ operation: 'isSubscribed', threadId: input.threadId })),
)

const subscribe = Effect.fn('slack.redis.subscriptions.subscribe')(
	function* (input: SubscriptionInput) {
		yield* Schema.decodeEffect(SubscriptionInput)(input)
		const redis = yield* Redis.Redis
		const created = yield* Schema.decodeUnknownEffect(Schema.Literals([0, 1]))(
			yield* redis.eval(Scripts.subscribe)(input),
		)
		return created === 1 ? SubscriptionCreated.make({}) : SubscriptionExisting.make({})
	},
	(effect, input) => effect.pipe(subscriptionErrors({ operation: 'subscribe', threadId: input.threadId })),
)

const unsubscribe = Effect.fn('slack.redis.subscriptions.unsubscribe')(
	function* (input: SubscriptionInput) {
		yield* Schema.decodeEffect(SubscriptionInput)(input)
		const redis = yield* Redis.Redis
		yield* Schema.decodeUnknownEffect(Schema.Literals([0, 1]))(yield* redis.send('DEL', subscriptionKey(input)))
	},
	(effect, input) => effect.pipe(subscriptionErrors({ operation: 'unsubscribe', threadId: input.threadId })),
)

const resolveDirectMessageRoute = Effect.fn('slack.redis.subscriptions.resolve_dm_route')(
	function* (input: ResolveSlackDirectMessageRoute) {
		yield* Schema.decodeEffect(ResolveSlackDirectMessageRoute)(input)
		const redis = yield* Redis.Redis
		const rootedJson = yield* Schema.encodeEffect(routeJson)({ thread: input.rootedThread, subscribed: true })
		const proactiveJson = yield* Schema.encodeEffect(routeJson)({ thread: input.proactiveThread, subscribed: true })
		const fallbackJson = yield* Schema.encodeEffect(routeJson)({ thread: input.rootedThread, subscribed: false })
		return yield* Schema.decodeUnknownEffect(routeJson)(
			yield* redis.eval(Scripts.resolve)({ ...input, rootedJson, proactiveJson, fallbackJson }),
		)
	},
	(effect, input) =>
		effect.pipe(subscriptionErrors({ operation: 'resolveDirectMessageRoute', threadId: input.rootedThread.id })),
)

export const subscriptions = Layer.effect(
	SlackSubscriptions,
	Effect.gen(function* () {
		const redis = yield* Redis.Redis
		return SlackSubscriptions.of({
			isSubscribed: (input) => isSubscribed(input).pipe(Effect.provideService(Redis.Redis, redis)),
			subscribe: (input) => subscribe(input).pipe(Effect.provideService(Redis.Redis, redis)),
			unsubscribe: (input) => unsubscribe(input).pipe(Effect.provideService(Redis.Redis, redis)),
			resolveDirectMessageRoute: (input) =>
				resolveDirectMessageRoute(input).pipe(Effect.provideService(Redis.Redis, redis)),
		})
	}),
)
