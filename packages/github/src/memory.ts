import { layerMailboxStoreServices } from '@humanlayer/channels-delivery'
import { layer as deliveryMemory } from '@humanlayer/channels-delivery/memory'
import { Effect, Layer, MutableHashMap, Option, Schema } from 'effect'

import {
	GitHubSubscriptionError,
	GitHubSubscriptionRoute,
	GitHubSubscriptionRouteInput,
	GitHubSubscriptionStore,
	GitHubSubscriptions,
	subscriptionKey,
} from './GitHubSubscriptions'

export interface GitHubSubscriptionsMemoryOptions {
	readonly maxSubscriptions?: number
	readonly maxRoutes?: number
}

type RouteKey = readonly [subscription: string, deliveryId: string]

export const subscriptionStore = (options: GitHubSubscriptionsMemoryOptions = {}) =>
	Layer.effect(
		GitHubSubscriptionStore,
		Effect.gen(function* () {
			const positive = Schema.Int.check(Schema.isGreaterThan(0))
			const maxSubscriptions = yield* positive.makeEffect(options.maxSubscriptions ?? 10_000)
			const maxRoutes = yield* positive.makeEffect(options.maxRoutes ?? 50_000)
			const subscriptions = new Set<string>()
			const routes = MutableHashMap.empty<RouteKey, GitHubSubscriptionRoute>()
			return GitHubSubscriptionStore.of({
				isSubscribed: Effect.fn('github.memory.is_subscribed')((input) =>
					Effect.sync(() => subscriptions.has(subscriptionKey(input))),
				),
				subscribe: Effect.fn('github.memory.subscribe')((input) =>
					Effect.suspend(() => {
						const key = subscriptionKey(input)
						if (!subscriptions.has(key) && subscriptions.size >= maxSubscriptions)
							return Effect.fail(GitHubSubscriptionError.make({ reason: 'capacity' }))
						subscriptions.add(key)
						return Effect.void
					}),
				),
				unsubscribe: Effect.fn('github.memory.unsubscribe')((input) =>
					Effect.sync(() => {
						subscriptions.delete(subscriptionKey(input))
					}),
				),
				resolveRoute: Effect.fn('github.memory.resolve_route')(
					function* (input) {
						const value = yield* Schema.decodeEffect(GitHubSubscriptionRouteInput)(input)
						const key: RouteKey = [subscriptionKey(value), value.deliveryId]
						const existing = MutableHashMap.get(routes, key)
						if (Option.isSome(existing)) return existing.value
						if (MutableHashMap.size(routes) >= maxRoutes)
							return yield* GitHubSubscriptionError.make({ reason: 'capacity' })
						const recipients = new Set(value.direct)
						if (subscriptions.has(subscriptionKey(value)))
							for (const target of value.followed) recipients.add(target)
						const targets = [...recipients]
						const route = GitHubSubscriptionRoute.make({ version: 1, targets })
						MutableHashMap.set(routes, key, route)
						return route
					},
					Effect.catchTag('SchemaError', () =>
						Effect.fail(GitHubSubscriptionError.make({ reason: 'invalid_input' })),
					),
				),
			})
		}),
	)

export const subscriptions = (options: GitHubSubscriptionsMemoryOptions = {}) =>
	GitHubSubscriptions.layer.pipe(Layer.provideMerge(subscriptionStore(options)))

export const layer = (options: GitHubSubscriptionsMemoryOptions & { readonly maxMailboxes?: number } = {}) => {
	const delivery = deliveryMemory({ maxMailboxes: options.maxMailboxes ?? 10_000 })
	return Layer.merge(subscriptions(options), layerMailboxStoreServices.pipe(Layer.provide(delivery)))
}
