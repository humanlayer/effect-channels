import { layerMailboxStoreServices } from '@humanlayer/channels-delivery'
import { layer as deliveryMemory } from '@humanlayer/channels-delivery/memory'
import { Effect, Layer, Schema } from 'effect'

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

export const subscriptionStore = (options: GitHubSubscriptionsMemoryOptions = {}) =>
	Layer.effect(
		GitHubSubscriptionStore,
		Effect.gen(function* () {
			const positive = Schema.Int.check(Schema.isGreaterThan(0))
			const maxSubscriptions = yield* positive.makeEffect(options.maxSubscriptions ?? 10_000)
			const maxRoutes = yield* positive.makeEffect(options.maxRoutes ?? 50_000)
			const subscriptions = new Set<string>()
			const routes = new Map<string, string>()
			const codec = Schema.fromJsonString(GitHubSubscriptionRoute)
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
				resolveRoute: Effect.fn('github.memory.resolve_route')((input) =>
					Effect.suspend(() => {
						return Schema.decodeUnknownEffect(GitHubSubscriptionRouteInput)(input).pipe(
							Effect.flatMap((value) =>
								Effect.suspend(
									(): Effect.Effect<
										GitHubSubscriptionRoute,
										GitHubSubscriptionError | Schema.SchemaError
									> => {
										const key = JSON.stringify([subscriptionKey(value), value.deliveryId])
										const existing = routes.get(key)
										if (existing !== undefined) return Schema.decodeEffect(codec)(existing)
										if (routes.size >= maxRoutes)
											return Effect.fail(GitHubSubscriptionError.make({ reason: 'capacity' }))
										const targets = [
											...new Set([
												...value.direct,
												...(subscriptions.has(subscriptionKey(value)) ? value.followed : []),
											]),
										]
										const route = GitHubSubscriptionRoute.make({ version: 1, targets })
										routes.set(key, JSON.stringify(route))
										return Effect.succeed(route)
									},
								),
							),
							Effect.catchTag('SchemaError', () =>
								Effect.fail(GitHubSubscriptionError.make({ reason: 'storage' })),
							),
						)
					}),
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
