import { Clock, Context, Effect, Layer, Schema } from 'effect'

import { SubscriptionStoreError } from './DomainErrors.ts'
import { IdempotencyKey, ThreadRef, type ThreadId } from './Model.ts'
import type { SubscriptionInput } from './Operations.ts'
import { SubscriptionCreated, SubscriptionExisting, type SubscriptionTransition } from './SlackEvents.ts'

export const SlackSubscriptionsMemoryOptions = Schema.Struct({
	maxSubscriptions: Schema.Int.check(Schema.isGreaterThan(0)),
	maxRoutes: Schema.Int.check(Schema.isGreaterThan(0)),
	subscriptionTtlMs: Schema.Int.check(Schema.isGreaterThan(0)),
	routeTtlMs: Schema.Int.check(Schema.isGreaterThan(0)),
})
export type SlackSubscriptionsMemoryOptions = typeof SlackSubscriptionsMemoryOptions.Type

export const ResolveSlackDirectMessageRoute = Schema.Struct({
	eventId: IdempotencyKey,
	rootedThread: ThreadRef,
	proactiveThread: ThreadRef,
})
export type ResolveSlackDirectMessageRoute = typeof ResolveSlackDirectMessageRoute.Type

export const SlackDirectMessageRoute = Schema.Struct({
	thread: ThreadRef,
	subscribed: Schema.Boolean,
})
export type SlackDirectMessageRoute = typeof SlackDirectMessageRoute.Type

const defaultMemoryOptions = SlackSubscriptionsMemoryOptions.make({
	maxSubscriptions: 10_000,
	maxRoutes: 50_000,
	subscriptionTtlMs: 30 * 24 * 60 * 60 * 1_000,
	routeTtlMs: 24 * 60 * 60 * 1_000,
})

const storeError = (operation: string, threadId: ThreadId) => SubscriptionStoreError.make({ operation, threadId })

export class SlackSubscriptions extends Context.Service<
	SlackSubscriptions,
	{
		readonly isSubscribed: (input: SubscriptionInput) => Effect.Effect<boolean, SubscriptionStoreError>
		readonly subscribe: (input: SubscriptionInput) => Effect.Effect<SubscriptionTransition, SubscriptionStoreError>
		readonly unsubscribe: (input: SubscriptionInput) => Effect.Effect<void, SubscriptionStoreError>
		readonly resolveDirectMessageRoute: (
			input: ResolveSlackDirectMessageRoute,
		) => Effect.Effect<SlackDirectMessageRoute, SubscriptionStoreError>
	}
>()('slack/SlackSubscriptions') {
	static readonly layerMemory = (options: SlackSubscriptionsMemoryOptions = defaultMemoryOptions) =>
		Layer.effect(
			SlackSubscriptions,
			Effect.gen(function* () {
				yield* SlackSubscriptionsMemoryOptions.makeEffect(options)
				const subscriptions = new Map<ThreadId, number>()
				const routes = new Map<
					string,
					{ readonly thread: ThreadRef; readonly subscribed: boolean; readonly expiresAt: number }
				>()

				const removeExpired = (now: number) => {
					for (const [threadId, expiresAt] of subscriptions) {
						if (expiresAt <= now) subscriptions.delete(threadId)
					}
					for (const [eventId, route] of routes) {
						if (route.expiresAt <= now) routes.delete(eventId)
					}
				}

				const isSubscribed = Effect.fn('slack.subscriptions.is_subscribed')(function* (
					input: SubscriptionInput,
				) {
					const now = yield* Clock.currentTimeMillis
					return yield* Effect.sync(() => {
						const expiresAt = subscriptions.get(input.threadId)
						if (expiresAt === undefined) return false
						if (expiresAt > now) return true
						subscriptions.delete(input.threadId)
						return false
					})
				})

				return SlackSubscriptions.of({
					isSubscribed,
					subscribe: Effect.fn('slack.subscriptions.subscribe')(function* (input: SubscriptionInput) {
						const now = yield* Clock.currentTimeMillis
						return yield* Effect.suspend(() => {
							removeExpired(now)
							const existing = subscriptions.has(input.threadId)
							if (!existing && subscriptions.size >= options.maxSubscriptions) {
								return Effect.fail(storeError('subscribe_capacity', input.threadId))
							}
							subscriptions.set(input.threadId, now + options.subscriptionTtlMs)
							return Effect.succeed(
								existing ? SubscriptionExisting.make({}) : SubscriptionCreated.make({}),
							)
						})
					}),
					unsubscribe: Effect.fn('slack.subscriptions.unsubscribe')(function* (input: SubscriptionInput) {
						yield* Effect.sync(() => subscriptions.delete(input.threadId))
					}),
					resolveDirectMessageRoute: Effect.fn('slack.subscriptions.resolve_dm_route')(function* (
						input: ResolveSlackDirectMessageRoute,
					) {
						const now = yield* Clock.currentTimeMillis
						return yield* Effect.suspend(() => {
							removeExpired(now)
							const key = [
								input.rootedThread.channel.tenant,
								input.rootedThread.channel.id,
								input.eventId,
							]
								.map((segment) => `${segment.length}:${segment}`)
								.join('')
							const existing = routes.get(key)
							if (existing !== undefined) {
								return Effect.succeed(
									SlackDirectMessageRoute.make({
										thread: existing.thread,
										subscribed: existing.subscribed,
									}),
								)
							}
							if (routes.size >= options.maxRoutes) {
								return Effect.fail(storeError('route_capacity', input.rootedThread.id))
							}
							const rooted = subscriptions.has(input.rootedThread.id)
							const proactive = subscriptions.has(input.proactiveThread.id)
							const subscribed = rooted || proactive
							const thread = !rooted && proactive ? input.proactiveThread : input.rootedThread
							routes.set(key, {
								thread,
								subscribed,
								expiresAt: now + options.routeTtlMs,
							})
							return Effect.succeed(SlackDirectMessageRoute.make({ thread, subscribed }))
						})
					}),
				})
			}),
		)
}
