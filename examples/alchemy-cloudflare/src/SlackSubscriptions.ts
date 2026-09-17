import {
	SlackSubscriptionCreated,
	SlackSubscriptionError,
	SlackSubscriptionExisting,
	SlackSubscriptions,
} from '@humanlayer/channels-slack-next'
import * as Cloudflare from 'alchemy/Cloudflare'
import { RuntimeContext } from 'alchemy/RuntimeContext'
import { Effect, Layer, Predicate } from 'effect'

const subscriptionKey = 'slack:subscribed'

const unavailable = (operation: 'subscribe' | 'is_subscribed' | 'unsubscribe') =>
	new SlackSubscriptionError({ operation, message: 'Durable Object storage was unavailable' })

/** Persists the subscription for this mailbox in Durable Object storage. */
export const SlackSubscriptionsDurableObject = Layer.effect(
	SlackSubscriptions,
	Effect.gen(function* () {
		const state = yield* Cloudflare.DurableObjectState
		const runtimeContext = yield* RuntimeContext

		return SlackSubscriptions.of({
			subscribe: () =>
				Effect.gen(function* () {
					const subscribed = yield* state.storage.get<boolean>(subscriptionKey)
					if (Predicate.isNotUndefined(subscribed)) return SlackSubscriptionExisting.make({})
					yield* state.storage.put(subscriptionKey, true)
					return SlackSubscriptionCreated.make({})
				}).pipe(
					Effect.provideService(RuntimeContext, runtimeContext),
					Effect.mapError(() => unavailable('subscribe')),
				),
			isSubscribed: () =>
				state.storage.get<boolean>(subscriptionKey).pipe(
					Effect.map((subscribed) => subscribed === true),
					Effect.provideService(RuntimeContext, runtimeContext),
					Effect.mapError(() => unavailable('is_subscribed')),
				),
			unsubscribe: () =>
				state.storage.delete(subscriptionKey).pipe(
					Effect.asVoid,
					Effect.provideService(RuntimeContext, runtimeContext),
					Effect.mapError(() => unavailable('unsubscribe')),
				),
		})
	}),
)
