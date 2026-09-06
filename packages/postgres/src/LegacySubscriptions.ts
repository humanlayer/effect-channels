import { SubscriptionStoreError } from '@humanlayer/channels-slack'
import { SubscriptionCreated, SubscriptionExisting, type SubscriptionTransition } from '@humanlayer/channels-slack'
import type { SubscriptionInput } from '@humanlayer/channels-slack'
import { ThreadId } from '@humanlayer/channels-slack'
import { Context, Effect, Exit, Layer } from 'effect'
import { Persistable, Persistence } from 'effect/unstable/persistence'

class SubscriptionKey extends Persistable.Class<{
	payload: { readonly threadId: ThreadId }
}>()('ChannelsSubscription', {
	primaryKey: ({ threadId }) => threadId,
	success: ThreadId,
}) {}

const mapStoreError = (operation: string, input: SubscriptionInput) =>
	SubscriptionStoreError.make({ operation, threadId: input.threadId })

export class LegacySubscriptions extends Context.Service<
	LegacySubscriptions,
	{
		readonly isSubscribed: (input: SubscriptionInput) => Effect.Effect<boolean, SubscriptionStoreError>
		readonly subscribe: (input: SubscriptionInput) => Effect.Effect<SubscriptionTransition, SubscriptionStoreError>
		readonly unsubscribe: (input: SubscriptionInput) => Effect.Effect<void, SubscriptionStoreError>
	}
>()('channels/Subscriptions') {
	static readonly layer = Layer.effect(
		LegacySubscriptions,
		Effect.gen(function* () {
			const persistence = yield* Persistence.Persistence
			const store = yield* persistence.make({ storeId: 'channels-subscriptions', timeToLive: () => '30 days' })
			const isSubscribed = (input: SubscriptionInput) =>
				store.get(new SubscriptionKey({ threadId: input.threadId })).pipe(
					Effect.map((result) => result !== undefined && Exit.isSuccess(result)),
					Effect.mapError(() => mapStoreError('Subscriptions.isSubscribed', input)),
					Effect.withSpan('channels.subscriptions.is_subscribed', {
						attributes: { thread_id: input.threadId },
					}),
				)
			return LegacySubscriptions.of({
				isSubscribed,
				subscribe: (input) =>
					Effect.gen(function* () {
						const existing = yield* isSubscribed(input)
						yield* store
							.set(new SubscriptionKey({ threadId: input.threadId }), Exit.succeed(input.threadId))
							.pipe(Effect.mapError(() => mapStoreError('Subscriptions.subscribe', input)))
						return existing ? SubscriptionExisting.make({}) : SubscriptionCreated.make({})
					}).pipe(Effect.withSpan('channels.subscribe', { attributes: { thread_id: input.threadId } })),
				unsubscribe: (input) =>
					store.remove(new SubscriptionKey({ threadId: input.threadId })).pipe(
						Effect.mapError(() => mapStoreError('Subscriptions.unsubscribe', input)),
						Effect.withSpan('channels.unsubscribe', { attributes: { thread_id: input.threadId } }),
					),
			})
		}),
	)
}
