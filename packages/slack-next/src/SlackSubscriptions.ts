import { Context, Effect, Layer, Ref, Schema } from 'effect'

import { SlackThreadRef } from './SlackModels'

export const SlackSubscriptionInput = Schema.Struct({ thread: SlackThreadRef })
export type SlackSubscriptionInput = typeof SlackSubscriptionInput.Type

export const SlackSubscriptionCreated = Schema.TaggedStruct('SlackSubscriptionCreated', {})
export type SlackSubscriptionCreated = typeof SlackSubscriptionCreated.Type

export const SlackSubscriptionExisting = Schema.TaggedStruct('SlackSubscriptionExisting', {})
export type SlackSubscriptionExisting = typeof SlackSubscriptionExisting.Type

export const SlackSubscriptionOutcome = Schema.Union([SlackSubscriptionCreated, SlackSubscriptionExisting])
export type SlackSubscriptionOutcome = typeof SlackSubscriptionOutcome.Type

export class SlackSubscriptionError extends Schema.TaggedError<SlackSubscriptionError>()('SlackSubscriptionError', {
	operation: Schema.Literals(['subscribe', 'is_subscribed', 'unsubscribe']),
	message: Schema.String,
}) {}

const subscriptionKey = (thread: SlackThreadRef) =>
	[thread.teamId, thread.channelId, thread.threadTs].map((part) => `${part.length}:${part}`).join('|')

export class SlackSubscriptions extends Context.Service<
	SlackSubscriptions,
	{
		readonly subscribe: (
			input: SlackSubscriptionInput,
		) => Effect.Effect<SlackSubscriptionOutcome, SlackSubscriptionError>
		readonly isSubscribed: (input: SlackSubscriptionInput) => Effect.Effect<boolean, SlackSubscriptionError>
		readonly unsubscribe: (input: SlackSubscriptionInput) => Effect.Effect<void, SlackSubscriptionError>
	}
>()('@humanlayer/channels-slack-next/SlackSubscriptions') {
	static readonly layerMemory = Layer.effect(
		SlackSubscriptions,
		Effect.gen(function* () {
			const subscriptions = yield* Ref.make(new Set<string>())

			return SlackSubscriptions.of({
				subscribe: Effect.fn('slack.subscriptions.subscribe')(function* (input) {
					const key = subscriptionKey(input.thread)
					return yield* Ref.modify(
						subscriptions,
						(current): readonly [SlackSubscriptionOutcome, Set<string>] => {
							if (current.has(key)) {
								const outcome: SlackSubscriptionOutcome = SlackSubscriptionExisting.make({})
								return [outcome, current] as const
							}
							const updated = new Set(current)
							updated.add(key)
							const outcome: SlackSubscriptionOutcome = SlackSubscriptionCreated.make({})
							return [outcome, updated] as const
						},
					)
				}),
				isSubscribed: Effect.fn('slack.subscriptions.is_subscribed')(function* (input) {
					const current = yield* Ref.get(subscriptions)
					return current.has(subscriptionKey(input.thread))
				}),
				unsubscribe: Effect.fn('slack.subscriptions.unsubscribe')(function* (input) {
					const key = subscriptionKey(input.thread)
					yield* Ref.update(subscriptions, (current) => {
						if (!current.has(key)) return current
						const updated = new Set(current)
						updated.delete(key)
						return updated
					})
				}),
			})
		}),
	)
}
