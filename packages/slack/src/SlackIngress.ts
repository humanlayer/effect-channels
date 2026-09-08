import {
	MailboxReadiness,
	MailboxStore,
	DeliveryPolicy,
	type HandlerContext,
	type RunnerOptions,
} from '@humanlayer/channels-delivery'
import { Context, Effect, Layer } from 'effect'

import { SlackIngressError } from './DomainErrors.js'
import type { Emoji } from './Emoji.js'
import type { IngressResult } from './Operations.js'
import { SlackAuthors } from './SlackAuthors.js'
import {
	ConversationStoppedEvent,
	MessageDeletedEvent,
	MessageEvent,
	MessageUpdatedEvent,
	NormalizedConversationStopped,
	NormalizedMessage,
	NormalizedMessageDeleted,
	NormalizedMessageUpdated,
	NormalizedReaction,
	ReactionEvent,
} from './SlackEvents.js'
import * as Operations from './SlackIngressOperations.js'
import { SlackSubscriptions } from './SlackSubscriptions.js'

export type SlackHandlerRegistration<A, E, R> = {
	readonly id: string
	readonly handler: (event: A, context: HandlerContext<A>) => Effect.Effect<void, E, R>
}

export type SlackIngressHandlers<E, R> = {
	readonly onNewMention?: ReadonlyArray<SlackHandlerRegistration<MessageEvent, E, R>>
	readonly onSubscribedMessage?: ReadonlyArray<SlackHandlerRegistration<MessageEvent, E, R>>
	readonly onDirectMessage?: ReadonlyArray<SlackHandlerRegistration<MessageEvent, E, R>>
	readonly onMessageUpdated?: ReadonlyArray<SlackHandlerRegistration<MessageUpdatedEvent, E, R>>
	readonly onMessageDeleted?: ReadonlyArray<SlackHandlerRegistration<MessageDeletedEvent, E, R>>
	readonly onReaction?: ReadonlyArray<
		SlackHandlerRegistration<ReactionEvent, E, R> & { readonly emojis?: ReadonlyArray<Emoji> }
	>
	readonly onConversationStopped?: ReadonlyArray<SlackHandlerRegistration<ConversationStoppedEvent, E, R>>
}

export type SlackIngressOptions<E, R> = {
	readonly namespace: string
	readonly policy: DeliveryPolicy
	readonly handlers: SlackIngressHandlers<E, R>
}

export class SlackIngress extends Context.Service<
	SlackIngress,
	{
		readonly acceptMessage: (event: NormalizedMessage) => Effect.Effect<IngressResult, SlackIngressError>
		readonly acceptMessageUpdated: (
			event: NormalizedMessageUpdated,
		) => Effect.Effect<IngressResult, SlackIngressError>
		readonly acceptMessageDeleted: (
			event: NormalizedMessageDeleted,
		) => Effect.Effect<IngressResult, SlackIngressError>
		readonly acceptReaction: (event: NormalizedReaction) => Effect.Effect<IngressResult, SlackIngressError>
		readonly acceptConversationStopped: (
			event: NormalizedConversationStopped,
		) => Effect.Effect<IngressResult, SlackIngressError>
		readonly run: (input: RunnerOptions) => Effect.Effect<void, SlackIngressError>
	}
>()('slack/SlackIngress') {
	static readonly layer = <E = never, R = never>(options: SlackIngressOptions<E, R>) =>
		Layer.effect(
			SlackIngress,
			Effect.gen(function* () {
				const bindings = yield* Operations.SlackIngressBindings
				const subscriptions = yield* SlackSubscriptions
				const store = yield* MailboxStore
				const readiness = yield* MailboxReadiness
				const authors = yield* SlackAuthors

				const provide = <A, E>(
					effect: Effect.Effect<
						A,
						E,
						| Operations.SlackIngressBindings
						| SlackSubscriptions
						| MailboxStore
						| MailboxReadiness
						| SlackAuthors
					>,
				) =>
					effect.pipe(
						Effect.provideService(Operations.SlackIngressBindings, bindings),
						Effect.provideService(SlackSubscriptions, subscriptions),
						Effect.provideService(MailboxStore, store),
						Effect.provideService(MailboxReadiness, readiness),
						Effect.provideService(SlackAuthors, authors),
					)
				return SlackIngress.of({
					acceptMessage: (event) => provide(Operations.acceptMessage(event)),
					acceptMessageUpdated: (event) => provide(Operations.acceptMessageUpdated(event)),
					acceptMessageDeleted: (event) => provide(Operations.acceptMessageDeleted(event)),
					acceptReaction: (event) => provide(Operations.acceptReaction(event)),
					acceptConversationStopped: (event) => provide(Operations.acceptConversationStopped(event)),
					run: (input) => provide(Operations.run(input)),
				})
			}),
		).pipe(Layer.provide(Operations.SlackIngressBindings.layer(options)))
}
