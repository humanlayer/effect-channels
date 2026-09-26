import {
	MailboxReadiness,
	MailboxStore,
	DeliveryQueue,
	DeliveryInterruption,
	IngressAttributionStore,
	DeliveryPolicy,
	type DeliveryHandoff,
	type HandlerContext,
	type RunnerOptions,
} from '@humanlayer/channels-delivery'
import { Context, Effect, Layer, Logger } from 'effect'

import { SlackIngressError } from './DomainErrors'
import type { Emoji } from './Emoji'
import type { IngressResult } from './Operations'
import { SlackAuthors } from './SlackAuthors'
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
} from './SlackEvents'
import * as Operations from './SlackIngressOperations'
import { SlackSubscriptions } from './SlackSubscriptions'

export type SlackHandlerRegistration<A, E, R> = {
	readonly id: string
	readonly handler: (event: A, context: HandlerContext<A>) => Effect.Effect<void | DeliveryHandoff, E, R>
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
		readonly processMailbox: (input: {
			readonly key: string
		}) => Effect.Effect<void, SlackIngressError, MailboxStore | SlackAuthors>
		readonly run: (
			input: RunnerOptions,
		) => Effect.Effect<void, SlackIngressError, MailboxStore | MailboxReadiness | SlackAuthors>
	}
>()('slack/SlackIngress') {
	static readonly layer = <E = never, R = never>(options: SlackIngressOptions<E, R>) =>
		Layer.effect(
			SlackIngress,
			Effect.gen(function* () {
				const bindings = yield* Operations.SlackIngressBindings
				const subscriptions = yield* SlackSubscriptions
				const queue = yield* DeliveryQueue
				const interruption = yield* DeliveryInterruption
				const attribution = yield* IngressAttributionStore
				const loggers = yield* Logger.CurrentLoggers

				const provide = <A, E, R2>(effect: Effect.Effect<A, E, R2>) =>
					effect.pipe(
						Effect.provideService(Operations.SlackIngressBindings, bindings),
						Effect.provideService(SlackSubscriptions, subscriptions),
						Effect.provideService(DeliveryQueue, queue),
						Effect.provideService(DeliveryInterruption, interruption),
						Effect.provideService(IngressAttributionStore, attribution),
						Effect.provideService(Logger.CurrentLoggers, loggers),
					)
				return SlackIngress.of({
					acceptMessage: (event) => provide(Operations.acceptMessage(event)),
					acceptMessageUpdated: (event) => provide(Operations.acceptMessageUpdated(event)),
					acceptMessageDeleted: (event) => provide(Operations.acceptMessageDeleted(event)),
					acceptReaction: (event) => provide(Operations.acceptReaction(event)),
					acceptConversationStopped: (event) => provide(Operations.acceptConversationStopped(event)),
					processMailbox: (input) => provide(bindings.processMailbox(input)),
					run: (input) => provide(Operations.run(input)),
				})
			}),
		).pipe(Layer.provide(Operations.SlackIngressBindings.layer(options)))
}

/** Explicit long-running polling program for server hosts. Constructing SlackBot does not start it. */
export const runDeliveryPolling = (input: RunnerOptions) =>
	Effect.flatMap(SlackIngress, (ingress) => ingress.run(input)).pipe(Effect.withSpan('slack.delivery.polling'))
