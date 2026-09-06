import {
	bind,
	HandlerFailure,
	MailboxReadiness,
	MailboxStore,
	DeliveryPolicy,
	type EventDefinition,
	type HandlerContext,
	type RunnerOptions,
} from '@humanlayer/channels-delivery'
import { Context, Effect, Layer, Schema } from 'effect'

import { RetryabilityMetadata, SlackIngressError } from './DomainErrors.ts'
import type { Emoji } from './Emoji.ts'
import type { IngressResult } from './Operations.ts'
import { SlackAuthors } from './SlackAuthors.ts'
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
} from './SlackEvents.ts'
import * as Operations from './SlackIngressOperations.ts'
import {
	SlackDeliveryResource,
	messageDefinition,
	updatedDefinition,
	deletedDefinition,
	reactionDefinition,
	stoppedDefinition,
	resolveMessage,
	resolveUpdated,
	resolveDeleted,
	resolveReaction,
	type DeliveryBinding,
	type BindingError,
} from './SlackIngressOperations.ts'
import { SlackSubscriptions } from './SlackSubscriptions.ts'

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
				yield* DeliveryPolicy.makeEffect(options.policy).pipe(
					Effect.mapError(() => SlackIngressError.make({ operation: 'configuration' })),
				)
				yield* Schema.NonEmptyString.makeEffect(options.namespace).pipe(
					Effect.mapError(() => SlackIngressError.make({ operation: 'configuration' })),
				)
				const registrations = [
					...(options.handlers.onNewMention ?? []),
					...(options.handlers.onSubscribedMessage ?? []),
					...(options.handlers.onDirectMessage ?? []),
					...(options.handlers.onMessageUpdated ?? []),
					...(options.handlers.onMessageDeleted ?? []),
					...(options.handlers.onReaction ?? []),
					...(options.handlers.onConversationStopped ?? []),
				]
				const ids = registrations.map((registration) => registration.id)
				if (new Set(ids).size !== ids.length || ids.some((id) => id.length === 0)) {
					return yield* SlackIngressError.make({ operation: 'duplicate_or_empty_handler_id' })
				}

				const store = yield* MailboxStore
				const readiness = yield* MailboxReadiness
				const subscriptions = yield* SlackSubscriptions
				const authors = yield* SlackAuthors
				const handlerContext = yield* Effect.context<R>()

				const bindRegistration = <A, I>(
					definition: EventDefinition<Schema.Codec<A, I>, typeof SlackDeliveryResource>,
					registration: SlackHandlerRegistration<A, E, R>,
					resolve: (event: A) => Effect.Effect<A, never, SlackAuthors>,
					before: (event: A) => Effect.Effect<void, BindingError> = () => Effect.void,
				): DeliveryBinding<A> => {
					const delivery = bind({
						namespace: options.namespace,
						handlerId: registration.id,
						definition,
						policy:
							definition.name === messageDefinition.name
								? options.policy
								: { ...options.policy, mode: 'serial' },
						handler: (event, context) =>
							Effect.gen(function* () {
								yield* before(event)
								const resolved = yield* resolve(event).pipe(
									Effect.provideService(SlackAuthors, authors),
								)
								const skipped = yield* Effect.forEach(context.skipped, resolve).pipe(
									Effect.provideService(SlackAuthors, authors),
								)
								yield* registration
									.handler(resolved, { skipped })
									.pipe(Effect.scoped, Effect.provide(handlerContext))
							}).pipe(
								Effect.tapError(Effect.logError),
								Effect.mapError((error) =>
									HandlerFailure.make({
										retryable:
											!Schema.is(RetryabilityMetadata)(error) ||
											error.retryability === 'retryable',
									}),
								),
							),
					})
					return {
						admit: (input) => delivery.admit(input).pipe(Effect.provideService(MailboxStore, store)),
						keyForResource: delivery.keyForResource,
						cancelActive: (input) =>
							delivery.cancelActive(input).pipe(Effect.provideService(MailboxStore, store)),
						awaitCancellation: (input) =>
							delivery.awaitCancellation(input).pipe(Effect.provideService(MailboxStore, store)),
						run: (input) =>
							delivery
								.run(input)
								.pipe(
									Effect.provideService(MailboxStore, store),
									Effect.provideService(MailboxReadiness, readiness),
								),
					}
				}

				const newMention = (options.handlers.onNewMention ?? []).map((registration) =>
					bindRegistration(messageDefinition, registration, resolveMessage),
				)
				const subscribedMessage = (options.handlers.onSubscribedMessage ?? []).map((registration) =>
					bindRegistration(messageDefinition, registration, resolveMessage),
				)
				const directMessage = (options.handlers.onDirectMessage ?? []).map((registration) =>
					bindRegistration(messageDefinition, registration, resolveMessage),
				)
				const messageBindings = [...newMention, ...subscribedMessage, ...directMessage]
				const updated = (options.handlers.onMessageUpdated ?? []).map((registration) =>
					bindRegistration(updatedDefinition, registration, resolveUpdated),
				)
				const deleted = (options.handlers.onMessageDeleted ?? []).map((registration) =>
					bindRegistration(deletedDefinition, registration, resolveDeleted),
				)
				const reactions = (options.handlers.onReaction ?? []).map((registration) =>
					bindRegistration(
						reactionDefinition,
						{
							...registration,
							handler: (event, context) =>
								registration.emojis === undefined ||
								registration.emojis.some((emoji) => emoji.name === event.emoji.name)
									? registration.handler(event, context)
									: Effect.void,
						},
						resolveReaction,
					),
				)

				const stopped = (options.handlers.onConversationStopped ?? []).map((registration) =>
					bindRegistration(stoppedDefinition, registration, Effect.succeed, (event) =>
						Operations.awaitStoppedTargets(event).pipe(
							Effect.provideService(Operations.SlackIngressBindings, bindings),
						),
					),
				)
				const allBindings = [...messageBindings, ...updated, ...deleted, ...reactions, ...stopped]

				const bindings = Operations.SlackIngressBindings.of({
					newMention,
					subscribedMessage,
					directMessage,
					messageBindings,
					updated,
					deleted,
					reactions,
					stopped,
					allBindings,
				})
				const provide = <A, E>(
					effect: Effect.Effect<A, E, Operations.SlackIngressBindings | SlackSubscriptions>,
				) =>
					effect.pipe(
						Effect.provideService(Operations.SlackIngressBindings, bindings),
						Effect.provideService(SlackSubscriptions, subscriptions),
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
		)
}
