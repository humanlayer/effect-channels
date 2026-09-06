import {
	bind,
	DeliveryError,
	HandlerFailure,
	MailboxReadiness,
	MailboxStore,
	type MailboxStoreError,
	DeliveryPolicy,
	type EventDefinition,
	type HandlerContext,
	type RunnerOptions,
} from '@humanlayer/channels-delivery'
import { Context, Effect, Layer, Schema } from 'effect'

import { RetryabilityMetadata, SlackIngressError } from './DomainErrors.ts'
import { Message } from './Message.ts'
import { ThreadId, type ThreadRef } from './Model.ts'
import { IngressAccepted, IngressDropped, type IngressResult } from './Operations.ts'
import {
	ConversationStoppedEvent,
	DirectMessageDelivery,
	MessageDeletedEvent,
	MessageEvent,
	MessageUpdatedEvent,
	NewMentionDelivery,
	NormalizedConversationStopped,
	NormalizedMessage,
	NormalizedMessageDeleted,
	NormalizedMessageUpdated,
	NormalizedReaction,
	ReactionEvent,
	SubscribedMessageDelivery,
} from './SlackEvents.ts'
import { SlackSubscriptions } from './SlackSubscriptions.ts'
import { SlackUserDirectory } from './SlackUserDirectory.ts'
import { Thread } from './Thread.ts'

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
	readonly onReaction?: ReadonlyArray<SlackHandlerRegistration<ReactionEvent, E, R>>
	readonly onConversationStopped?: ReadonlyArray<SlackHandlerRegistration<ConversationStoppedEvent, E, R>>
}

export type SlackIngressOptions<E, R> = {
	readonly namespace: string
	readonly policy: DeliveryPolicy
	readonly handlers: SlackIngressHandlers<E, R>
}

const SlackDeliveryResource = Schema.Struct({ threadId: ThreadId })
type SlackDeliveryResource = typeof SlackDeliveryResource.Type

const resourceKey = (resource: SlackDeliveryResource) => String(resource.threadId.length) + ':' + resource.threadId

const messageDefinition = {
	provider: 'slack',
	name: 'slack.message',
	version: '1',
	event: MessageEvent,
	resource: SlackDeliveryResource,
	resourceKey,
	identify: (event: MessageEvent) => ({
		installation: event.tenant,
		eventId: event.idempotencyKey,
		resource: SlackDeliveryResource.make({ threadId: event.thread.ref.id }),
	}),
} satisfies EventDefinition<typeof MessageEvent, typeof SlackDeliveryResource>

const updatedDefinition = {
	provider: 'slack',
	name: 'slack.message_updated',
	version: '1',
	event: MessageUpdatedEvent,
	resource: SlackDeliveryResource,
	resourceKey,
	identify: (event: MessageUpdatedEvent) => ({
		installation: event.tenant,
		eventId: event.idempotencyKey,
		resource: SlackDeliveryResource.make({ threadId: event.thread.ref.id }),
	}),
} satisfies EventDefinition<typeof MessageUpdatedEvent, typeof SlackDeliveryResource>

const deletedDefinition = {
	provider: 'slack',
	name: 'slack.message_deleted',
	version: '1',
	event: MessageDeletedEvent,
	resource: SlackDeliveryResource,
	resourceKey,
	identify: (event: MessageDeletedEvent) => ({
		installation: event.tenant,
		eventId: event.idempotencyKey,
		resource: SlackDeliveryResource.make({ threadId: event.threadRef.id }),
	}),
} satisfies EventDefinition<typeof MessageDeletedEvent, typeof SlackDeliveryResource>

const reactionDefinition = {
	provider: 'slack',
	name: 'slack.reaction',
	version: '1',
	event: ReactionEvent,
	resource: SlackDeliveryResource,
	resourceKey,
	identify: (event: ReactionEvent) => ({
		installation: event.tenant,
		eventId: event.idempotencyKey,
		resource: SlackDeliveryResource.make({ threadId: event.thread.ref.id }),
	}),
} satisfies EventDefinition<typeof ReactionEvent, typeof SlackDeliveryResource>

const stoppedDefinition = {
	provider: 'slack',
	name: 'slack.conversation_stopped',
	version: '1',
	event: ConversationStoppedEvent,
	resource: SlackDeliveryResource,
	resourceKey,
	identify: (event: ConversationStoppedEvent) => ({
		installation: event.tenant,
		eventId: event.idempotencyKey,
		resource: SlackDeliveryResource.make({ threadId: event.threadRef.id }),
	}),
} satisfies EventDefinition<typeof ConversationStoppedEvent, typeof SlackDeliveryResource>

const messageInThread = (message: Message, threadRef: ThreadRef) => {
	const fields = {
		ref: message.ref,
		threadRef,
		text: message.text,
		markdown: message.markdown,
		author: message.author,
		metadata: message.metadata,
		attachments: message.attachments,
		raw: message.raw,
	}
	return message.replyTo === undefined ? Message.make(fields) : Message.make({ ...fields, replyTo: message.replyTo })
}

const mapIngressError =
	(operation: string) =>
	<E, A, R>(effect: Effect.Effect<A, E, R>) =>
		effect.pipe(
			Effect.tapError(Effect.logError),
			Effect.mapError(() => SlackIngressError.make({ operation })),
		)

type BindingError = DeliveryError | MailboxStoreError

type DeliveryBinding<A> = {
	readonly admit: (input: {
		readonly event: A
	}) => Effect.Effect<{ readonly key: string; readonly accepted: boolean }, BindingError>
	readonly keyForResource: (input: {
		readonly installation: string
		readonly resource: SlackDeliveryResource
	}) => Effect.Effect<string, DeliveryError>
	readonly cancelActive: (input: {
		readonly key: string
		readonly controlId: string
	}) => Effect.Effect<boolean, BindingError>
	readonly awaitInactive: (input: { readonly key: string }) => Effect.Effect<void, BindingError>
	readonly run: (input: RunnerOptions) => Effect.Effect<void, BindingError>
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
				const directory = yield* SlackUserDirectory
				const handlerContext = yield* Effect.context<R>()

				const bindRegistration = <A, I>(
					definition: EventDefinition<Schema.Codec<A, I>, typeof SlackDeliveryResource>,
					registration: SlackHandlerRegistration<A, E, R>,
					hydrate: (event: A) => Effect.Effect<A>,
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
								const hydrated = yield* hydrate(event)
								const skipped = yield* Effect.forEach(context.skipped, hydrate)
								yield* registration
									.handler(hydrated, { skipped })
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
						awaitInactive: (input) =>
							delivery.awaitInactive(input).pipe(Effect.provideService(MailboxStore, store)),
						run: (input) =>
							delivery
								.run(input)
								.pipe(
									Effect.provideService(MailboxStore, store),
									Effect.provideService(MailboxReadiness, readiness),
								),
					}
				}

				const hydrateMessage = (event: MessageEvent) =>
					Effect.map(directory.hydrateDelivery(event.thread, event.message), ({ thread, message }) =>
						MessageEvent.make({ ...event, thread, message }),
					)
				const hydrateUpdated = (event: MessageUpdatedEvent) =>
					Effect.gen(function* () {
						const delivery = yield* directory.hydrateDelivery(event.thread, event.message)
						const previousMessage =
							event.previousMessage === undefined
								? undefined
								: yield* directory.hydrateMessage(event.previousMessage)
						return previousMessage === undefined
							? MessageUpdatedEvent.make({ ...event, thread: delivery.thread, message: delivery.message })
							: MessageUpdatedEvent.make({
									...event,
									thread: delivery.thread,
									message: delivery.message,
									previousMessage,
								})
					})
				const hydrateDeleted = (event: MessageDeletedEvent) =>
					event.previousMessage === undefined
						? Effect.succeed(event)
						: Effect.map(directory.hydrateMessage(event.previousMessage), (previousMessage) =>
								MessageDeletedEvent.make({ ...event, previousMessage }),
							)
				const hydrateReaction = (event: ReactionEvent) =>
					Effect.gen(function* () {
						const actor = yield* directory.hydrateAuthor(
							{ provider: 'slack', tenant: event.tenant, userId: event.actor.userId },
							event.actor,
						)
						const message =
							event.message === undefined ? undefined : yield* directory.hydrateMessage(event.message)
						return message === undefined
							? ReactionEvent.make({ ...event, actor })
							: ReactionEvent.make({ ...event, actor, message })
					})
				const hydrateStopped = (event: ConversationStoppedEvent) => Effect.succeed(event)

				const newMention = (options.handlers.onNewMention ?? []).map((registration) =>
					bindRegistration(messageDefinition, registration, hydrateMessage),
				)
				const subscribedMessage = (options.handlers.onSubscribedMessage ?? []).map((registration) =>
					bindRegistration(messageDefinition, registration, hydrateMessage),
				)
				const directMessage = (options.handlers.onDirectMessage ?? []).map((registration) =>
					bindRegistration(messageDefinition, registration, hydrateMessage),
				)
				const messageBindings = [...newMention, ...subscribedMessage, ...directMessage]
				const updated = (options.handlers.onMessageUpdated ?? []).map((registration) =>
					bindRegistration(updatedDefinition, registration, hydrateUpdated),
				)
				const deleted = (options.handlers.onMessageDeleted ?? []).map((registration) =>
					bindRegistration(deletedDefinition, registration, hydrateDeleted),
				)
				const reactions = (options.handlers.onReaction ?? []).map((registration) =>
					bindRegistration(reactionDefinition, registration, hydrateReaction),
				)

				const messageResource = (tenant: string, threadId: ThreadId) => ({
					installation: tenant,
					resource: SlackDeliveryResource.make({ threadId }),
				})
				const awaitMessageIdle = (event: ConversationStoppedEvent) =>
					Effect.forEach(
						messageBindings,
						(binding) =>
							binding
								.keyForResource(messageResource(event.tenant, event.threadRef.id))
								.pipe(Effect.flatMap((key) => binding.awaitInactive({ key }))),
						{ discard: true },
					)
				const stopped = (options.handlers.onConversationStopped ?? []).map((registration) =>
					bindRegistration(stoppedDefinition, registration, hydrateStopped, awaitMessageIdle),
				)
				const allBindings = [...messageBindings, ...updated, ...deleted, ...reactions, ...stopped]

				const admitAll = <A>(bindings: ReadonlyArray<DeliveryBinding<A>>, event: A) =>
					Effect.forEach(bindings, (binding) => binding.admit({ event }), { discard: true }).pipe(
						mapIngressError('delivery_admit'),
					)

				const resolveDirectMessageIdentity = (input: {
					readonly idempotencyKey: NormalizedMessage['idempotencyKey']
					readonly threadRef: ThreadRef
					readonly directMessageThread?: ThreadRef
				}) => {
					return subscriptions
						.resolveDirectMessageRoute({
							eventId: input.idempotencyKey,
							rootedThread: input.threadRef,
							proactiveThread: input.directMessageThread ?? input.threadRef,
						})
						.pipe(
							Effect.map((route) => ({ threadRef: route.thread, subscribed: route.subscribed })),
							mapIngressError('resolve_dm_route'),
						)
				}

				const acceptMessage = Effect.fn('slack.ingress.message')(function* (event: NormalizedMessage) {
					if (event.message.author.isMe) return IngressDropped.make({ reason: 'bot' })
					const direct = yield* resolveDirectMessageIdentity({
						idempotencyKey: event.idempotencyKey,
						threadRef: event.thread.ref,
						directMessageThread: event.directMessageThread,
					})
					const bridgedMessage = direct.subscribed
						? messageInThread(event.message, direct.threadRef)
						: event.message
					const thread = direct.subscribed
						? Thread.make({
								ref: direct.threadRef,
								currentMessage: bridgedMessage,
								recentMessages: [bridgedMessage],
							})
						: event.thread
					const message = thread.currentMessage ?? bridgedMessage
					const isDirectMessage = event.thread.ref.channel.isDm
					const subscribed = direct.subscribed
					if (!isDirectMessage && !subscribed && !event.mentioned) {
						return IngressDropped.make({ reason: 'irrelevant' })
					}
					const delivery = isDirectMessage
						? DirectMessageDelivery.make({})
						: subscribed
							? SubscribedMessageDelivery.make({})
							: NewMentionDelivery.make({ location: event.thread.ref.isNew ? 'channel_root' : 'thread' })
					const delivered = MessageEvent.make({ ...event, thread, message, delivery })
					const bindings = isDirectMessage ? directMessage : subscribed ? subscribedMessage : newMention
					yield* admitAll(bindings, delivered)
					if (subscribed) {
						yield* subscriptions
							.subscribe({ threadId: thread.ref.id })
							.pipe(Effect.tapError(Effect.logWarning), Effect.ignore)
					}
					return IngressAccepted.make({ idempotencyKey: event.idempotencyKey })
				})

				const acceptMessageUpdated = Effect.fn('slack.ingress.message_updated')(function* (
					event: NormalizedMessageUpdated,
				) {
					if (event.message.author.isMe || event.previousMessage?.author.isMe === true) {
						return IngressDropped.make({ reason: 'bot' })
					}
					const direct = yield* resolveDirectMessageIdentity({
						idempotencyKey: event.idempotencyKey,
						threadRef: event.thread.ref,
						directMessageThread: event.directMessageThread,
					})
					const message = direct.subscribed ? messageInThread(event.message, direct.threadRef) : event.message
					const previousMessage =
						direct.subscribed && event.previousMessage !== undefined
							? messageInThread(event.previousMessage, direct.threadRef)
							: event.previousMessage
					const thread = direct.subscribed
						? Thread.make({ ref: direct.threadRef, currentMessage: message, recentMessages: [message] })
						: event.thread
					const delivered =
						previousMessage === undefined
							? MessageUpdatedEvent.make({ ...event, thread, message })
							: MessageUpdatedEvent.make({ ...event, thread, message, previousMessage })
					yield* admitAll(updated, delivered)
					return IngressAccepted.make({ idempotencyKey: event.idempotencyKey })
				})

				const acceptMessageDeleted = Effect.fn('slack.ingress.message_deleted')(function* (
					event: NormalizedMessageDeleted,
				) {
					if (event.previousMessage?.author.isMe === true) return IngressDropped.make({ reason: 'bot' })
					const direct = yield* resolveDirectMessageIdentity({
						idempotencyKey: event.idempotencyKey,
						threadRef: event.threadRef,
						directMessageThread: event.directMessageThread,
					})
					const previousMessage =
						direct.subscribed && event.previousMessage !== undefined
							? messageInThread(event.previousMessage, direct.threadRef)
							: event.previousMessage
					const delivered =
						previousMessage === undefined
							? MessageDeletedEvent.make({ ...event, threadRef: direct.threadRef })
							: MessageDeletedEvent.make({ ...event, threadRef: direct.threadRef, previousMessage })
					yield* admitAll(deleted, delivered)
					return IngressAccepted.make({ idempotencyKey: event.idempotencyKey })
				})

				const acceptReaction = Effect.fn('slack.ingress.reaction')(function* (event: NormalizedReaction) {
					if (event.actor.isMe) return IngressDropped.make({ reason: 'bot' })
					const direct = yield* resolveDirectMessageIdentity({
						idempotencyKey: event.idempotencyKey,
						threadRef: event.thread.ref,
						directMessageThread: event.directMessageThread,
					})
					const message =
						direct.subscribed && event.message !== undefined
							? messageInThread(event.message, direct.threadRef)
							: event.message
					const thread = direct.subscribed ? Thread.fromRef(direct.threadRef) : event.thread
					const delivered =
						message === undefined
							? ReactionEvent.make({ ...event, thread })
							: ReactionEvent.make({ ...event, thread, message })
					yield* admitAll(reactions, delivered)
					return IngressAccepted.make({ idempotencyKey: event.idempotencyKey })
				})

				const acceptConversationStopped = Effect.fn('slack.ingress.conversation_stopped')(function* (
					event: NormalizedConversationStopped,
				) {
					const direct = yield* resolveDirectMessageIdentity({
						idempotencyKey: event.idempotencyKey,
						threadRef: event.threadRef,
						directMessageThread: event.directMessageThread,
					})
					const stoppedEvent = ConversationStoppedEvent.make({ ...event, threadRef: direct.threadRef })
					yield* Effect.forEach(
						messageBindings,
						(binding) =>
							binding
								.keyForResource(messageResource(event.tenant, direct.threadRef.id))
								.pipe(
									Effect.flatMap((key) =>
										binding.cancelActive({ key, controlId: event.idempotencyKey }),
									),
								),
						{ discard: true },
					).pipe(mapIngressError('cancel_active'))
					yield* admitAll(stopped, stoppedEvent)
					return IngressAccepted.make({ idempotencyKey: event.idempotencyKey })
				})

				return SlackIngress.of({
					acceptMessage,
					acceptMessageUpdated,
					acceptMessageDeleted,
					acceptReaction,
					acceptConversationStopped,
					run: (input) => {
						if (allBindings.length === 0) return Effect.never
						return Effect.all(
							allBindings.map((binding) => binding.run(input)),
							{ concurrency: 'unbounded', discard: true },
						).pipe(mapIngressError('delivery_run'))
					},
				})
			}),
		)
}
