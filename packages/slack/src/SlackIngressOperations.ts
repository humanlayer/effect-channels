import {
	DeliveryError,
	type MailboxStoreError,
	type EventDefinition,
	type RunnerOptions,
} from '@humanlayer/channels-delivery'
import { Context, Effect, Schema } from 'effect'

import { SlackIngressError } from './DomainErrors.ts'
import { Message } from './Message.ts'
import { ThreadId, type ThreadRef } from './Model.ts'
import { IngressAccepted, IngressDropped, type IngressResult } from './Operations.ts'
import { SlackAuthors } from './SlackAuthors.ts'
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
import { Thread } from './Thread.ts'

export const SlackDeliveryResource = Schema.Struct({ threadId: ThreadId })
type SlackDeliveryResource = typeof SlackDeliveryResource.Type

const resourceKey = (resource: SlackDeliveryResource) => String(resource.threadId.length) + ':' + resource.threadId

export const messageDefinition = {
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

export const updatedDefinition = {
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

export const deletedDefinition = {
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

export const reactionDefinition = {
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

export const stoppedDefinition = {
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

export type BindingError = DeliveryError | MailboxStoreError

export type DeliveryBinding<A> = {
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
	readonly awaitCancellation: (input: {
		readonly key: string
		readonly controlId: string
	}) => Effect.Effect<void, BindingError>
	readonly run: (input: RunnerOptions) => Effect.Effect<void, BindingError>
}

/** Internal configured delivery bindings; constructed once with the captured handler environment. */
export class SlackIngressBindings extends Context.Service<
	SlackIngressBindings,
	{
		readonly newMention: ReadonlyArray<DeliveryBinding<MessageEvent>>
		readonly subscribedMessage: ReadonlyArray<DeliveryBinding<MessageEvent>>
		readonly directMessage: ReadonlyArray<DeliveryBinding<MessageEvent>>
		readonly messageBindings: ReadonlyArray<DeliveryBinding<MessageEvent>>
		readonly updated: ReadonlyArray<DeliveryBinding<MessageUpdatedEvent>>
		readonly deleted: ReadonlyArray<DeliveryBinding<MessageDeletedEvent>>
		readonly reactions: ReadonlyArray<DeliveryBinding<ReactionEvent>>
		readonly stopped: ReadonlyArray<DeliveryBinding<ConversationStoppedEvent>>
		readonly allBindings: ReadonlyArray<Pick<DeliveryBinding<never>, 'run'>>
	}
>()('slack/IngressBindings') {}

export const resolveMessage = (event: MessageEvent): Effect.Effect<MessageEvent, never, SlackAuthors> =>
	Effect.gen(function* () {
		const authors = yield* SlackAuthors
		return yield* Effect.map(
			authors.resolveDelivery({ thread: event.thread, message: event.message }),
			({ thread, message }) => MessageEvent.make({ ...event, thread, message }),
		)
	}).pipe(Effect.withSpan('slack.ingress.resolve_message'))

export const resolveUpdated = (event: MessageUpdatedEvent): Effect.Effect<MessageUpdatedEvent, never, SlackAuthors> =>
	Effect.gen(function* () {
		const authors = yield* SlackAuthors
		const delivery = yield* authors.resolveDelivery({
			thread: event.thread,
			message: event.message,
		})
		const previousMessage =
			event.previousMessage === undefined ? undefined : yield* authors.resolveMessage(event.previousMessage)
		return previousMessage === undefined
			? MessageUpdatedEvent.make({ ...event, thread: delivery.thread, message: delivery.message })
			: MessageUpdatedEvent.make({
					...event,
					thread: delivery.thread,
					message: delivery.message,
					previousMessage,
				})
	}).pipe(Effect.withSpan('slack.ingress.resolve_updated'))

export const resolveDeleted = (event: MessageDeletedEvent): Effect.Effect<MessageDeletedEvent, never, SlackAuthors> =>
	Effect.gen(function* () {
		const authors = yield* SlackAuthors
		return yield* event.previousMessage === undefined
			? Effect.succeed(event)
			: Effect.map(authors.resolveMessage(event.previousMessage), (previousMessage) =>
					MessageDeletedEvent.make({ ...event, previousMessage }),
				)
	}).pipe(Effect.withSpan('slack.ingress.resolve_deleted'))

export const resolveReaction = (event: ReactionEvent): Effect.Effect<ReactionEvent, never, SlackAuthors> =>
	Effect.gen(function* () {
		const authors = yield* SlackAuthors
		const actor = yield* authors.resolveAuthor({ tenant: event.tenant, author: event.actor })
		const message = event.message === undefined ? undefined : yield* authors.resolveMessage(event.message)
		return message === undefined
			? ReactionEvent.make({ ...event, actor })
			: ReactionEvent.make({ ...event, actor, message })
	}).pipe(Effect.withSpan('slack.ingress.resolve_reaction'))

const messageResource = (tenant: string, threadId: ThreadId) => ({
	installation: tenant,
	resource: SlackDeliveryResource.make({ threadId }),
})
export const awaitStoppedTargets = (
	event: ConversationStoppedEvent,
): Effect.Effect<void, BindingError, SlackIngressBindings> =>
	Effect.gen(function* () {
		const { messageBindings } = yield* SlackIngressBindings
		return yield* Effect.forEach(
			messageBindings,
			(binding) =>
				binding
					.keyForResource(messageResource(event.tenant, event.threadRef.id))
					.pipe(Effect.flatMap((key) => binding.awaitCancellation({ key, controlId: event.idempotencyKey }))),
			{ discard: true },
		)
	}).pipe(Effect.withSpan('slack.ingress.await_stopped_targets'))

export const resolveDirectMessageIdentity = (input: {
	readonly idempotencyKey: NormalizedMessage['idempotencyKey']
	readonly threadRef: ThreadRef
	readonly directMessageThread?: ThreadRef
}): Effect.Effect<
	{ readonly threadRef: ThreadRef; readonly subscribed: boolean },
	SlackIngressError,
	SlackSubscriptions
> =>
	Effect.gen(function* () {
		const subscriptions = yield* SlackSubscriptions
		return yield* subscriptions
			.resolveDirectMessageRoute({
				eventId: input.idempotencyKey,
				rootedThread: input.threadRef,
				proactiveThread: input.directMessageThread ?? input.threadRef,
			})
			.pipe(
				Effect.map((route) => ({ threadRef: route.thread, subscribed: route.subscribed })),
				mapIngressError('resolve_dm_route'),
			)
	}).pipe(Effect.withSpan('slack.ingress.resolve_dm_route'))

export const acceptMessage = Effect.fn('slack.ingress.message')(function* (
	event: NormalizedMessage,
): Effect.fn.Return<IngressResult, SlackIngressError, SlackSubscriptions | SlackIngressBindings> {
	if (event.message.author.isMe) return IngressDropped.make({ reason: 'bot' })
	const direct = yield* resolveDirectMessageIdentity({
		idempotencyKey: event.idempotencyKey,
		threadRef: event.thread.ref,
		directMessageThread: event.directMessageThread,
	})
	const bridgedMessage = direct.subscribed ? messageInThread(event.message, direct.threadRef) : event.message
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
	const { directMessage, subscribedMessage, newMention } = yield* SlackIngressBindings
	const bindings = isDirectMessage ? directMessage : subscribed ? subscribedMessage : newMention
	yield* Effect.forEach(bindings, (binding) => binding.admit({ event: delivered }), { discard: true }).pipe(
		mapIngressError('delivery_admit'),
	)
	if (subscribed) {
		const subscriptions = yield* SlackSubscriptions
		yield* subscriptions
			.subscribe({ threadId: thread.ref.id })
			.pipe(Effect.tapError(Effect.logWarning), Effect.ignore)
	}
	return IngressAccepted.make({ idempotencyKey: event.idempotencyKey })
})

export const acceptMessageUpdated = Effect.fn('slack.ingress.message_updated')(function* (
	event: NormalizedMessageUpdated,
): Effect.fn.Return<IngressResult, SlackIngressError, SlackSubscriptions | SlackIngressBindings> {
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
	const { updated } = yield* SlackIngressBindings
	yield* Effect.forEach(updated, (binding) => binding.admit({ event: delivered }), { discard: true }).pipe(
		mapIngressError('delivery_admit'),
	)
	return IngressAccepted.make({ idempotencyKey: event.idempotencyKey })
})

export const acceptMessageDeleted = Effect.fn('slack.ingress.message_deleted')(function* (
	event: NormalizedMessageDeleted,
): Effect.fn.Return<IngressResult, SlackIngressError, SlackSubscriptions | SlackIngressBindings> {
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
	const { deleted } = yield* SlackIngressBindings
	yield* Effect.forEach(deleted, (binding) => binding.admit({ event: delivered }), { discard: true }).pipe(
		mapIngressError('delivery_admit'),
	)
	return IngressAccepted.make({ idempotencyKey: event.idempotencyKey })
})

export const acceptReaction = Effect.fn('slack.ingress.reaction')(function* (
	event: NormalizedReaction,
): Effect.fn.Return<IngressResult, SlackIngressError, SlackSubscriptions | SlackIngressBindings> {
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
	const { reactions } = yield* SlackIngressBindings
	yield* Effect.forEach(reactions, (binding) => binding.admit({ event: delivered }), { discard: true }).pipe(
		mapIngressError('delivery_admit'),
	)
	return IngressAccepted.make({ idempotencyKey: event.idempotencyKey })
})

export const acceptConversationStopped = Effect.fn('slack.ingress.conversation_stopped')(function* (
	event: NormalizedConversationStopped,
): Effect.fn.Return<IngressResult, SlackIngressError, SlackSubscriptions | SlackIngressBindings> {
	const direct = yield* resolveDirectMessageIdentity({
		idempotencyKey: event.idempotencyKey,
		threadRef: event.threadRef,
		directMessageThread: event.directMessageThread,
	})
	const { messageBindings, stopped } = yield* SlackIngressBindings
	const stoppedEvent = ConversationStoppedEvent.make({ ...event, threadRef: direct.threadRef })
	yield* Effect.forEach(
		messageBindings,
		(binding) =>
			binding
				.keyForResource(messageResource(event.tenant, direct.threadRef.id))
				.pipe(Effect.flatMap((key) => binding.cancelActive({ key, controlId: event.idempotencyKey }))),
		{ discard: true },
	).pipe(mapIngressError('cancel_active'))
	yield* Effect.forEach(stopped, (binding) => binding.admit({ event: stoppedEvent }), { discard: true }).pipe(
		mapIngressError('delivery_admit'),
	)
	return IngressAccepted.make({ idempotencyKey: event.idempotencyKey })
})

export const run = (input: RunnerOptions): Effect.Effect<void, SlackIngressError, SlackIngressBindings> =>
	Effect.gen(function* () {
		const { allBindings } = yield* SlackIngressBindings
		if (allBindings.length === 0) return yield* Effect.never
		return yield* Effect.all(
			allBindings.map((binding) => binding.run(input)),
			{
				concurrency: 'unbounded',
				discard: true,
			},
		).pipe(mapIngressError('delivery_run'))
	}).pipe(Effect.withSpan('slack.ingress.run'))
