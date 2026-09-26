import {
	DeliveryQueue,
	DeliveryInterruption,
	IngressAttributionStore,
	type RunnerOptions,
} from '@humanlayer/channels-delivery'
import { Effect } from 'effect'

import { SlackIngressError } from './DomainErrors'
import { Message } from './Message'
import { type ThreadRef } from './Model'
import { IngressAccepted, IngressDropped, type IngressResult } from './Operations'
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
} from './SlackEvents'
import { SlackSubscriptions } from './SlackSubscriptions'
import { Thread } from './Thread'

export {
	SlackIngressBindings,
	resolveMessage,
	resolveUpdated,
	resolveDeleted,
	resolveReaction,
} from './SlackIngressBindings'
import { AdmitInput, SlackIngressBindings } from './SlackIngressBindings'
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
): Effect.fn.Return<
	IngressResult,
	SlackIngressError,
	SlackSubscriptions | SlackIngressBindings | DeliveryQueue | IngressAttributionStore
> {
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
	const bindings = yield* SlackIngressBindings
	yield* bindings.admit(AdmitInput.Message({ event: delivered }))
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
): Effect.fn.Return<
	IngressResult,
	SlackIngressError,
	SlackSubscriptions | SlackIngressBindings | DeliveryQueue | IngressAttributionStore
> {
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
	const bindings = yield* SlackIngressBindings
	yield* bindings.admit(AdmitInput.Updated({ event: delivered }))
	return IngressAccepted.make({ idempotencyKey: event.idempotencyKey })
})

export const acceptMessageDeleted = Effect.fn('slack.ingress.message_deleted')(function* (
	event: NormalizedMessageDeleted,
): Effect.fn.Return<
	IngressResult,
	SlackIngressError,
	SlackSubscriptions | SlackIngressBindings | DeliveryQueue | IngressAttributionStore
> {
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
	const bindings = yield* SlackIngressBindings
	yield* bindings.admit(AdmitInput.Deleted({ event: delivered }))
	return IngressAccepted.make({ idempotencyKey: event.idempotencyKey })
})

export const acceptReaction = Effect.fn('slack.ingress.reaction')(function* (
	event: NormalizedReaction,
): Effect.fn.Return<
	IngressResult,
	SlackIngressError,
	SlackSubscriptions | SlackIngressBindings | DeliveryQueue | IngressAttributionStore
> {
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
	const bindings = yield* SlackIngressBindings
	yield* bindings.admit(AdmitInput.Reaction({ event: delivered }))
	return IngressAccepted.make({ idempotencyKey: event.idempotencyKey })
})

export const acceptConversationStopped = Effect.fn('slack.ingress.conversation_stopped')(function* (
	event: NormalizedConversationStopped,
): Effect.fn.Return<
	IngressResult,
	SlackIngressError,
	SlackSubscriptions | SlackIngressBindings | DeliveryQueue | DeliveryInterruption | IngressAttributionStore
> {
	const direct = yield* resolveDirectMessageIdentity({
		idempotencyKey: event.idempotencyKey,
		threadRef: event.threadRef,
		directMessageThread: event.directMessageThread,
	})
	const bindings = yield* SlackIngressBindings
	yield* bindings.stopConversation({
		event: ConversationStoppedEvent.make({ ...event, threadRef: direct.threadRef }),
	})
	return IngressAccepted.make({ idempotencyKey: event.idempotencyKey })
})

export const run = Effect.fn('slack.ingress.run')(function* (input: RunnerOptions) {
	const bindings = yield* SlackIngressBindings
	return yield* bindings.run(input)
})
