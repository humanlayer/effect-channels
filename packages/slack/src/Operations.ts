import { Schema } from 'effect'

import { Content } from './Content.ts'
import { Emoji } from './Emoji.ts'
import { Message } from './Message.ts'
import {
	AttachmentRef,
	Author,
	ChannelRef,
	IdempotencyKey,
	MessageRef,
	ProviderName,
	TenantId,
	ThreadId,
	ThreadRef,
	UserId,
} from './Model.ts'
import { SentMessage } from './SentMessage.ts'
import { MessageEvent } from './SlackEvents.ts'

export const MessageHistoryOptions = Schema.Struct({
	limit: Schema.optionalKey(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 100 }))),
	cursor: Schema.optionalKey(Schema.String),
	direction: Schema.optionalKey(Schema.Literals(['forward', 'backward'])),
})
/**
 * Selects the page size, cursor, and ordering for provider-backed message history.
 *
 * @category models
 * @since 0.0.0
 */
export type MessageHistoryOptions = typeof MessageHistoryOptions.Type

export const MessagePage = Schema.Struct({
	messages: Schema.Array(Schema.suspend(() => Message)),
	nextCursor: Schema.optionalKey(Schema.String),
})
/**
 * Contains one page of messages and the cursor for the next page, when one exists.
 *
 * @category models
 * @since 0.0.0
 */
export type MessagePage = typeof MessagePage.Type

export const ThreadSummary = Schema.Struct({
	thread: ThreadRef,
	rootMessage: Schema.suspend(() => Message),
	replyCount: Schema.Natural,
	lastActivityAt: Schema.optionalKey(Schema.DateTimeUtc),
})
export type ThreadSummary = typeof ThreadSummary.Type

export const ThreadPage = Schema.Struct({
	threads: Schema.Array(ThreadSummary),
	nextCursor: Schema.optionalKey(Schema.String),
})
export type ThreadPage = typeof ThreadPage.Type

export const PostInput = Schema.Struct({ threadId: ThreadId, content: Content })
export type PostInput = typeof PostInput.Type

export const ChannelPostInput = Schema.Struct({ channel: ChannelRef, content: Content })
export type ChannelPostInput = typeof ChannelPostInput.Type

export const EditInput = Schema.Struct({ threadId: ThreadId, messageRef: MessageRef, content: Content })
export type EditInput = typeof EditInput.Type

export const DeleteInput = Schema.Struct({ threadId: ThreadId, messageRef: MessageRef })
export type DeleteInput = typeof DeleteInput.Type

export const StreamInput = Schema.Struct({
	threadId: ThreadId,
	recipientUserId: Schema.optionalKey(UserId),
})
export type StreamInput = typeof StreamInput.Type

export const StartThreadTypingInput = Schema.Struct({ threadId: ThreadId })
export type StartThreadTypingInput = typeof StartThreadTypingInput.Type

export const StartChannelTypingInput = Schema.Struct({ channel: ChannelRef })
export type StartChannelTypingInput = typeof StartChannelTypingInput.Type

export const ReactInput = Schema.Struct({ threadId: ThreadId, messageRef: MessageRef, emoji: Emoji })
export type ReactInput = typeof ReactInput.Type

export const MessagesInput = Schema.Struct({
	threadId: ThreadId,
	options: Schema.optionalKey(MessageHistoryOptions),
})
/**
 * Identifies a thread and optional paging settings for thread message history.
 *
 * @category models
 * @since 0.0.0
 */
export type MessagesInput = typeof MessagesInput.Type

export const ContainerMessagesInput = Schema.Struct({
	channel: ChannelRef,
	before: Schema.optionalKey(MessageRef),
	options: Schema.optionalKey(MessageHistoryOptions),
})
/**
 * Selects message history from a thread's containing channel, optionally before a message.
 *
 * @category models
 * @since 0.0.0
 */
export type ContainerMessagesInput = typeof ContainerMessagesInput.Type

export const ChannelThreadsInput = Schema.Struct({
	channel: ChannelRef,
	options: Schema.optionalKey(MessageHistoryOptions),
})
/**
 * Identifies a channel and optional paging settings for its threads.
 *
 * @category models
 * @since 0.0.0
 */
export type ChannelThreadsInput = typeof ChannelThreadsInput.Type

export const ThreadContext = Schema.TaggedStruct('ThreadContext', {
	threadLimit: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 100 })),
})
/**
 * Requests bounded thread history when loading context for an event.
 *
 * @category models
 * @since 0.0.0
 */
export type ThreadContext = typeof ThreadContext.Type

export const ContainerAndThreadContext = Schema.TaggedStruct('ContainerAndThreadContext', {
	threadLimit: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 100 })),
	containerLimit: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 100 })),
})
/**
 * Requests bounded thread history plus messages preceding the event in its containing channel.
 *
 * @category models
 * @since 0.0.0
 */
export type ContainerAndThreadContext = typeof ContainerAndThreadContext.Type

export const ContextPolicy = Schema.Union([ThreadContext, ContainerAndThreadContext])
export type ContextPolicy = typeof ContextPolicy.Type

export const LoadContextInput = Schema.Struct({ event: Schema.suspend(() => MessageEvent), policy: ContextPolicy })
/**
 * Selects the message event and history policy used to build conversation context.
 *
 * @category models
 * @since 0.0.0
 */
export type LoadContextInput = typeof LoadContextInput.Type

export const MessageList = Schema.Array(Schema.suspend(() => Message))
export type MessageList = typeof MessageList.Type

export const ConversationContext = Schema.Struct({
	event: Schema.suspend(() => MessageEvent),
	threadMessages: MessageList,
	containerMessages: Schema.Array(Schema.suspend(() => Message)),
})
/**
 * Contains the triggering event and its loaded thread and channel history.
 *
 * @category models
 * @since 0.0.0
 */
export type ConversationContext = typeof ConversationContext.Type

export const InfoInput = Schema.Struct({ threadId: ThreadId })
export type InfoInput = typeof InfoInput.Type

export const ChannelInfoInput = Schema.Struct({ channel: ChannelRef })
export type ChannelInfoInput = typeof ChannelInfoInput.Type

export const GetUserInput = Schema.Struct({ provider: ProviderName, tenant: TenantId, userId: UserId })
export type GetUserInput = typeof GetUserInput.Type

export const SubjectInput = Schema.Struct({ message: Schema.suspend(() => Message) })
export type SubjectInput = typeof SubjectInput.Type

export const DownloadAttachmentInput = Schema.Struct({ attachment: AttachmentRef })
export type DownloadAttachmentInput = typeof DownloadAttachmentInput.Type

export const OpenDMInput = Schema.Struct({ provider: ProviderName, tenant: TenantId, user: Author })
export type OpenDMInput = typeof OpenDMInput.Type

export const EphemeralFallbackToDm = Schema.TaggedStruct('EphemeralFallbackToDm', {})
export type EphemeralFallbackToDm = typeof EphemeralFallbackToDm.Type

export const EphemeralNoFallback = Schema.TaggedStruct('EphemeralNoFallback', {})
export type EphemeralNoFallback = typeof EphemeralNoFallback.Type

export const EphemeralFallback = Schema.Union([EphemeralFallbackToDm, EphemeralNoFallback])
export type EphemeralFallback = typeof EphemeralFallback.Type

export const PostEphemeralInput = Schema.Struct({
	threadId: ThreadId,
	user: Author,
	content: Content,
	fallback: EphemeralFallback,
})
export type PostEphemeralInput = typeof PostEphemeralInput.Type

export const EphemeralResult = Schema.Struct({
	sent: Schema.optionalKey(Schema.suspend(() => SentMessage)),
	usedFallback: Schema.Boolean,
})
export type EphemeralResult = typeof EphemeralResult.Type

export const SubscriptionInput = Schema.Struct({ threadId: ThreadId })
export type SubscriptionInput = typeof SubscriptionInput.Type

export const IngressAccepted = Schema.TaggedStruct('IngressAccepted', {
	idempotencyKey: IdempotencyKey,
})
export type IngressAccepted = typeof IngressAccepted.Type

export const IngressDropped = Schema.TaggedStruct('IngressDropped', {
	reason: Schema.Literals(['unknown_installation', 'irrelevant', 'bot', 'unsupported']),
})
export type IngressDropped = typeof IngressDropped.Type

export const IngressResult = Schema.Union([IngressAccepted, IngressDropped])
export type IngressResult = typeof IngressResult.Type
export const SlackIngressResult = IngressResult
