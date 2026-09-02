import { Schema } from 'effect'

import { Content } from './Content.ts'
import { Emoji } from './Emoji.ts'
import { MessageEvent } from './Events.ts'
import { Message } from './Message.ts'
import {
	AttachmentRef,
	Author,
	ChannelRef,
	ConversationRunId,
	IdempotencyKey,
	MessageRef,
	OrgId,
	ProviderName,
	RuntimeInstanceId,
	SourceName,
	TenantId,
	ThreadId,
	ThreadRef,
	UserId,
} from './Schema.ts'
import { SentMessage } from './SentMessage.ts'

export const MessageHistoryOptions = Schema.Struct({
	limit: Schema.optionalKey(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 100 }))),
	cursor: Schema.optionalKey(Schema.String),
	direction: Schema.optionalKey(Schema.Literals(['forward', 'backward'])),
})
export type MessageHistoryOptions = typeof MessageHistoryOptions.Type

export const MessagePage = Schema.Struct({
	messages: Schema.Array(Schema.suspend(() => Message)),
	nextCursor: Schema.optionalKey(Schema.String),
})
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

export const StreamInput = Schema.Struct({ threadId: ThreadId })
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
export type MessagesInput = typeof MessagesInput.Type

export const ContainerMessagesInput = Schema.Struct({
	channel: ChannelRef,
	before: Schema.optionalKey(MessageRef),
	options: Schema.optionalKey(MessageHistoryOptions),
})
export type ContainerMessagesInput = typeof ContainerMessagesInput.Type

export const ChannelThreadsInput = Schema.Struct({
	channel: ChannelRef,
	options: Schema.optionalKey(MessageHistoryOptions),
})
export type ChannelThreadsInput = typeof ChannelThreadsInput.Type

export const ThreadContext = Schema.TaggedStruct('ThreadContext', {
	threadLimit: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 100 })),
})
export type ThreadContext = typeof ThreadContext.Type

export const ContainerAndThreadContext = Schema.TaggedStruct('ContainerAndThreadContext', {
	threadLimit: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 100 })),
	containerLimit: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 100 })),
})
export type ContainerAndThreadContext = typeof ContainerAndThreadContext.Type

export const ContextPolicy = Schema.Union([ThreadContext, ContainerAndThreadContext])
export type ContextPolicy = typeof ContextPolicy.Type

export const LoadContextInput = Schema.Struct({ event: Schema.suspend(() => MessageEvent), policy: ContextPolicy })
export type LoadContextInput = typeof LoadContextInput.Type

export const MessageList = Schema.Array(Schema.suspend(() => Message))
export type MessageList = typeof MessageList.Type

export const ConversationContext = Schema.Struct({
	event: Schema.suspend(() => MessageEvent),
	threadMessages: MessageList,
	containerMessages: Schema.Array(Schema.suspend(() => Message)),
})
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

export const ConversationCoordinatorOptions = Schema.Struct({
	leaseTtlMs: Schema.Int.check(Schema.isGreaterThan(0)),
	heartbeatEveryMs: Schema.Int.check(Schema.isGreaterThan(0)),
	acquireTimeoutMs: Schema.Int.check(Schema.isGreaterThan(0)),
	retryBaseMs: Schema.Int.check(Schema.isGreaterThan(0)),
	retryMaxMs: Schema.Int.check(Schema.isGreaterThan(0)),
	alertAfterAttempts: Schema.Int.check(Schema.isGreaterThan(0)),
})
export type ConversationCoordinatorOptions = typeof ConversationCoordinatorOptions.Type

export const CancelConversationInput = Schema.Struct({
	threadId: ThreadId,
	reason: Schema.Literals(['provider_stop', 'application']),
})
export type CancelConversationInput = typeof CancelConversationInput.Type

export const InterruptConversation = Schema.TaggedStruct('InterruptConversation', {
	ownerId: RuntimeInstanceId,
	threadId: ThreadId,
	runId: ConversationRunId,
})
export type InterruptConversation = typeof InterruptConversation.Type

export const SupersedeConversation = Schema.TaggedStruct('SupersedeConversation', {
	ownerId: RuntimeInstanceId,
	threadId: ThreadId,
	runId: ConversationRunId,
})
export type SupersedeConversation = typeof SupersedeConversation.Type

export const ConversationSignal = Schema.Union([InterruptConversation, SupersedeConversation])
export type ConversationSignal = typeof ConversationSignal.Type

export const ProviderByNameInput = Schema.Struct({ provider: ProviderName })
export type ProviderByNameInput = typeof ProviderByNameInput.Type

export const ProviderByThreadIdInput = Schema.Struct({ threadId: ThreadId })
export type ProviderByThreadIdInput = typeof ProviderByThreadIdInput.Type

export const ProviderByChannelInput = Schema.Struct({ channel: ChannelRef })
export type ProviderByChannelInput = typeof ProviderByChannelInput.Type

export const QueueDelivery = Schema.TaggedStruct('QueueDelivery', {})
export type QueueDelivery = typeof QueueDelivery.Type

export const DebounceDelivery = Schema.TaggedStruct('DebounceDelivery', {
	windowMs: Schema.Int.check(Schema.isGreaterThan(0)),
})
export type DebounceDelivery = typeof DebounceDelivery.Type

export const ConcurrentDelivery = Schema.TaggedStruct('ConcurrentDelivery', {
	concurrency: Schema.Int.check(Schema.isGreaterThan(0)),
})
export type ConcurrentDelivery = typeof ConcurrentDelivery.Type

export const InterruptDelivery = Schema.TaggedStruct('InterruptDelivery', {})
export type InterruptDelivery = typeof InterruptDelivery.Type

export const DeliveryStrategy = Schema.Union([QueueDelivery, DebounceDelivery, ConcurrentDelivery, InterruptDelivery])
export type DeliveryStrategy = typeof DeliveryStrategy.Type

export const OrganizationLookup = Schema.Struct({ source: SourceName, tenant: TenantId })
export type OrganizationLookup = typeof OrganizationLookup.Type

export const GateCheck = Schema.Struct({ orgId: OrgId, source: SourceName, tenant: TenantId })
export type GateCheck = typeof GateCheck.Type

export const OutboundReport = Schema.Struct({
	orgId: OrgId,
	provider: ProviderName,
	tenant: TenantId,
	threadId: ThreadId,
	operation: Schema.NonEmptyString,
	ok: Schema.Boolean,
	degraded: Schema.Array(Schema.String),
})
export type OutboundReport = typeof OutboundReport.Type

export const IngressAccepted = Schema.TaggedStruct('IngressAccepted', {
	idempotencyKey: IdempotencyKey,
})
export type IngressAccepted = typeof IngressAccepted.Type

export const IngressDropped = Schema.TaggedStruct('IngressDropped', {
	reason: Schema.Literals(['unknown_organization', 'tenant_disabled', 'irrelevant', 'bot', 'unsupported']),
})
export type IngressDropped = typeof IngressDropped.Type

export const IngressResult = Schema.Union([IngressAccepted, IngressDropped])
export type IngressResult = typeof IngressResult.Type
