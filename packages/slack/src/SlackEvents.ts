import { Schema } from 'effect'

import { Emoji } from './Emoji'
import { Message } from './Message'
import { Author, IdempotencyKey, MessageRef, ProviderName, TenantId, ThreadRef, UserId } from './Model'
import { Thread } from './Thread'

export const ReactionAdded = Schema.TaggedStruct('ReactionAdded', {})
export type ReactionAdded = typeof ReactionAdded.Type

export const ReactionRemoved = Schema.TaggedStruct('ReactionRemoved', {})
export type ReactionRemoved = typeof ReactionRemoved.Type

export const ReactionChange = Schema.Union([ReactionAdded, ReactionRemoved])
export type ReactionChange = typeof ReactionChange.Type

export const NormalizedMessage = Schema.Struct({
	provider: ProviderName,
	tenant: TenantId,
	idempotencyKey: IdempotencyKey,
	thread: Schema.suspend(() => Thread),
	directMessageThread: Schema.optionalKey(ThreadRef),
	message: Schema.suspend(() => Message),
	mentioned: Schema.Boolean,
	raw: Schema.Json,
})
export type NormalizedMessage = typeof NormalizedMessage.Type

export const NormalizedMessageUpdated = Schema.Struct({
	provider: ProviderName,
	tenant: TenantId,
	idempotencyKey: IdempotencyKey,
	thread: Schema.suspend(() => Thread),
	directMessageThread: Schema.optionalKey(ThreadRef),
	message: Schema.suspend(() => Message),
	previousMessage: Schema.optionalKey(Schema.suspend(() => Message)),
	raw: Schema.Json,
})
export type NormalizedMessageUpdated = typeof NormalizedMessageUpdated.Type

export const NormalizedMessageDeleted = Schema.Struct({
	provider: ProviderName,
	tenant: TenantId,
	idempotencyKey: IdempotencyKey,
	threadRef: ThreadRef,
	directMessageThread: Schema.optionalKey(ThreadRef),
	messageRef: MessageRef,
	previousMessage: Schema.optionalKey(Schema.suspend(() => Message)),
	deletedAt: Schema.optionalKey(Schema.DateTimeUtc),
	raw: Schema.Json,
})
export type NormalizedMessageDeleted = typeof NormalizedMessageDeleted.Type

export const NormalizedReaction = Schema.Struct({
	provider: ProviderName,
	tenant: TenantId,
	idempotencyKey: IdempotencyKey,
	thread: Schema.suspend(() => Thread),
	directMessageThread: Schema.optionalKey(ThreadRef),
	messageRef: MessageRef,
	message: Schema.optionalKey(Schema.suspend(() => Message)),
	change: ReactionChange,
	emoji: Schema.suspend(() => Emoji),
	rawEmoji: Schema.String,
	actor: Author,
	raw: Schema.Json,
})
export type NormalizedReaction = typeof NormalizedReaction.Type

export const NormalizedConversationStopped = Schema.Struct({
	provider: ProviderName,
	tenant: TenantId,
	idempotencyKey: IdempotencyKey,
	threadRef: ThreadRef,
	directMessageThread: Schema.optionalKey(ThreadRef),
	userId: Schema.optionalKey(UserId),
	raw: Schema.Json,
})
export type NormalizedConversationStopped = typeof NormalizedConversationStopped.Type

export const SubscriptionCreated = Schema.TaggedStruct('SubscriptionCreated', {})
export type SubscriptionCreated = typeof SubscriptionCreated.Type

export const SubscriptionExisting = Schema.TaggedStruct('SubscriptionExisting', {})
export type SubscriptionExisting = typeof SubscriptionExisting.Type

export const SubscriptionTransition = Schema.Union([SubscriptionCreated, SubscriptionExisting])
export type SubscriptionTransition = typeof SubscriptionTransition.Type

export const NewMentionDelivery = Schema.TaggedStruct('NewMentionDelivery', {
	location: Schema.Literals(['channel_root', 'thread']),
})
export type NewMentionDelivery = typeof NewMentionDelivery.Type

export const SubscribedMessageDelivery = Schema.TaggedStruct('SubscribedMessageDelivery', {})
export type SubscribedMessageDelivery = typeof SubscribedMessageDelivery.Type

export const DirectMessageDelivery = Schema.TaggedStruct('DirectMessageDelivery', {})
export type DirectMessageDelivery = typeof DirectMessageDelivery.Type

export const PatternMessageDelivery = Schema.TaggedStruct('PatternMessageDelivery', {})
export type PatternMessageDelivery = typeof PatternMessageDelivery.Type

export const MessageDelivery = Schema.Union([
	NewMentionDelivery,
	SubscribedMessageDelivery,
	DirectMessageDelivery,
	PatternMessageDelivery,
])
export type MessageDelivery = typeof MessageDelivery.Type

export const MessageEvent = Schema.TaggedStruct('MessageEvent', {
	provider: ProviderName,
	tenant: TenantId,
	idempotencyKey: IdempotencyKey,
	thread: Schema.suspend(() => Thread),
	message: Schema.suspend(() => Message),
	delivery: MessageDelivery,
	raw: Schema.Json,
})
export type MessageEvent = typeof MessageEvent.Type

export const MessageUpdatedEvent = Schema.TaggedStruct('MessageUpdatedEvent', {
	provider: ProviderName,
	tenant: TenantId,
	idempotencyKey: IdempotencyKey,
	thread: Schema.suspend(() => Thread),
	message: Schema.suspend(() => Message),
	previousMessage: Schema.optionalKey(Schema.suspend(() => Message)),
	raw: Schema.Json,
})
export type MessageUpdatedEvent = typeof MessageUpdatedEvent.Type

export const MessageDeletedEvent = Schema.TaggedStruct('MessageDeletedEvent', {
	provider: ProviderName,
	tenant: TenantId,
	idempotencyKey: IdempotencyKey,
	threadRef: ThreadRef,
	messageRef: MessageRef,
	previousMessage: Schema.optionalKey(Schema.suspend(() => Message)),
	deletedAt: Schema.optionalKey(Schema.DateTimeUtc),
	raw: Schema.Json,
})
export type MessageDeletedEvent = typeof MessageDeletedEvent.Type

export const ConversationStoppedEvent = Schema.TaggedStruct('ConversationStoppedEvent', {
	provider: ProviderName,
	tenant: TenantId,
	idempotencyKey: IdempotencyKey,
	threadRef: ThreadRef,
	userId: Schema.optionalKey(UserId),
	raw: Schema.Json,
})
export type ConversationStoppedEvent = typeof ConversationStoppedEvent.Type

export const ReactionEvent = Schema.TaggedStruct('ReactionEvent', {
	provider: ProviderName,
	tenant: TenantId,
	idempotencyKey: IdempotencyKey,
	thread: Schema.suspend(() => Thread),
	messageRef: MessageRef,
	message: Schema.optionalKey(Schema.suspend(() => Message)),
	change: ReactionChange,
	emoji: Schema.suspend(() => Emoji),
	rawEmoji: Schema.String,
	actor: Author,
	raw: Schema.Json,
})
export type ReactionEvent = typeof ReactionEvent.Type

export const InboundEvent = Schema.Union([
	MessageEvent,
	MessageUpdatedEvent,
	MessageDeletedEvent,
	ConversationStoppedEvent,
	ReactionEvent,
])
export type InboundEvent = typeof InboundEvent.Type
export const SlackInboundEvent = InboundEvent
