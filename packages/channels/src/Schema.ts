import { Schema } from 'effect'

export const providerNames = ['slack', 'github', 'linear', 'discord'] as const

export const ProviderName = Schema.Literals(providerNames)
export type ProviderName = typeof ProviderName.Type

export const SourceName = Schema.Literals(providerNames)
export type SourceName = typeof SourceName.Type

export const OrgId = Schema.NonEmptyString.pipe(Schema.brand('OrgId'))
export type OrgId = typeof OrgId.Type

export const TenantId = Schema.NonEmptyString.pipe(Schema.brand('TenantId'))
export type TenantId = typeof TenantId.Type

export const ThreadId = Schema.NonEmptyString.pipe(Schema.brand('ThreadId'))
export type ThreadId = typeof ThreadId.Type

export const ChannelId = Schema.NonEmptyString.pipe(Schema.brand('ChannelId'))
export type ChannelId = typeof ChannelId.Type

export const MessageRef = Schema.NonEmptyString.pipe(Schema.brand('MessageRef'))
export type MessageRef = typeof MessageRef.Type

export const UserId = Schema.NonEmptyString.pipe(Schema.brand('UserId'))
export type UserId = typeof UserId.Type

export const RuntimeInstanceId = Schema.NonEmptyString.pipe(Schema.brand('RuntimeInstanceId'))
export type RuntimeInstanceId = typeof RuntimeInstanceId.Type

export const ConversationRunId = Schema.NonEmptyString.pipe(Schema.brand('ConversationRunId'))
export type ConversationRunId = typeof ConversationRunId.Type

export const IdempotencyKey = Schema.String.check(Schema.isPattern(/^evt_[a-f0-9]{32}$/)).pipe(
	Schema.brand('IdempotencyKey'),
)
export type IdempotencyKey = typeof IdempotencyKey.Type

export const Author = Schema.Struct({
	userId: UserId,
	userName: Schema.String,
	fullName: Schema.String,
	isBot: Schema.Union([Schema.Boolean, Schema.Literal('unknown')]),
	isMe: Schema.Boolean,
})
export type Author = typeof Author.Type

export const UserProfile = Schema.Struct({
	author: Author,
	email: Schema.optionalKey(Schema.String),
	avatarUrl: Schema.optionalKey(Schema.URLFromString),
})
export type UserProfile = typeof UserProfile.Type

export const MessageMetadata = Schema.Struct({
	sentAt: Schema.DateTimeUtc,
	editedAt: Schema.optionalKey(Schema.DateTimeUtc),
})
export type MessageMetadata = typeof MessageMetadata.Type

export const AttachmentRef = Schema.Struct({
	provider: ProviderName,
	tenant: TenantId,
	id: Schema.NonEmptyString,
	kind: Schema.NonEmptyString,
	name: Schema.optionalKey(Schema.String),
	mimeType: Schema.optionalKey(Schema.String),
	size: Schema.optionalKey(Schema.Natural),
	width: Schema.optionalKey(Schema.Natural),
	height: Schema.optionalKey(Schema.Natural),
	providerLocator: Schema.Json,
})
export type AttachmentRef = typeof AttachmentRef.Type

export const FileUpload = Schema.Struct({
	data: Schema.Uint8ArrayFromBase64,
	filename: Schema.NonEmptyString,
	mimeType: Schema.optionalKey(Schema.String),
})
export type FileUpload = typeof FileUpload.Type

export const FileData = Schema.Uint8ArrayFromBase64
export type FileData = typeof FileData.Type

export const ChannelRef = Schema.Struct({
	id: ChannelId,
	provider: ProviderName,
	tenant: TenantId,
	isDm: Schema.Boolean,
})
export type ChannelRef = typeof ChannelRef.Type

export const ThreadRef = Schema.Struct({
	id: ThreadId,
	channel: ChannelRef,
	isNew: Schema.Boolean,
})
export type ThreadRef = typeof ThreadRef.Type

export const TypingCapabilities = Schema.Struct({
	thread: Schema.Boolean,
	channel: Schema.Boolean,
})
export type TypingCapabilities = typeof TypingCapabilities.Type

export const HistoryCapabilities = Schema.Struct({
	thread: Schema.Boolean,
	channelMessages: Schema.Boolean,
	channelThreads: Schema.Boolean,
})
export type HistoryCapabilities = typeof HistoryCapabilities.Type

export const ReactionCapabilities = Schema.Struct({
	add: Schema.Boolean,
	remove: Schema.Boolean,
	events: Schema.Boolean,
})
export type ReactionCapabilities = typeof ReactionCapabilities.Type

export const FileCapabilities = Schema.Struct({
	read: Schema.Boolean,
	upload: Schema.Boolean,
})
export type FileCapabilities = typeof FileCapabilities.Type

export const DirectMessageCapabilities = Schema.Struct({
	ingress: Schema.Boolean,
	open: Schema.Boolean,
})
export type DirectMessageCapabilities = typeof DirectMessageCapabilities.Type

export const EphemeralCapabilities = Schema.Struct({
	native: Schema.Boolean,
	dmFallback: Schema.Boolean,
})
export type EphemeralCapabilities = typeof EphemeralCapabilities.Type

export const Capabilities = Schema.Struct({
	threadPost: Schema.Boolean,
	channelPost: Schema.Boolean,
	edit: Schema.Boolean,
	delete: Schema.Boolean,
	streaming: Schema.Literals(['native', 'post_and_edit', 'buffered', 'unsupported']),
	typing: TypingCapabilities,
	history: HistoryCapabilities,
	reactions: ReactionCapabilities,
	files: FileCapabilities,
	actions: Schema.Boolean,
	threadInfo: Schema.Boolean,
	channelInfo: Schema.Boolean,
	createThread: Schema.Boolean,
	directMessages: DirectMessageCapabilities,
	ephemeral: EphemeralCapabilities,
	subject: Schema.Boolean,
})
export type Capabilities = typeof Capabilities.Type

export const ThreadInfo = Schema.Struct({
	thread: ThreadRef,
	title: Schema.optionalKey(Schema.String),
})
export type ThreadInfo = typeof ThreadInfo.Type

export const ChannelInfo = Schema.Struct({
	channel: ChannelRef,
	name: Schema.optionalKey(Schema.String),
	memberCount: Schema.optionalKey(Schema.Natural),
})
export type ChannelInfo = typeof ChannelInfo.Type

export const MessageSubject = Schema.Struct({
	provider: ProviderName,
	id: Schema.NonEmptyString,
	kind: Schema.NonEmptyString,
	title: Schema.String,
	status: Schema.optionalKey(Schema.String),
	url: Schema.optionalKey(Schema.URLFromString),
})
export type MessageSubject = typeof MessageSubject.Type
