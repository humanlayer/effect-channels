import {
	AttachmentRef,
	ChannelInfo,
	Content,
	FileUpload,
	MessagePage,
	StreamChunk,
	ThreadId,
	ThreadPage,
	UserId,
	UserProfile,
} from '@humanlayer/channels'
import type { FileData } from '@humanlayer/channels'
import { Schema } from 'effect'

export const SlackTeamId = Schema.NonEmptyString.pipe(Schema.brand('SlackTeamId'))
export type SlackTeamId = typeof SlackTeamId.Type

export const SlackChannelId = Schema.NonEmptyString.pipe(Schema.brand('SlackChannelId'))
export type SlackChannelId = typeof SlackChannelId.Type

export const SlackMessageTs = Schema.NonEmptyString.pipe(Schema.brand('SlackMessageTs'))
export type SlackMessageTs = typeof SlackMessageTs.Type

export const SlackTenantCreds = Schema.Struct({
	botToken: Schema.Redacted(Schema.String, { disallowJsonEncode: true }),
	refreshToken: Schema.optionalKey(Schema.Redacted(Schema.String, { disallowJsonEncode: true })),
	expiresAt: Schema.optionalKey(Schema.DateTimeUtc),
	botUserId: Schema.optionalKey(Schema.String),
	botId: Schema.optionalKey(Schema.String),
})
export type SlackTenantCreds = typeof SlackTenantCreds.Type

export const SlackLoadCredentialsInput = Schema.Struct({ teamId: SlackTeamId })
export type SlackLoadCredentialsInput = typeof SlackLoadCredentialsInput.Type

export const SlackSaveCredentialsInput = Schema.Struct({ teamId: SlackTeamId, credentials: SlackTenantCreds })
export type SlackSaveCredentialsInput = typeof SlackSaveCredentialsInput.Type

export const SlackFileMetadata = Schema.Struct({
	id: Schema.NonEmptyString,
	name: Schema.optionalKey(Schema.String),
	mimetype: Schema.optionalKey(Schema.String),
	size: Schema.optionalKey(Schema.Natural),
	url_private: Schema.optionalKey(Schema.URLFromString),
	url_private_download: Schema.optionalKey(Schema.URLFromString),
})
export type SlackFileMetadata = typeof SlackFileMetadata.Type

export const SlackMessageSnapshot = Schema.Struct({
	subtype: Schema.optionalKey(Schema.String),
	user: Schema.optionalKey(Schema.NonEmptyString),
	bot_id: Schema.optionalKey(Schema.NonEmptyString),
	text: Schema.optionalKey(Schema.String),
	ts: SlackMessageTs,
	thread_ts: Schema.optionalKey(SlackMessageTs),
	files: Schema.optionalKey(Schema.Array(SlackFileMetadata)),
})
export type SlackMessageSnapshot = typeof SlackMessageSnapshot.Type

export const SlackAppMentionEvent = Schema.Struct({
	type: Schema.Literal('app_mention'),
	user: Schema.optionalKey(Schema.NonEmptyString),
	bot_id: Schema.optionalKey(Schema.NonEmptyString),
	text: Schema.String,
	ts: SlackMessageTs,
	thread_ts: Schema.optionalKey(SlackMessageTs),
	channel: SlackChannelId,
	team: Schema.optionalKey(SlackTeamId),
	files: Schema.optionalKey(Schema.Array(SlackFileMetadata)),
})
export type SlackAppMentionEvent = typeof SlackAppMentionEvent.Type

export const SlackMessageEvent = Schema.Struct({
	type: Schema.Literal('message'),
	subtype: Schema.optionalKey(Schema.String),
	user: Schema.optionalKey(Schema.NonEmptyString),
	bot_id: Schema.optionalKey(Schema.NonEmptyString),
	text: Schema.optionalKey(Schema.String),
	ts: SlackMessageTs,
	thread_ts: Schema.optionalKey(SlackMessageTs),
	channel: SlackChannelId,
	channel_type: Schema.optionalKey(Schema.Literals(['channel', 'group', 'im', 'mpim'])),
	message: Schema.optionalKey(SlackMessageSnapshot),
	previous_message: Schema.optionalKey(SlackMessageSnapshot),
	deleted_ts: Schema.optionalKey(SlackMessageTs),
	files: Schema.optionalKey(Schema.Array(SlackFileMetadata)),
})
export type SlackMessageEvent = typeof SlackMessageEvent.Type

export const SlackReactionItem = Schema.Struct({
	type: Schema.Literal('message'),
	channel: SlackChannelId,
	ts: SlackMessageTs,
})
export type SlackReactionItem = typeof SlackReactionItem.Type

export const SlackReactionAddedEvent = Schema.Struct({
	type: Schema.Literal('reaction_added'),
	user: Schema.NonEmptyString,
	reaction: Schema.NonEmptyString,
	item: SlackReactionItem,
	event_ts: SlackMessageTs,
})
export type SlackReactionAddedEvent = typeof SlackReactionAddedEvent.Type

export const SlackReactionRemovedEvent = Schema.Struct({
	type: Schema.Literal('reaction_removed'),
	user: Schema.NonEmptyString,
	reaction: Schema.NonEmptyString,
	item: SlackReactionItem,
	event_ts: SlackMessageTs,
})
export type SlackReactionRemovedEvent = typeof SlackReactionRemovedEvent.Type

export const SlackAgentSessionStoppedEvent = Schema.Struct({
	type: Schema.Literal('agent_session_stopped'),
	channel_id: SlackChannelId,
	thread_ts: SlackMessageTs,
	user_id: Schema.optionalKey(Schema.NonEmptyString),
})
export type SlackAgentSessionStoppedEvent = typeof SlackAgentSessionStoppedEvent.Type

export const SlackInnerEvent = Schema.Union([
	SlackAppMentionEvent,
	SlackMessageEvent,
	SlackReactionAddedEvent,
	SlackReactionRemovedEvent,
	SlackAgentSessionStoppedEvent,
])
export type SlackInnerEvent = typeof SlackInnerEvent.Type

export const SlackEventCallback = Schema.Struct({
	type: Schema.Literal('event_callback'),
	team_id: SlackTeamId,
	event_id: Schema.NonEmptyString,
	event_time: Schema.Finite,
	event: SlackInnerEvent,
})
export type SlackEventCallback = typeof SlackEventCallback.Type

export const SlackUrlVerification = Schema.Struct({
	type: Schema.Literal('url_verification'),
	challenge: Schema.NonEmptyString,
})
export type SlackUrlVerification = typeof SlackUrlVerification.Type

export const SlackEventsRequest = Schema.Union([SlackEventCallback, SlackUrlVerification])
export type SlackEventsRequest = typeof SlackEventsRequest.Type

export const SlackThreadRef = Schema.Struct({
	teamId: SlackTeamId,
	channelId: SlackChannelId,
	threadTs: SlackMessageTs,
})
export type SlackThreadRef = typeof SlackThreadRef.Type

export const SlackPostMessageInput = Schema.Struct({
	teamId: SlackTeamId,
	channelId: SlackChannelId,
	threadTs: Schema.optionalKey(SlackMessageTs),
	text: Schema.String,
})
export type SlackPostMessageInput = typeof SlackPostMessageInput.Type

export const SlackSentMessage = Schema.Struct({
	channelId: SlackChannelId,
	ts: SlackMessageTs,
	botUserId: Schema.optionalKey(Schema.NonEmptyString),
})
export type SlackSentMessage = typeof SlackSentMessage.Type

export const SlackSentMessageList = Schema.Array(SlackSentMessage)
export type SlackSentMessageList = typeof SlackSentMessageList.Type

export const SlackSessionStatus = Schema.Literals(['processing', 'active', 'suspended'])
export type SlackSessionStatus = typeof SlackSessionStatus.Type

export const SlackSessionStatusInput = Schema.Struct({
	teamId: SlackTeamId,
	channelId: SlackChannelId,
	threadTs: SlackMessageTs,
	status: SlackSessionStatus,
})
export type SlackSessionStatusInput = typeof SlackSessionStatusInput.Type

export const SlackStreamRef = Schema.Struct({
	channelId: SlackChannelId,
	messageTs: SlackMessageTs,
	threadTs: SlackMessageTs,
})
export type SlackStreamRef = typeof SlackStreamRef.Type

export const SlackStartStreamInput = Schema.Struct({
	teamId: SlackTeamId,
	channelId: SlackChannelId,
	threadTs: SlackMessageTs,
	chunks: Schema.Array(StreamChunk),
})
export type SlackStartStreamInput = typeof SlackStartStreamInput.Type

export const SlackAppendStreamInput = Schema.Struct({
	teamId: SlackTeamId,
	stream: SlackStreamRef,
	chunks: Schema.Array(StreamChunk),
})
export type SlackAppendStreamInput = typeof SlackAppendStreamInput.Type

export const SlackStopStreamInput = Schema.Struct({
	teamId: SlackTeamId,
	stream: SlackStreamRef,
	chunks: Schema.Array(StreamChunk),
})
export type SlackStopStreamInput = typeof SlackStopStreamInput.Type

export const SlackUpdateMessageInput = Schema.Struct({
	teamId: SlackTeamId,
	channelId: SlackChannelId,
	ts: SlackMessageTs,
	text: Schema.String,
})
export type SlackUpdateMessageInput = typeof SlackUpdateMessageInput.Type

export const SlackDeleteMessageInput = Schema.Struct({
	teamId: SlackTeamId,
	channelId: SlackChannelId,
	ts: SlackMessageTs,
})
export type SlackDeleteMessageInput = typeof SlackDeleteMessageInput.Type

export const SlackReactionInput = Schema.Struct({
	teamId: SlackTeamId,
	channelId: SlackChannelId,
	ts: SlackMessageTs,
	emoji: Schema.NonEmptyString,
})
export type SlackReactionInput = typeof SlackReactionInput.Type

export const SlackRepliesInput = Schema.Struct({
	teamId: SlackTeamId,
	channelId: SlackChannelId,
	threadTs: SlackMessageTs,
	limit: Schema.optionalKey(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 100 }))),
	cursor: Schema.optionalKey(Schema.String),
	direction: Schema.optionalKey(Schema.Literals(['forward', 'backward'])),
})
export type SlackRepliesInput = typeof SlackRepliesInput.Type

export const SlackHistoryInput = Schema.Struct({
	teamId: SlackTeamId,
	channelId: SlackChannelId,
	before: Schema.optionalKey(SlackMessageTs),
	limit: Schema.optionalKey(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 100 }))),
	cursor: Schema.optionalKey(Schema.String),
	direction: Schema.optionalKey(Schema.Literals(['forward', 'backward'])),
})
export type SlackHistoryInput = typeof SlackHistoryInput.Type

export const SlackChannelInfoInput = Schema.Struct({ teamId: SlackTeamId, channelId: SlackChannelId })
export type SlackChannelInfoInput = typeof SlackChannelInfoInput.Type

export const SlackListThreadsInput = Schema.Struct({
	teamId: SlackTeamId,
	channelId: SlackChannelId,
	limit: Schema.optionalKey(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 100 }))),
	cursor: Schema.optionalKey(Schema.String),
})
export type SlackListThreadsInput = typeof SlackListThreadsInput.Type

export const SlackGetUserInput = Schema.Struct({ teamId: SlackTeamId, userId: UserId })
export type SlackGetUserInput = typeof SlackGetUserInput.Type

export const SlackFileUploadInput = Schema.Struct({
	teamId: SlackTeamId,
	channelId: SlackChannelId,
	threadTs: Schema.optionalKey(SlackMessageTs),
	files: Schema.Array(FileUpload),
})
export type SlackFileUploadInput = typeof SlackFileUploadInput.Type

export const SlackFileDownloadInput = Schema.Struct({ teamId: SlackTeamId, attachment: AttachmentRef })
export type SlackFileDownloadInput = typeof SlackFileDownloadInput.Type

export const SlackOpenDMInput = Schema.Struct({ teamId: SlackTeamId, userId: UserId })
export type SlackOpenDMInput = typeof SlackOpenDMInput.Type

export const SlackPostEphemeralInput = Schema.Struct({
	teamId: SlackTeamId,
	channelId: SlackChannelId,
	threadTs: Schema.optionalKey(SlackMessageTs),
	userId: UserId,
	text: Schema.String,
})
export type SlackPostEphemeralInput = typeof SlackPostEphemeralInput.Type

export const SlackApiInput = Schema.Struct({ teamId: SlackTeamId, method: Schema.NonEmptyString, payload: Schema.Json })
export type SlackApiInput = typeof SlackApiInput.Type

export const SlackApiResponse = Schema.Json
export type SlackApiResponse = typeof SlackApiResponse.Type

export const SlackCreateThreadInput = Schema.Struct({
	teamId: SlackTeamId,
	channelId: SlackChannelId,
	content: Content,
})
export type SlackCreateThreadInput = typeof SlackCreateThreadInput.Type

export const SlackNativePostInput = Schema.Struct({
	threadId: ThreadId,
	payload: Schema.Json,
})
export type SlackNativePostInput = typeof SlackNativePostInput.Type

export const SlackEphemeralInput = Schema.Struct({
	threadId: ThreadId,
	userId: UserId,
	payload: Schema.Json,
})
export type SlackEphemeralInput = typeof SlackEphemeralInput.Type

export const SlackBotIdentity = Schema.Struct({
	botUserId: Schema.optionalKey(Schema.String),
	botId: Schema.optionalKey(Schema.String),
})
export type SlackBotIdentity = typeof SlackBotIdentity.Type

export const SlackChannelAddress = Schema.Struct({
	teamId: SlackTeamId,
	channelId: SlackChannelId,
})
export type SlackChannelAddress = typeof SlackChannelAddress.Type

export const SlackHistoryMessage = Schema.Struct({
	type: Schema.optionalKey(Schema.String),
	subtype: Schema.optionalKey(Schema.String),
	user: Schema.optionalKey(Schema.NonEmptyString),
	bot_id: Schema.optionalKey(Schema.NonEmptyString),
	text: Schema.optionalKey(Schema.String),
	ts: SlackMessageTs,
	thread_ts: Schema.optionalKey(SlackMessageTs),
	reply_count: Schema.optionalKey(Schema.Natural),
	latest_reply: Schema.optionalKey(SlackMessageTs),
	files: Schema.optionalKey(Schema.Array(SlackFileMetadata)),
})
export type SlackHistoryMessage = typeof SlackHistoryMessage.Type

export const SlackResponseMetadata = Schema.Struct({
	next_cursor: Schema.optionalKey(Schema.String),
})
export type SlackResponseMetadata = typeof SlackResponseMetadata.Type

export const SlackOkResponse = Schema.Struct({
	ok: Schema.Boolean,
	error: Schema.optionalKey(Schema.String),
})
export type SlackOkResponse = typeof SlackOkResponse.Type

export const SlackConversationsPageResponse = Schema.Struct({
	ok: Schema.Boolean,
	error: Schema.optionalKey(Schema.String),
	messages: Schema.optionalKey(Schema.Array(SlackHistoryMessage)),
	has_more: Schema.optionalKey(Schema.Boolean),
	response_metadata: Schema.optionalKey(SlackResponseMetadata),
})
export type SlackConversationsPageResponse = typeof SlackConversationsPageResponse.Type

export const SlackChannelSnapshot = Schema.Struct({
	id: SlackChannelId,
	name: Schema.optionalKey(Schema.String),
	is_im: Schema.optionalKey(Schema.Boolean),
	is_mpim: Schema.optionalKey(Schema.Boolean),
	is_private: Schema.optionalKey(Schema.Boolean),
	num_members: Schema.optionalKey(Schema.Natural),
})
export type SlackChannelSnapshot = typeof SlackChannelSnapshot.Type

export const SlackConversationsInfoResponse = Schema.Struct({
	ok: Schema.Boolean,
	error: Schema.optionalKey(Schema.String),
	channel: Schema.optionalKey(SlackChannelSnapshot),
})
export type SlackConversationsInfoResponse = typeof SlackConversationsInfoResponse.Type

export const SlackUserProfileSnapshot = Schema.Struct({
	display_name: Schema.optionalKey(Schema.String),
	real_name: Schema.optionalKey(Schema.String),
	email: Schema.optionalKey(Schema.String),
	image_192: Schema.optionalKey(Schema.URLFromString),
})
export type SlackUserProfileSnapshot = typeof SlackUserProfileSnapshot.Type

export const SlackUserSnapshot = Schema.Struct({
	id: Schema.NonEmptyString,
	name: Schema.optionalKey(Schema.String),
	real_name: Schema.optionalKey(Schema.String),
	is_bot: Schema.optionalKey(Schema.Boolean),
	profile: Schema.optionalKey(SlackUserProfileSnapshot),
})
export type SlackUserSnapshot = typeof SlackUserSnapshot.Type

export const SlackUsersInfoResponse = Schema.Struct({
	ok: Schema.Boolean,
	error: Schema.optionalKey(Schema.String),
	user: Schema.optionalKey(SlackUserSnapshot),
})
export type SlackUsersInfoResponse = typeof SlackUsersInfoResponse.Type

export const SlackSignatureInput = Schema.Struct({
	body: Schema.String,
	timestamp: Schema.String,
	signature: Schema.String,
	signingSecret: Schema.Redacted(Schema.String, { disallowJsonEncode: true }),
})
export type SlackSignatureInput = typeof SlackSignatureInput.Type

export const SlackHmacInput = Schema.Struct({
	secret: Schema.Redacted(Schema.String, { disallowJsonEncode: true }),
	data: Schema.Uint8Array,
})
export type SlackHmacInput = typeof SlackHmacInput.Type

export const SlackPostedMessageSnapshot = Schema.Struct({
	user: Schema.optionalKey(Schema.NonEmptyString),
	bot_id: Schema.optionalKey(Schema.NonEmptyString),
})
export type SlackPostedMessageSnapshot = typeof SlackPostedMessageSnapshot.Type

export const SlackPostMessageResponse = Schema.Struct({
	ok: Schema.Boolean,
	channel: Schema.optionalKey(SlackChannelId),
	ts: Schema.optionalKey(SlackMessageTs),
	message: Schema.optionalKey(SlackPostedMessageSnapshot),
	error: Schema.optionalKey(Schema.String),
})
export type SlackPostMessageResponse = typeof SlackPostMessageResponse.Type

export type SlackRepliesOutput = MessagePage
export type SlackHistoryOutput = MessagePage
export type SlackChannelInfoOutput = ChannelInfo
export type SlackThreadPageOutput = ThreadPage
export type SlackUserOutput = UserProfile
export type SlackFileOutput = FileData
