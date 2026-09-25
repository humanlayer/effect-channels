import { Context, Effect, Schema, Stream } from 'effect'

import { SlackMessageTs, SlackTeamId } from './SlackIdentity'
import type {
	SlackChannelInfo,
	SlackFile,
	SlackMessage,
	SlackMessages,
	SlackParticipant,
	SlackParticipants,
	SlackSentMessage,
	SlackThreadInfo,
} from './SlackModels'
import {
	SlackChannelRef,
	SlackContent,
	SlackDownloadBytesOptions,
	SlackFileRef,
	SlackMessageCount,
	SlackMessageRef,
	SlackReaction,
	SlackThreadRef,
	SlackUploadFileInput,
} from './SlackModels'
import type { SlackStreamChunk } from './SlackStreamChunk'

export const SlackListChannelMessagesBeforeThreadRequest = Schema.Struct({
	thread: SlackThreadRef,
	count: SlackMessageCount,
})
export type SlackListChannelMessagesBeforeThreadRequest = typeof SlackListChannelMessagesBeforeThreadRequest.Type

export const SlackPostToThreadRequest = Schema.Struct({ thread: SlackThreadRef, content: SlackContent })
export type SlackPostToThreadRequest = typeof SlackPostToThreadRequest.Type

export const SlackPostToChannelRequest = Schema.Struct({ channel: SlackChannelRef, content: SlackContent })
export type SlackPostToChannelRequest = typeof SlackPostToChannelRequest.Type

export const SlackThreadRequest = Schema.Struct({ thread: SlackThreadRef })
export type SlackThreadRequest = typeof SlackThreadRequest.Type

export const SlackChannelRequest = Schema.Struct({ channel: SlackChannelRef })
export type SlackChannelRequest = typeof SlackChannelRequest.Type

export const SlackReactionRequest = Schema.Struct({ message: SlackMessageRef, reaction: SlackReaction })
export type SlackReactionRequest = typeof SlackReactionRequest.Type

export const SlackParticipantRequest = Schema.Struct({
	teamId: SlackTeamId,
	userId: Schema.optionalKey(Schema.NonEmptyString),
	botId: Schema.optionalKey(Schema.NonEmptyString),
})
export type SlackParticipantRequest = typeof SlackParticipantRequest.Type

export const SlackMessageRequest = Schema.Struct({ thread: SlackThreadRef, message: SlackMessageRef })
export type SlackMessageRequest = typeof SlackMessageRequest.Type

export const SlackResolveReactionThreadRequest = Schema.Struct({ message: SlackMessageRef })
export type SlackResolveReactionThreadRequest = typeof SlackResolveReactionThreadRequest.Type

export const SlackUploadFileToChannelRequest = Schema.Struct({
	channel: SlackChannelRef,
	input: SlackUploadFileInput,
})
export type SlackUploadFileToChannelRequest = typeof SlackUploadFileToChannelRequest.Type

export const SlackUploadFileToThreadRequest = Schema.Struct({
	thread: SlackThreadRef,
	input: SlackUploadFileInput,
})
export type SlackUploadFileToThreadRequest = typeof SlackUploadFileToThreadRequest.Type

export const SlackDownloadFileRequest = Schema.Struct({
	file: SlackFileRef,
	downloadUrl: Schema.NullOr(Schema.String),
	contentType: Schema.NullOr(Schema.String),
	size: Schema.NullOr(Schema.Natural),
})
export type SlackDownloadFileRequest = typeof SlackDownloadFileRequest.Type

export const SlackDownloadFileBytesRequest = Schema.Struct({
	...SlackDownloadFileRequest.fields,
	...SlackDownloadBytesOptions.fields,
})
export type SlackDownloadFileBytesRequest = typeof SlackDownloadFileBytesRequest.Type

export const SlackApiOperation = Schema.Literals([
	'list_participants',
	'list_thread_messages',
	'list_channel_messages_before_thread',
	'post',
	'post_to_channel',
	'start_typing',
	'stream',
	'add_reaction',
	'remove_reaction',
	'resolve_participant',
	'get_message',
	'resolve_reaction_thread',
	'get_thread_info',
	'get_channel_info',
	'get_file_upload_url',
	'upload_file_bytes',
	'complete_file_upload',
	'download_file',
])
export type SlackApiOperation = typeof SlackApiOperation.Type

export class SlackApiError extends Schema.TaggedError<SlackApiError>()('SlackApiError', {
	operation: SlackApiOperation,
	message: Schema.String,
}) {}

export const SlackFileScope = Schema.Literals(['files:read', 'files:write'])
export type SlackFileScope = typeof SlackFileScope.Type

/** Slack refused a file operation because the bot token lacks the named scope. Reinstall the app after adding it. */
export class SlackFileAuthorizationError extends Schema.TaggedError<SlackFileAuthorizationError>()(
	'SlackFileAuthorizationError',
	{
		operation: SlackApiOperation,
		requiredScope: SlackFileScope,
		retryable: Schema.Boolean,
	},
) {}

export class SlackFileSizeLimitExceeded extends Schema.TaggedError<SlackFileSizeLimitExceeded>()(
	'SlackFileSizeLimitExceeded',
	{
		maxBytes: Schema.Int,
		observedBytes: Schema.Natural,
		source: Schema.Literals(['declared_size', 'content_length', 'received_bytes']),
	},
) {}

export const SlackFileUploadError = Schema.Union([SlackApiError, SlackFileAuthorizationError])
export type SlackFileUploadError = typeof SlackFileUploadError.Type

export const SlackFileDownloadError = Schema.Union([
	SlackApiError,
	SlackFileAuthorizationError,
	SlackFileSizeLimitExceeded,
])
export type SlackFileDownloadError = typeof SlackFileDownloadError.Type

/** Application-wide Slack operations. Behavior-bearing Slack values pass their stored refs here. */
export class SlackApi extends Context.Service<
	SlackApi,
	{
		readonly listParticipants: (request: SlackThreadRequest) => Effect.Effect<SlackParticipants, SlackApiError>
		readonly listThreadMessages: (request: SlackThreadRequest) => Effect.Effect<SlackMessages, SlackApiError>
		readonly listChannelMessagesBeforeThread: (
			request: SlackListChannelMessagesBeforeThreadRequest,
		) => Effect.Effect<SlackMessages, SlackApiError>
		readonly postToThread: (request: SlackPostToThreadRequest) => Effect.Effect<SlackSentMessage, SlackApiError>
		readonly postToChannel: (request: SlackPostToChannelRequest) => Effect.Effect<SlackSentMessage, SlackApiError>
		readonly startTyping: (request: SlackThreadRequest) => Effect.Effect<void, SlackApiError>
		readonly stream: <E, R>(
			thread: SlackThreadRef,
			chunks: Stream.Stream<SlackStreamChunk, E, R>,
		) => Effect.Effect<SlackSentMessage, SlackApiError | E, R>
		readonly addReaction: (request: SlackReactionRequest) => Effect.Effect<void, SlackApiError>
		readonly removeReaction: (request: SlackReactionRequest) => Effect.Effect<void, SlackApiError>
		readonly resolveParticipant: (
			request: SlackParticipantRequest,
		) => Effect.Effect<SlackParticipant, SlackApiError>
		readonly getMessage: (request: SlackMessageRequest) => Effect.Effect<SlackMessage, SlackApiError>
		readonly resolveReactionThread: (
			request: SlackResolveReactionThreadRequest,
		) => Effect.Effect<SlackMessageTs, SlackApiError>
		readonly getThreadInfo: (request: SlackThreadRequest) => Effect.Effect<SlackThreadInfo, SlackApiError>
		readonly getChannelInfo: (request: SlackChannelRequest) => Effect.Effect<SlackChannelInfo, SlackApiError>
		readonly uploadFileToChannel: (
			request: SlackUploadFileToChannelRequest,
		) => Effect.Effect<SlackFile, SlackFileUploadError>
		readonly uploadFileToThread: (
			request: SlackUploadFileToThreadRequest,
		) => Effect.Effect<SlackFile, SlackFileUploadError>
		readonly downloadFile: (
			request: SlackDownloadFileRequest,
		) => Effect.Effect<Stream.Stream<Uint8Array, SlackApiError>, SlackFileDownloadError>
		readonly downloadFileBytes: (
			request: SlackDownloadFileBytesRequest,
		) => Effect.Effect<Uint8Array, SlackFileDownloadError>
	}
>()('@humanlayer/channels-slack-next/SlackApi') {}
