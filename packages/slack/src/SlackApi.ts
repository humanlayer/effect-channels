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
	SlackPlan,
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

/** Replace the text of a message the bot posted. */
export const SlackUpdateMessageRequest = Schema.Struct({ message: SlackMessageRef, content: SlackContent })
export type SlackUpdateMessageRequest = typeof SlackUpdateMessageRequest.Type

/** Remove a message the bot posted. */
export const SlackDeleteMessageRequest = Schema.Struct({ message: SlackMessageRef })
export type SlackDeleteMessageRequest = typeof SlackDeleteMessageRequest.Type

/** Show a status line under the thread, such as `Running tests…`, while the agent works. */
export const SlackThreadStatusRequest = Schema.Struct({
	thread: SlackThreadRef,
	status: Schema.NonEmptyString,
})
export type SlackThreadStatusRequest = typeof SlackThreadStatusRequest.Type

/** Post a plan to a thread as a message holding one plan block. */
export const SlackPostPlanRequest = Schema.Struct({ thread: SlackThreadRef, plan: SlackPlan })
export type SlackPostPlanRequest = typeof SlackPostPlanRequest.Type

/** Replace the plan a plan message shows. */
export const SlackUpdatePlanRequest = Schema.Struct({ message: SlackMessageRef, plan: SlackPlan })
export type SlackUpdatePlanRequest = typeof SlackUpdatePlanRequest.Type

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
	'update_message',
	'delete_message',
	'set_thread_status',
	'clear_thread_status',
	'start_typing',
	'stream',
	'post_plan',
	'update_plan',
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

/** Slack's answers when it refuses the bot token itself, not one request. */
const rejectedTokenErrors: ReadonlySet<string> = new Set([
	'account_inactive',
	'invalid_auth',
	'not_authed',
	'token_expired',
	'token_revoked',
])

/** Whether Slack refused the bot token itself. Another attempt with the same token cannot succeed. */
export const isSlackTokenRejected = (error: SlackApiError) => rejectedTokenErrors.has(error.message)

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
		/** `chat.update`: replace the text of a message the bot posted. */
		readonly updateMessage: (request: SlackUpdateMessageRequest) => Effect.Effect<void, SlackApiError>
		/** `chat.delete`: remove a message the bot posted. */
		readonly deleteMessage: (request: SlackDeleteMessageRequest) => Effect.Effect<void, SlackApiError>
		readonly startTyping: (request: SlackThreadRequest) => Effect.Effect<void, SlackApiError>
		/** `assistant.threads.setStatus`: show a status line under the thread while the agent works. */
		readonly setThreadStatus: (request: SlackThreadStatusRequest) => Effect.Effect<void, SlackApiError>
		/** `agents.sessions.setStatus` `active`: clear the thread's status line and typing indicator. */
		readonly clearThreadStatus: (request: SlackThreadRequest) => Effect.Effect<void, SlackApiError>
		readonly stream: <E, R>(
			thread: SlackThreadRef,
			chunks: Stream.Stream<SlackStreamChunk, E, R>,
		) => Effect.Effect<SlackSentMessage, SlackApiError | E, R>
		/** `chat.postMessage` with one plan block: post a task list to a thread. */
		readonly postPlanToThread: (request: SlackPostPlanRequest) => Effect.Effect<SlackMessageRef, SlackApiError>
		/** `chat.update` with one plan block: replace the whole task list a plan message shows. */
		readonly updatePlan: (request: SlackUpdatePlanRequest) => Effect.Effect<void, SlackApiError>
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
>()('@humanlayer/channels-slack/SlackApi') {}
