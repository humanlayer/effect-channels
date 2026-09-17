import { Context, Effect, Schema, Stream } from 'effect'

import { SlackMessageTs, SlackTeamId } from './SlackIdentity'
import type {
	SlackChannelInfo,
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
	SlackMessageCount,
	SlackMessageRef,
	SlackReaction,
	SlackThreadRef,
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
])
export type SlackApiOperation = typeof SlackApiOperation.Type

export class SlackApiError extends Schema.TaggedError<SlackApiError>()('SlackApiError', {
	operation: SlackApiOperation,
	message: Schema.String,
}) {}

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
	}
>()('@humanlayer/channels-slack-next/SlackApi') {}
