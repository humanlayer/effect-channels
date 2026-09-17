import { Effect, Schema } from 'effect'

import type { SlackApiError } from './SlackApi'
import { SlackApi } from './SlackApi'
import { SlackChannelId, SlackMessageTs, SlackTeamId } from './SlackIdentity'

export const SlackUserId = Schema.NonEmptyString.pipe(Schema.brand('SlackUserId'))
export type SlackUserId = typeof SlackUserId.Type

export const SlackMessageCount = Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 100 }))
export type SlackMessageCount = typeof SlackMessageCount.Type

export const SlackReaction = Schema.NonEmptyString.pipe(Schema.brand('SlackReaction'))
export type SlackReaction = typeof SlackReaction.Type

export const SlackChannelRef = Schema.Struct({
	teamId: SlackTeamId,
	channelId: SlackChannelId,
	isDm: Schema.Boolean,
})
export type SlackChannelRef = typeof SlackChannelRef.Type

export const SlackThreadRef = Schema.Struct({
	teamId: SlackTeamId,
	channelId: SlackChannelId,
	threadTs: SlackMessageTs,
	isDm: Schema.Boolean,
})
export type SlackThreadRef = typeof SlackThreadRef.Type

export const SlackMessageRef = Schema.Struct({
	teamId: SlackTeamId,
	channelId: SlackChannelId,
	messageTs: SlackMessageTs,
})
export type SlackMessageRef = typeof SlackMessageRef.Type

export const SlackParticipant = Schema.Struct({
	userId: SlackUserId,
	teamId: Schema.optionalKey(SlackTeamId),
	userName: Schema.String,
	fullName: Schema.String,
	isBot: Schema.Boolean,
	isMe: Schema.Boolean,
})
export type SlackParticipant = typeof SlackParticipant.Type

export const SlackMetadataValue = Schema.Json
export type SlackMetadataValue = typeof SlackMetadataValue.Type

export const SlackMetadata = Schema.Record(Schema.String, SlackMetadataValue)
export type SlackMetadata = typeof SlackMetadata.Type

export const SlackPlainTextContent = Schema.TaggedStruct('SlackPlainTextContent', {
	text: Schema.String,
})
export type SlackPlainTextContent = typeof SlackPlainTextContent.Type

export const SlackMarkdownContent = Schema.TaggedStruct('SlackMarkdownContent', {
	markdown: Schema.String,
})
export type SlackMarkdownContent = typeof SlackMarkdownContent.Type

export const SlackContent = Schema.Union([SlackPlainTextContent, SlackMarkdownContent])
export type SlackContent = typeof SlackContent.Type

export class SlackMessage extends Schema.TaggedClass<SlackMessage>()('SlackMessage', {
	ref: SlackMessageRef,
	thread: SlackThreadRef,
	author: SlackParticipant,
	content: SlackContent,
	metadata: SlackMetadata,
}) {
	addReaction(reaction: SlackReaction): Effect.Effect<void, SlackApiError, SlackApi> {
		return Effect.flatMap(SlackApi, (api) => api.addReaction({ message: this.ref, reaction })).pipe(
			Effect.withSpan('slack.message.add_reaction', {
				attributes: {
					'slack.team_id': this.ref.teamId,
					'slack.channel_id': this.ref.channelId,
					'slack.message_ts': this.ref.messageTs,
				},
			}),
		)
	}

	removeReaction(reaction: SlackReaction): Effect.Effect<void, SlackApiError, SlackApi> {
		return Effect.flatMap(SlackApi, (api) => api.removeReaction({ message: this.ref, reaction })).pipe(
			Effect.withSpan('slack.message.remove_reaction', {
				attributes: {
					'slack.team_id': this.ref.teamId,
					'slack.channel_id': this.ref.channelId,
					'slack.message_ts': this.ref.messageTs,
				},
			}),
		)
	}
}

export const SlackMessages = Schema.Array(SlackMessage)
export type SlackMessages = typeof SlackMessages.Type

export const SlackParticipants = Schema.Array(SlackParticipant)
export type SlackParticipants = typeof SlackParticipants.Type

export const SlackSentMessage = Schema.Struct({
	ref: SlackMessageRef,
	message: SlackMessage,
})
export type SlackSentMessage = typeof SlackSentMessage.Type

export const SlackThreadInfo = Schema.Struct({
	thread: SlackThreadRef,
	title: Schema.optionalKey(Schema.String),
})
export type SlackThreadInfo = typeof SlackThreadInfo.Type

export const SlackChannelInfo = Schema.Struct({
	channel: SlackChannelRef,
	name: Schema.optionalKey(Schema.String),
	memberCount: Schema.optionalKey(Schema.Natural),
})
export type SlackChannelInfo = typeof SlackChannelInfo.Type
