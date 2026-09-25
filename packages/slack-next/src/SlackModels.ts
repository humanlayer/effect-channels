import { Effect, Schema, type Stream } from 'effect'

import type { SlackApiError, SlackFileDownloadError } from './SlackApi'
import { SlackApi, SlackDownloadFileBytesRequest, SlackDownloadFileRequest } from './SlackApi'
import { SlackChannelId, SlackMessageTs, SlackTeamId } from './SlackIdentity'
import type { SlackFileMetadata } from './SlackWebhookEventSchemas'

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

export const SlackFileId = Schema.NonEmptyString.pipe(Schema.brand('SlackFileId'))
export type SlackFileId = typeof SlackFileId.Type

export const SlackFileRef = Schema.Struct({
	teamId: SlackTeamId,
	fileId: SlackFileId,
})
export type SlackFileRef = typeof SlackFileRef.Type

export const SlackDownloadBytesOptions = Schema.Struct({
	maxBytes: Schema.Int.check(Schema.isGreaterThan(0)),
})
export type SlackDownloadBytesOptions = typeof SlackDownloadBytesOptions.Type

/** Upper bound for one in-memory upload; Slack's upload setup requires the exact byte length up front. */
export const slackMaxUploadBytes = 50 * 1024 * 1024

export const SlackUploadBytes = Schema.Uint8Array.check(
	Schema.makeFilter((bytes: Uint8Array) =>
		bytes.byteLength > 0 && bytes.byteLength <= slackMaxUploadBytes
			? undefined
			: `expected between 1 and ${slackMaxUploadBytes} bytes`,
	),
)

export const SlackUploadFileInput = Schema.Struct({
	filename: Schema.NonEmptyString,
	bytes: SlackUploadBytes,
	title: Schema.optionalKey(Schema.NonEmptyString),
	initialComment: Schema.optionalKey(Schema.NonEmptyString),
})
export type SlackUploadFileInput = typeof SlackUploadFileInput.Type

export class SlackFile extends Schema.TaggedClass<SlackFile>()('SlackFile', {
	ref: SlackFileRef,
	name: Schema.NullOr(Schema.String),
	contentType: Schema.NullOr(Schema.String),
	size: Schema.NullOr(Schema.Natural),
	downloadUrl: Schema.NullOr(Schema.String),
}) {
	/** Streams the file with the bot token. The stream is not buffered. */
	download(): Effect.Effect<Stream.Stream<Uint8Array, SlackApiError>, SlackFileDownloadError, SlackApi> {
		return Effect.flatMap(SlackApi, (api) =>
			api.downloadFile(
				SlackDownloadFileRequest.make({
					file: this.ref,
					downloadUrl: this.downloadUrl,
					contentType: this.contentType,
					size: this.size,
				}),
			),
		).pipe(
			Effect.withSpan('slack.file.download', {
				attributes: { 'slack.team_id': this.ref.teamId, 'slack.file_id': this.ref.fileId },
			}),
		)
	}

	/** Buffers the file, failing before `maxBytes` is exceeded. */
	downloadBytes(options: SlackDownloadBytesOptions): Effect.Effect<Uint8Array, SlackFileDownloadError, SlackApi> {
		return Effect.flatMap(SlackApi, (api) =>
			api.downloadFileBytes(
				SlackDownloadFileBytesRequest.make({
					file: this.ref,
					downloadUrl: this.downloadUrl,
					contentType: this.contentType,
					size: this.size,
					maxBytes: options.maxBytes,
				}),
			),
		).pipe(
			Effect.withSpan('slack.file.download_bytes', {
				attributes: {
					'slack.team_id': this.ref.teamId,
					'slack.file_id': this.ref.fileId,
					'slack.max_bytes': options.maxBytes,
				},
			}),
		)
	}
}

const nonEmpty = (value: string | undefined) => (value === undefined || value.length === 0 ? null : value)

/**
 * Normalizes Slack file metadata from webhooks, Web API messages, and upload completion into one public value.
 * `url_private_download` wins over `url_private`; absent or empty provider fields become null.
 */
export const slackFileFromMetadata = (teamId: SlackTeamId, metadata: SlackFileMetadata) =>
	SlackFile.make({
		ref: SlackFileRef.make({ teamId, fileId: SlackFileId.make(metadata.id) }),
		name: nonEmpty(metadata.name),
		contentType: nonEmpty(metadata.mimetype),
		size: metadata.size ?? null,
		downloadUrl: nonEmpty(metadata.url_private_download) ?? nonEmpty(metadata.url_private),
	})

export class SlackMessage extends Schema.TaggedClass<SlackMessage>()('SlackMessage', {
	ref: SlackMessageRef,
	thread: SlackThreadRef,
	author: SlackParticipant,
	content: SlackContent,
	files: Schema.Array(SlackFile),
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
