/**
 * Plain Slack data: refs, ids, content, upload inputs, and the records `SlackApi` takes and returns. Nothing here
 * calls a service, so this module imports neither `SlackApi` nor the callback objects built on it.
 */
import { Schema } from 'effect'

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

/** One task of a Slack plan block: its state, with an optional note while it runs (`details`) or after (`output`). */
export const SlackPlanTask = Schema.Struct({
	id: Schema.NonEmptyString,
	title: Schema.NonEmptyString,
	status: Schema.Literals(['pending', 'in_progress', 'complete', 'error']),
	details: Schema.optionalKey(Schema.NonEmptyString),
	output: Schema.optionalKey(Schema.NonEmptyString),
})
export type SlackPlanTask = typeof SlackPlanTask.Type

/** A task list shown as one Slack plan block, at most 50 tasks. */
export const SlackPlan = Schema.Struct({
	title: Schema.NonEmptyString,
	tasks: Schema.Array(SlackPlanTask).check(Schema.isMaxLength(50)),
})
export type SlackPlan = typeof SlackPlan.Type

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

/** A Slack file as `SlackApi` returns it: the fields of `SlackFile`, without its methods. */
export const SlackFileRecord = Schema.Struct({
	ref: SlackFileRef,
	name: Schema.NullOr(Schema.String),
	contentType: Schema.NullOr(Schema.String),
	size: Schema.NullOr(Schema.Natural),
	downloadUrl: Schema.NullOr(Schema.String),
})
export type SlackFileRecord = typeof SlackFileRecord.Type

const nonEmpty = (value: string | undefined) => (value === undefined || value.length === 0 ? null : value)

/**
 * Normalizes Slack file metadata from webhooks, Web API messages, and upload completion into one record.
 * `url_private_download` wins over `url_private`; absent or empty provider fields become null.
 */
export const slackFileRecordFromMetadata = (teamId: SlackTeamId, metadata: SlackFileMetadata): SlackFileRecord =>
	SlackFileRecord.make({
		ref: SlackFileRef.make({ teamId, fileId: SlackFileId.make(metadata.id) }),
		name: nonEmpty(metadata.name),
		contentType: nonEmpty(metadata.mimetype),
		size: metadata.size ?? null,
		downloadUrl: nonEmpty(metadata.url_private_download) ?? nonEmpty(metadata.url_private),
	})

/** A Slack message as `SlackApi` returns it: the fields of `SlackMessage`, without its methods. */
export const SlackMessageRecord = Schema.Struct({
	ref: SlackMessageRef,
	thread: SlackThreadRef,
	author: SlackParticipant,
	/**
	 * The author's workspace, from the message's `user_team`. Slack sends it in Slack Connect channels, where an
	 * author from another workspace can post; when absent, treat the author as from the installation's workspace.
	 */
	authorTeamId: Schema.optionalKey(SlackTeamId),
	content: SlackContent,
	files: Schema.Array(SlackFileRecord),
	metadata: SlackMetadata,
})
export type SlackMessageRecord = typeof SlackMessageRecord.Type

export const SlackMessageRecords = Schema.Array(SlackMessageRecord)
export type SlackMessageRecords = typeof SlackMessageRecords.Type

export const SlackParticipants = Schema.Array(SlackParticipant)
export type SlackParticipants = typeof SlackParticipants.Type

/** A message `SlackApi` posted, as it returns it. */
export const SlackSentMessageRecord = Schema.Struct({
	ref: SlackMessageRef,
	message: SlackMessageRecord,
})
export type SlackSentMessageRecord = typeof SlackSentMessageRecord.Type

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
