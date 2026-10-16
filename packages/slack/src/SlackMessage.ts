import { Effect, Schema } from 'effect'

import { SlackApi } from './SlackApi'
import type { SlackApiError } from './SlackApiContract'
import { SlackFile } from './SlackFile'
import { SlackMessageRecord, SlackMessageRef, type SlackReaction, type SlackSentMessageRecord } from './SlackModels'

const spanAttributes = (ref: SlackMessageRef) => ({
	'slack.team_id': ref.teamId,
	'slack.channel_id': ref.channelId,
	'slack.message_ts': ref.messageTs,
})

/** A Slack message you can react to. `SlackApi` returns its fields as a `SlackMessageRecord`. */
export class SlackMessage extends Schema.TaggedClass<SlackMessage>()('SlackMessage', {
	...SlackMessageRecord.fields,
	files: Schema.Array(SlackFile),
}) {
	/** Wraps a message record from `SlackApi`, and each of its files. */
	static fromRecord(record: SlackMessageRecord): SlackMessage {
		return SlackMessage.make({ ...record, files: record.files.map((file) => SlackFile.make(file)) })
	}

	addReaction(reaction: SlackReaction): Effect.Effect<void, SlackApiError, SlackApi> {
		return Effect.flatMap(SlackApi, (api) => api.addReaction({ message: this.ref, reaction })).pipe(
			Effect.withSpan('slack.message.add_reaction', { attributes: spanAttributes(this.ref) }),
		)
	}

	removeReaction(reaction: SlackReaction): Effect.Effect<void, SlackApiError, SlackApi> {
		return Effect.flatMap(SlackApi, (api) => api.removeReaction({ message: this.ref, reaction })).pipe(
			Effect.withSpan('slack.message.remove_reaction', { attributes: spanAttributes(this.ref) }),
		)
	}
}

export const SlackMessages = Schema.Array(SlackMessage)
export type SlackMessages = typeof SlackMessages.Type

export const SlackSentMessage = Schema.Struct({
	ref: SlackMessageRef,
	message: SlackMessage,
})
export type SlackSentMessage = typeof SlackSentMessage.Type

/** Wraps a sent-message record from `SlackApi`. */
export const slackSentMessageFromRecord = (record: SlackSentMessageRecord): SlackSentMessage =>
	SlackSentMessage.make({ ref: record.ref, message: SlackMessage.fromRecord(record.message) })
