import { Effect, Schema } from 'effect'

import type { SlackApiError, SlackFileUploadError } from './SlackApi'
import { SlackApi } from './SlackApi'
import { SlackFile } from './SlackFile'
import { type SlackSentMessage, slackSentMessageFromRecord } from './SlackMessage'
import { type SlackChannelInfo, SlackChannelRef, type SlackContent, type SlackUploadFileInput } from './SlackModels'

const spanAttributes = (channel: SlackChannelRef) => ({
	'slack.team_id': channel.teamId,
	'slack.channel_id': channel.channelId,
	'slack.is_dm': channel.isDm,
})

export class SlackChannel extends Schema.TaggedClass<SlackChannel>()('SlackChannel', {
	ref: SlackChannelRef,
}) {
	post(content: SlackContent): Effect.Effect<SlackSentMessage, SlackApiError, SlackApi> {
		return Effect.flatMap(SlackApi, (api) => api.postToChannel({ channel: this.ref, content })).pipe(
			Effect.map(slackSentMessageFromRecord),
			Effect.withSpan('slack.channel.post', { attributes: spanAttributes(this.ref) }),
		)
	}

	/** Uploads bytes and shares the file at the channel root. */
	uploadFile(input: SlackUploadFileInput): Effect.Effect<SlackFile, SlackFileUploadError, SlackApi> {
		return Effect.flatMap(SlackApi, (api) => api.uploadFileToChannel({ channel: this.ref, input })).pipe(
			Effect.map((record) => SlackFile.make(record)),
			Effect.withSpan('slack.channel.upload_file', { attributes: spanAttributes(this.ref) }),
		)
	}

	fetchInfo(): Effect.Effect<SlackChannelInfo, SlackApiError, SlackApi> {
		return Effect.flatMap(SlackApi, (api) => api.getChannelInfo({ channel: this.ref })).pipe(
			Effect.withSpan('slack.channel.fetch_info', { attributes: spanAttributes(this.ref) }),
		)
	}
}
