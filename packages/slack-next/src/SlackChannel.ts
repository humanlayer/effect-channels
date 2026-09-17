import { Effect, Schema } from 'effect'

import type { SlackApiError } from './SlackApi'
import { SlackApi } from './SlackApi'
import { SlackChannelInfo, SlackChannelRef, type SlackContent, type SlackSentMessage } from './SlackModels'

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
			Effect.withSpan('slack.channel.post', { attributes: spanAttributes(this.ref) }),
		)
	}

	fetchInfo(): Effect.Effect<SlackChannelInfo, SlackApiError, SlackApi> {
		return Effect.flatMap(SlackApi, (api) => api.getChannelInfo({ channel: this.ref })).pipe(
			Effect.withSpan('slack.channel.fetch_info', { attributes: spanAttributes(this.ref) }),
		)
	}
}
