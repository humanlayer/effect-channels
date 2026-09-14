import { Schema } from 'effect'

import { SlackChannelId, SlackMessageTs, SlackTeamId } from './SlackIdentity'

export const SlackWebhookHeaders = Schema.Struct({
	'x-slack-request-timestamp': Schema.NonEmptyString,
	'x-slack-signature': Schema.NonEmptyString,
})
export type SlackWebhookHeaders = typeof SlackWebhookHeaders.Type

export const SlackWebhookEnvelope = Schema.Struct({
	type: Schema.NonEmptyString,
})
export type SlackWebhookEnvelope = typeof SlackWebhookEnvelope.Type

export const SlackUrlVerification = Schema.Struct({
	type: Schema.Literal('url_verification'),
	challenge: Schema.NonEmptyString,
})
export type SlackUrlVerification = typeof SlackUrlVerification.Type

export const SlackEventCallbackEnvelope = Schema.Struct({
	type: Schema.Literal('event_callback'),
	team_id: SlackTeamId,
	event_id: Schema.NonEmptyString,
	event_time: Schema.Finite,
	event: Schema.Struct({
		type: Schema.NonEmptyString,
	}),
})
export type SlackEventCallbackEnvelope = typeof SlackEventCallbackEnvelope.Type

export const SlackAppMentionEvent = Schema.Struct({
	type: Schema.Literal('app_mention'),
	user: Schema.optionalKey(Schema.NonEmptyString),
	bot_id: Schema.optionalKey(Schema.NonEmptyString),
	text: Schema.String,
	ts: SlackMessageTs,
	thread_ts: Schema.optionalKey(SlackMessageTs),
	channel: SlackChannelId,
})
export type SlackAppMentionEvent = typeof SlackAppMentionEvent.Type

export const SlackAppMentionCallback = Schema.Struct({
	type: Schema.Literal('event_callback'),
	team_id: SlackTeamId,
	event_id: Schema.NonEmptyString,
	event_time: Schema.Finite,
	event: SlackAppMentionEvent,
})
export type SlackAppMentionCallback = typeof SlackAppMentionCallback.Type
