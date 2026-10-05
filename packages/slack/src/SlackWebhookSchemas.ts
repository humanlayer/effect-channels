import { Schema } from 'effect'

import { SlackTeamId } from './SlackIdentity'
import {
	SlackAppMentionEvent,
	SlackAgentSessionStoppedEvent,
	SlackMessageDeletedEvent,
	SlackMessageEvent,
	SlackMessageUpdatedEvent,
	SlackReactionAddedEvent,
	SlackReactionRemovedEvent,
} from './SlackWebhookEventSchemas'

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

export const SlackEventEnvelope = Schema.Struct({
	type: Schema.Literal('event_callback'),
	team_id: SlackTeamId,
	event_id: Schema.NonEmptyString,
	event_time: Schema.Finite,
	event: Schema.Struct({
		type: Schema.NonEmptyString,
	}),
})
export type SlackEventEnvelope = typeof SlackEventEnvelope.Type

export const SlackAppMentionEnvelope = Schema.Struct({
	type: Schema.Literal('event_callback'),
	team_id: SlackTeamId,
	event_id: Schema.NonEmptyString,
	event_time: Schema.Finite,
	event: SlackAppMentionEvent,
})
export type SlackAppMentionEnvelope = typeof SlackAppMentionEnvelope.Type

export const SlackAgentSessionStoppedEnvelope = Schema.Struct({
	type: Schema.Literal('event_callback'),
	team_id: SlackTeamId,
	event_id: Schema.NonEmptyString,
	event_time: Schema.Finite,
	event: SlackAgentSessionStoppedEvent,
})
export type SlackAgentSessionStoppedEnvelope = typeof SlackAgentSessionStoppedEnvelope.Type

export const SlackMessageEnvelope = Schema.Struct({
	type: Schema.Literal('event_callback'),
	team_id: SlackTeamId,
	event_id: Schema.NonEmptyString,
	event_time: Schema.Finite,
	event: SlackMessageEvent,
})
export type SlackMessageEnvelope = typeof SlackMessageEnvelope.Type

export const SlackMessageUpdatedEnvelope = Schema.Struct({
	type: Schema.Literal('event_callback'),
	team_id: SlackTeamId,
	event_id: Schema.NonEmptyString,
	event_time: Schema.Finite,
	event: SlackMessageUpdatedEvent,
})
export type SlackMessageUpdatedEnvelope = typeof SlackMessageUpdatedEnvelope.Type

export const SlackMessageDeletedEnvelope = Schema.Struct({
	type: Schema.Literal('event_callback'),
	team_id: SlackTeamId,
	event_id: Schema.NonEmptyString,
	event_time: Schema.Finite,
	event: SlackMessageDeletedEvent,
})
export type SlackMessageDeletedEnvelope = typeof SlackMessageDeletedEnvelope.Type

export const SlackReactionAddedEnvelope = Schema.Struct({
	type: Schema.Literal('event_callback'),
	team_id: SlackTeamId,
	event_id: Schema.NonEmptyString,
	event_time: Schema.Finite,
	event: SlackReactionAddedEvent,
})
export type SlackReactionAddedEnvelope = typeof SlackReactionAddedEnvelope.Type

export const SlackReactionRemovedEnvelope = Schema.Struct({
	type: Schema.Literal('event_callback'),
	team_id: SlackTeamId,
	event_id: Schema.NonEmptyString,
	event_time: Schema.Finite,
	event: SlackReactionRemovedEvent,
})
export type SlackReactionRemovedEnvelope = typeof SlackReactionRemovedEnvelope.Type
