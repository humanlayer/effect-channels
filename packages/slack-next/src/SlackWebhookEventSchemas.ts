/**
 * Define the possible event shapes for slack webhooks
 */
import { Schema } from 'effect'

import { SlackChannelId, SlackMessageTs, SlackTeamId } from './SlackIdentity'

export const SlackFileMetadata = Schema.Struct({
	id: Schema.NonEmptyString,
	name: Schema.optionalKey(Schema.String),
	mimetype: Schema.optionalKey(Schema.String),
	size: Schema.optionalKey(Schema.Natural),
	original_w: Schema.optionalKey(Schema.Natural),
	original_h: Schema.optionalKey(Schema.Natural),
	url_private: Schema.optionalKey(Schema.String),
	url_private_download: Schema.optionalKey(Schema.String),
})
export type SlackFileMetadata = typeof SlackFileMetadata.Type

export const SlackMessageSnapshot = Schema.Struct({
	subtype: Schema.optionalKey(Schema.String),
	user: Schema.optionalKey(Schema.NonEmptyString),
	bot_id: Schema.optionalKey(Schema.NonEmptyString),
	text: Schema.optionalKey(Schema.String),
	ts: SlackMessageTs,
	thread_ts: Schema.optionalKey(SlackMessageTs),
	files: Schema.optionalKey(Schema.Array(SlackFileMetadata)),
})
export type SlackMessageSnapshot = typeof SlackMessageSnapshot.Type

export const SlackAppMentionEvent = Schema.Struct({
	type: Schema.Literal('app_mention'),
	user: Schema.optionalKey(Schema.NonEmptyString),
	bot_id: Schema.optionalKey(Schema.NonEmptyString),
	text: Schema.String,
	ts: SlackMessageTs,
	thread_ts: Schema.optionalKey(SlackMessageTs),
	channel: SlackChannelId,
	team: Schema.optionalKey(SlackTeamId),
	files: Schema.optionalKey(Schema.Array(SlackFileMetadata)),
})
export type SlackAppMentionEvent = typeof SlackAppMentionEvent.Type

export const SlackMessageEvent = Schema.Struct({
	type: Schema.Literal('message'),
	subtype: Schema.optionalKey(Schema.String),
	user: Schema.optionalKey(Schema.NonEmptyString),
	bot_id: Schema.optionalKey(Schema.NonEmptyString),
	text: Schema.optionalKey(Schema.String),
	ts: SlackMessageTs,
	thread_ts: Schema.optionalKey(SlackMessageTs),
	channel: SlackChannelId,
	channel_type: Schema.optionalKey(Schema.Literals(['channel', 'group', 'im', 'mpim'])),
	message: Schema.optionalKey(SlackMessageSnapshot),
	previous_message: Schema.optionalKey(SlackMessageSnapshot),
	deleted_ts: Schema.optionalKey(SlackMessageTs),
	files: Schema.optionalKey(Schema.Array(SlackFileMetadata)),
})
export type SlackMessageEvent = typeof SlackMessageEvent.Type

export const SlackMessageUpdatedEvent = Schema.Struct({
	...SlackMessageEvent.fields,
	subtype: Schema.Literal('message_changed'),
	message: SlackMessageSnapshot,
})
export type SlackMessageUpdatedEvent = typeof SlackMessageUpdatedEvent.Type

export const SlackMessageDeletedEvent = Schema.Struct({
	...SlackMessageEvent.fields,
	subtype: Schema.Literal('message_deleted'),
})
export type SlackMessageDeletedEvent = typeof SlackMessageDeletedEvent.Type

export const SlackReactionItem = Schema.Struct({
	type: Schema.Literal('message'),
	channel: SlackChannelId,
	ts: SlackMessageTs,
})
export type SlackReactionItem = typeof SlackReactionItem.Type

export const SlackReactionAddedEvent = Schema.Struct({
	type: Schema.Literal('reaction_added'),
	user: Schema.NonEmptyString,
	reaction: Schema.NonEmptyString,
	item: SlackReactionItem,
	event_ts: SlackMessageTs,
})
export type SlackReactionAddedEvent = typeof SlackReactionAddedEvent.Type

export const SlackReactionRemovedEvent = Schema.Struct({
	type: Schema.Literal('reaction_removed'),
	user: Schema.NonEmptyString,
	reaction: Schema.NonEmptyString,
	item: SlackReactionItem,
	event_ts: SlackMessageTs,
})
export type SlackReactionRemovedEvent = typeof SlackReactionRemovedEvent.Type

export const SlackAgentSessionStoppedEvent = Schema.Struct({
	type: Schema.Literal('agent_session_stopped'),
	channel: SlackChannelId,
	thread_ts: SlackMessageTs,
	user: Schema.NonEmptyString,
	event_ts: SlackMessageTs,
	streaming_message_ts: Schema.Array(SlackMessageTs),
})
export type SlackAgentSessionStoppedEvent = typeof SlackAgentSessionStoppedEvent.Type

export const SlackInnerEvent = Schema.Union([
	SlackAppMentionEvent,
	SlackMessageUpdatedEvent,
	SlackMessageDeletedEvent,
	SlackMessageEvent,
	SlackReactionAddedEvent,
	SlackReactionRemovedEvent,
	SlackAgentSessionStoppedEvent,
])
export type SlackInnerEvent = typeof SlackInnerEvent.Type
