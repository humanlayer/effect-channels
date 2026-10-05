import { Schema } from 'effect'

import { SlackMessage, SlackMessageRef, SlackParticipant, SlackReaction } from './SlackModels'
import { SlackThread } from './SlackThread'

export const SlackEventId = Schema.NonEmptyString.pipe(Schema.brand('SlackEventId'))
export type SlackEventId = typeof SlackEventId.Type

export class SlackMessageReceived extends Schema.TaggedClass<SlackMessageReceived>()('SlackMessageReceived', {
	eventId: SlackEventId,
	message: SlackMessage,
}) {}

export class SlackMessageUpdated extends Schema.TaggedClass<SlackMessageUpdated>()('SlackMessageUpdated', {
	eventId: SlackEventId,
	message: SlackMessage,
	previousMessage: Schema.optionalKey(SlackMessage),
}) {}

export class SlackMessageDeleted extends Schema.TaggedClass<SlackMessageDeleted>()('SlackMessageDeleted', {
	eventId: SlackEventId,
	messageRef: SlackMessageRef,
	previousMessage: Schema.optionalKey(SlackMessage),
	actor: Schema.optionalKey(SlackParticipant),
}) {}

export class SlackReactionAdded extends Schema.TaggedClass<SlackReactionAdded>()('SlackReactionAdded', {
	eventId: SlackEventId,
	message: SlackMessage,
	actor: SlackParticipant,
	reaction: SlackReaction,
}) {}

export class SlackReactionRemoved extends Schema.TaggedClass<SlackReactionRemoved>()('SlackReactionRemoved', {
	eventId: SlackEventId,
	message: SlackMessage,
	actor: SlackParticipant,
	reaction: SlackReaction,
}) {}

export class SlackConversationStopped extends Schema.TaggedClass<SlackConversationStopped>()(
	'SlackConversationStopped',
	{
		eventId: SlackEventId,
		actor: SlackParticipant,
		streamingMessages: Schema.Array(SlackMessageRef),
	},
) {}

export const SlackThreadEvent = Schema.Union([
	SlackMessageReceived,
	SlackMessageUpdated,
	SlackMessageDeleted,
	SlackReactionAdded,
	SlackReactionRemoved,
	SlackConversationStopped,
])
export type SlackThreadEvent = typeof SlackThreadEvent.Type

export const SlackThreadEvents = Schema.Array(SlackThreadEvent)
export type SlackThreadEvents = typeof SlackThreadEvents.Type

export const SlackNonEmptyThreadEvents = Schema.NonEmptyArray(SlackThreadEvent)
export type SlackNonEmptyThreadEvents = typeof SlackNonEmptyThreadEvents.Type

export class SlackNewMention extends Schema.TaggedClass<SlackNewMention>()('SlackNewMention', {
	thread: SlackThread,
	trigger: SlackMessage,
	events: SlackThreadEvents,
}) {}

export class SlackSubscribedThreadEvents extends Schema.TaggedClass<SlackSubscribedThreadEvents>()(
	'SlackSubscribedThreadEvents',
	{
		thread: SlackThread,
		events: SlackNonEmptyThreadEvents,
	},
) {}
