import { Schema } from 'effect'

export const SlackTeamId = Schema.NonEmptyString.pipe(Schema.brand('SlackTeamId'))
export type SlackTeamId = typeof SlackTeamId.Type
export const SlackChannelId = Schema.NonEmptyString.pipe(Schema.brand('SlackChannelId'))
export type SlackChannelId = typeof SlackChannelId.Type
export const SlackMessageTs = Schema.NonEmptyString.pipe(Schema.brand('SlackMessageTs'))
export type SlackMessageTs = typeof SlackMessageTs.Type

export const SlackThreadRef = Schema.Struct({
	teamId: SlackTeamId,
	channelId: SlackChannelId,
	threadTs: SlackMessageTs,
	directMessageKind: Schema.optionalKey(Schema.Literals(['im', 'mpim'])),
})
export type SlackThreadRef = typeof SlackThreadRef.Type

export const SlackChannelAddress = Schema.Struct({
	teamId: SlackTeamId,
	channelId: SlackChannelId,
})
export type SlackChannelAddress = typeof SlackChannelAddress.Type
