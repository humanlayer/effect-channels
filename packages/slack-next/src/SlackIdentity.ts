import { Schema } from 'effect'

export const SlackTeamId = Schema.NonEmptyString.pipe(Schema.brand('SlackTeamId'))
export type SlackTeamId = typeof SlackTeamId.Type

export const SlackChannelId = Schema.NonEmptyString.pipe(Schema.brand('SlackChannelId'))
export type SlackChannelId = typeof SlackChannelId.Type

export const SlackMessageTs = Schema.NonEmptyString.pipe(Schema.brand('SlackMessageTs'))
export type SlackMessageTs = typeof SlackMessageTs.Type

/**
 * Given a slack event get the resource ID from it - used for mailboxing
 * e.g. thread TS, team ID, channel ID
 * @param input
 * @returns
 */
export const slackThreadResourceId = (input: {
	readonly teamId: SlackTeamId
	readonly channelId: SlackChannelId
	readonly threadTs: SlackMessageTs
}) =>
	[
		'slack',
		'v1',
		encodeURIComponent(input.teamId),
		encodeURIComponent(input.channelId),
		encodeURIComponent(input.threadTs),
	].join(':')
