import { Schema } from 'effect'

import { SlackTeamId } from './SlackIdentity'

export const SlackConnectionLookupInput = Schema.Struct({ workspaceId: SlackTeamId })
export type SlackConnectionLookupInput = typeof SlackConnectionLookupInput.Type

export const SlackConnectionCredentials = Schema.Struct({
	botToken: Schema.Redacted(Schema.NonEmptyString, { disallowJsonEncode: true }),
	botUserId: Schema.NonEmptyString,
	botId: Schema.NonEmptyString,
})
export type SlackConnectionCredentials = typeof SlackConnectionCredentials.Type

export const SlackConnection = Schema.Struct({ credentials: SlackConnectionCredentials })
export type SlackConnection = typeof SlackConnection.Type
