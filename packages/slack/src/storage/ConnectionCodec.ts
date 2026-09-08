import { Schema } from 'effect'

import { SlackConnection, SlackConnectionCredentials } from '../SlackConnection.js'

const persistedConnection = Schema.Struct({
	...SlackConnection.fields,
	credentials: Schema.Struct({
		...SlackConnectionCredentials.fields,
		botToken: Schema.RedactedFromValue(Schema.NonEmptyString),
	}),
})

export const connectionJson = Schema.fromJsonString(persistedConnection)
