import { Effect, Layer, Schema } from 'effect'
import * as SqlClient from 'effect/unstable/sql/SqlClient'

import { SlackConnectionLookupInput } from '../Schema'
import { SlackConnectionStore, UpsertSlackConnection } from '../SlackConnectionStore'
import { connectionJson } from '../storage/ConnectionCodec'
import { connectionErrors } from './errors'
import { initialized } from './migrations'

const rowsCodec = Schema.Array(Schema.Struct({ connection_json: connectionJson })).check(Schema.isMaxLength(1))

const get = Effect.fn('slack.postgres.connections.get')(
	function* (input: SlackConnectionLookupInput) {
		yield* Schema.decodeEffect(SlackConnectionLookupInput)(input)
		const sql = (yield* SqlClient.SqlClient).withoutTransforms()
		const rows = yield* sql`SELECT connection_json FROM humanlayer_slack_v1_connections
			WHERE workspace_id = ${input.workspaceId}`
		const [row] = yield* Schema.decodeUnknownEffect(rowsCodec)(rows)
		return row?.connection_json
	},
	connectionErrors({ operation: 'get' }),
)

const upsert = Effect.fn('slack.postgres.connections.upsert')(
	function* (input: UpsertSlackConnection) {
		yield* Schema.decodeEffect(UpsertSlackConnection)(input)
		const sql = (yield* SqlClient.SqlClient).withoutTransforms()
		const json = yield* Schema.encodeEffect(connectionJson)(input.connection)
		yield* sql`INSERT INTO humanlayer_slack_v1_connections (workspace_id, connection_json)
			VALUES (${input.workspaceId}, ${json})
			ON CONFLICT (workspace_id) DO UPDATE SET connection_json = EXCLUDED.connection_json`
	},
	connectionErrors({ operation: 'upsert' }),
)

const remove = Effect.fn('slack.postgres.connections.remove')(
	function* (input: SlackConnectionLookupInput) {
		yield* Schema.decodeEffect(SlackConnectionLookupInput)(input)
		const sql = (yield* SqlClient.SqlClient).withoutTransforms()
		yield* sql`DELETE FROM humanlayer_slack_v1_connections WHERE workspace_id = ${input.workspaceId}`
	},
	connectionErrors({ operation: 'remove' }),
)

export const connections = Layer.effect(
	SlackConnectionStore,
	Effect.gen(function* () {
		const sql = yield* SqlClient.SqlClient
		return SlackConnectionStore.of({
			get: (input) => get(input).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
			upsert: (input) => upsert(input).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
			remove: (input) => remove(input).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
		})
	}),
).pipe(Layer.provide(initialized))
