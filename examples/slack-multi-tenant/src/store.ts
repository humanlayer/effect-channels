import {
	SlackConnection,
	SlackConnectionCredentials,
	type SlackConnectionLookupInput,
} from '@humanlayer/channels-slack'
import { Effect, Layer, Redacted, Schema } from 'effect'
import { SqlClient } from 'effect/unstable/sql'

const SlackConnectionRow = Schema.Struct({
	workspace_id: Schema.NonEmptyString,
	bot_token: Schema.NonEmptyString,
	bot_user_id: Schema.NonEmptyString,
	bot_id: Schema.NonEmptyString,
})

export class SlackConnectionRepositoryError extends Schema.TaggedError<SlackConnectionRepositoryError>()(
	'SlackConnectionRepositoryError',
	{ operation: Schema.Literals(['load', 'seed', 'migrate']) },
) {}

export const SlackConnectionRepositoryMigration = Effect.gen(function* () {
	const sql = (yield* SqlClient.SqlClient).withoutTransforms()
	yield* sql`
		CREATE TABLE IF NOT EXISTS example_slack_installations_v1 (
			workspace_id text PRIMARY KEY,
			bot_token text NOT NULL,
			bot_user_id text NOT NULL,
			bot_id text NOT NULL,
			created_at timestamptz NOT NULL DEFAULT now(),
			updated_at timestamptz NOT NULL DEFAULT now()
		)
	`
}).pipe(Effect.asVoid)

/** Runs the example-owned Slack connection table migration during layer acquisition. */
export const SlackConnectionRepositoryLive = Layer.effectDiscard(
	SlackConnectionRepositoryMigration.pipe(
		Effect.tapError(() => Effect.logError('Slack credential repository migration failed')),
		Effect.mapError(() => SlackConnectionRepositoryError.make({ operation: 'migrate' })),
		Effect.withSpan('example.slack.credentials.migrate'),
	),
)

/** Loads one Slack connection from the example-owned Postgres repository. */
export const loadSlackConnection = (
	input: SlackConnectionLookupInput,
): Effect.Effect<SlackConnection | undefined, SlackConnectionRepositoryError, SqlClient.SqlClient> =>
	Effect.gen(function* () {
		const sql = (yield* SqlClient.SqlClient).withoutTransforms()
		const rows = yield* sql<typeof SlackConnectionRow.Encoded>`
			SELECT workspace_id, bot_token, bot_user_id, bot_id
			FROM example_slack_installations_v1
			WHERE workspace_id = ${input.workspaceId}
		`
		const row = rows.at(0)
		if (row === undefined) {
			return undefined
		}
		const decoded = yield* Schema.decodeEffect(SlackConnectionRow)(row)
		return SlackConnection.make({
			credentials: SlackConnectionCredentials.make({
				botToken: Redacted.make(decoded.bot_token),
				botUserId: decoded.bot_user_id,
				botId: decoded.bot_id,
			}),
		})
	}).pipe(
		Effect.tapError(() => Effect.logError('Slack credential repository lookup failed')),
		Effect.mapError(() => SlackConnectionRepositoryError.make({ operation: 'load' })),
		Effect.withSpan('example.slack.credentials.load'),
	)

export const SlackConnectionSeed = Schema.Struct({
	workspaceId: Schema.NonEmptyString,
	botToken: Schema.Redacted(Schema.NonEmptyString, { disallowJsonEncode: true }),
	botUserId: Schema.NonEmptyString,
	botId: Schema.NonEmptyString,
})
export type SlackConnectionSeed = typeof SlackConnectionSeed.Type

/** Explicitly upserts one bootstrap record without logging its token. */
export const seedSlackConnection = Effect.fn('example.slack.credentials.seed')(
	function* (seed: SlackConnectionSeed) {
		const sql = (yield* SqlClient.SqlClient).withoutTransforms()
		yield* sql`
			INSERT INTO example_slack_installations_v1 (
				workspace_id, bot_token, bot_user_id, bot_id
			) VALUES (
				${seed.workspaceId},
				${Redacted.value(seed.botToken)},
				${seed.botUserId},
				${seed.botId}
			)
			ON CONFLICT (workspace_id) DO UPDATE SET
				bot_token = EXCLUDED.bot_token,
				bot_user_id = EXCLUDED.bot_user_id,
				bot_id = EXCLUDED.bot_id,
				updated_at = now()
		`
	},
	Effect.tapError(() => Effect.logError('Slack credential repository seed failed')),
	Effect.mapError(() => SlackConnectionRepositoryError.make({ operation: 'seed' })),
)
