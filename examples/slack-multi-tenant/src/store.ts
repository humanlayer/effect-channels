import { OrgId } from '@humanlayer/channels'
import {
	SlackConnection,
	SlackConnectionCredentials,
	type SlackConnectionLookupInput,
} from '@humanlayer/channels-slack'
import { Effect, Layer, Redacted, Schema } from 'effect'
import { SqlClient } from 'effect/unstable/sql'

const SlackConnectionRow = Schema.Struct({
	workspace_id: Schema.NonEmptyString,
	organization_id: Schema.NonEmptyString,
	enabled: Schema.Boolean,
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
		CREATE TABLE IF NOT EXISTS example_slack_connections (
			workspace_id text PRIMARY KEY,
			organization_id text NOT NULL,
			enabled boolean NOT NULL DEFAULT true,
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
		Effect.mapError(() => SlackConnectionRepositoryError.make({ operation: 'migrate' })),
	),
)

/** Loads one Slack connection from the example-owned Postgres repository. */
export const loadSlackConnection = (
	input: SlackConnectionLookupInput,
): Effect.Effect<unknown, SlackConnectionRepositoryError, SqlClient.SqlClient> =>
	Effect.gen(function* () {
		const sql = (yield* SqlClient.SqlClient).withoutTransforms()
		const rows = yield* sql<typeof SlackConnectionRow.Encoded>`
			SELECT workspace_id, organization_id, enabled, bot_token, bot_user_id, bot_id
			FROM example_slack_connections
			WHERE workspace_id = ${input.workspaceId}
		`
		const row = rows.at(0)
		if (row === undefined) {
			return undefined
		}
		const decoded = yield* Schema.decodeEffect(SlackConnectionRow)(row)
		return SlackConnection.make({
			organizationId: OrgId.make(decoded.organization_id),
			enabled: decoded.enabled,
			credentials: SlackConnectionCredentials.make({
				botToken: Redacted.make(decoded.bot_token),
				botUserId: decoded.bot_user_id,
				botId: decoded.bot_id,
			}),
		})
	}).pipe(Effect.mapError(() => SlackConnectionRepositoryError.make({ operation: 'load' })))

export const SlackConnectionSeed = Schema.Struct({
	workspaceId: Schema.NonEmptyString,
	organizationId: Schema.NonEmptyString,
	enabled: Schema.Boolean,
	botToken: Schema.Redacted(Schema.String, { disallowJsonEncode: true }),
	botUserId: Schema.NonEmptyString,
	botId: Schema.NonEmptyString,
})
export type SlackConnectionSeed = typeof SlackConnectionSeed.Type

/** Explicitly upserts one bootstrap record without logging its token. */
export const seedSlackConnection = (seed: SlackConnectionSeed) =>
	Effect.gen(function* () {
		const sql = (yield* SqlClient.SqlClient).withoutTransforms()
		yield* sql`
			INSERT INTO example_slack_connections (
				workspace_id, organization_id, enabled, bot_token, bot_user_id, bot_id
			) VALUES (
				${seed.workspaceId},
				${seed.organizationId},
				${seed.enabled},
				${Redacted.value(seed.botToken)},
				${seed.botUserId},
				${seed.botId}
			)
			ON CONFLICT (workspace_id) DO UPDATE SET
				organization_id = EXCLUDED.organization_id,
				enabled = EXCLUDED.enabled,
				bot_token = EXCLUDED.bot_token,
				bot_user_id = EXCLUDED.bot_user_id,
				bot_id = EXCLUDED.bot_id,
				updated_at = now()
		`
	}).pipe(
		Effect.asVoid,
		Effect.mapError(() => SlackConnectionRepositoryError.make({ operation: 'seed' })),
	)
