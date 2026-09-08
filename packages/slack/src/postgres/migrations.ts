import { Effect, Layer } from 'effect'
import * as Migrator from 'effect/unstable/sql/Migrator'
import * as SqlClient from 'effect/unstable/sql/SqlClient'

import { SlackConnectionStoreError } from '../SlackConnectionStore.js'

const initial = Effect.gen(function* () {
	const sql = (yield* SqlClient.SqlClient).withoutTransforms()
	yield* sql`CREATE TABLE humanlayer_slack_v1_connections (
		workspace_id text COLLATE "C" PRIMARY KEY,
		connection_json text NOT NULL
	)`
	yield* sql`CREATE TABLE humanlayer_slack_v1_subscriptions (
		thread_id text COLLATE "C" PRIMARY KEY,
		expires_at timestamp with time zone NOT NULL,
		created boolean NOT NULL
	)`
	yield* sql`CREATE INDEX humanlayer_slack_v1_subscriptions_expiry
		ON humanlayer_slack_v1_subscriptions (expires_at, thread_id)`
	yield* sql`CREATE TABLE humanlayer_slack_v1_routes (
		tenant text COLLATE "C" NOT NULL,
		channel_id text COLLATE "C" NOT NULL,
		event_id text COLLATE "C" NOT NULL,
		route_json text NOT NULL,
		expires_at timestamp with time zone NOT NULL,
		PRIMARY KEY (tenant, channel_id, event_id)
	)`
	yield* sql`CREATE INDEX humanlayer_slack_v1_routes_expiry
		ON humanlayer_slack_v1_routes (expires_at, tenant, channel_id, event_id)`
}).pipe(
	Effect.tapErrorTag('SqlError', (error) =>
		Effect.logError('Slack Postgres migration statement failed', {
			reason: error.reason._tag,
			retryable: error.isRetryable,
		}),
	),
	Effect.asVoid,
)

export const migrate = Effect.gen(function* () {
	const sql = (yield* SqlClient.SqlClient).withoutTransforms()
	return yield* sql.withTransaction(
		Effect.gen(function* () {
			yield* sql`SELECT pg_advisory_xact_lock(1751936118, 1936482678)`
			yield* sql`CREATE TABLE IF NOT EXISTS humanlayer_slack_v1_migrations (
				migration_id integer PRIMARY KEY,
				created_at timestamp with time zone NOT NULL DEFAULT now(),
				name text NOT NULL
			)`
			return yield* Migrator.make({})({
				table: 'humanlayer_slack_v1_migrations',
				loader: Migrator.fromRecord({ '1_slack_state': initial }),
			}).pipe(Effect.provideService(SqlClient.SqlClient, sql))
		}),
	)
}).pipe(
	Effect.tapErrorTag('SqlError', (error) =>
		Effect.logError('Slack Postgres migration command failed', {
			reason: error.reason._tag,
			retryable: error.isRetryable,
		}),
	),
	Effect.tapErrorTag('MigrationError', (error) =>
		Effect.logError('Slack Postgres migration failed', { reason: error.kind }),
	),
	Effect.catchTags({
		SqlError: () => Effect.fail(new SlackConnectionStoreError({ operation: 'initialize' })),
		MigrationError: () => Effect.fail(new SlackConnectionStoreError({ operation: 'initialize' })),
	}),
	Effect.catchDefect(() =>
		Effect.logError('Slack Postgres migration defect', { reason: 'MigrationDefect' }).pipe(
			Effect.andThen(Effect.fail(new SlackConnectionStoreError({ operation: 'initialize' }))),
		),
	),
	Effect.withSpan('slack.postgres.initialize'),
)

export const initialized = Layer.effectDiscard(migrate)
