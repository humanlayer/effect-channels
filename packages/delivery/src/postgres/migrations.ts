import { Effect, Schema } from 'effect'
import * as Migrator from 'effect/unstable/sql/Migrator'
import * as SqlClient from 'effect/unstable/sql/SqlClient'

export class PostgresInitializationError extends Schema.TaggedError<PostgresInitializationError>()(
	'PostgresInitializationError',
	{},
) {}

const initial = Effect.gen(function* () {
	const sql = (yield* SqlClient.SqlClient).withoutTransforms()
	yield* sql`CREATE TABLE humanlayer_delivery_v1_mailboxes (
		key text COLLATE "C" PRIMARY KEY,
		revision bigint NOT NULL CHECK (revision >= 0 AND revision <= 9007199254740991),
		state_json text NOT NULL,
		ready_at double precision
	)`
	yield* sql`CREATE INDEX humanlayer_delivery_v1_ready_key
		ON humanlayer_delivery_v1_mailboxes (key text_pattern_ops, ready_at)
		WHERE ready_at IS NOT NULL`
	yield* sql`CREATE INDEX humanlayer_delivery_v1_ready_time
		ON humanlayer_delivery_v1_mailboxes (ready_at, key)
		WHERE ready_at IS NOT NULL`
}).pipe(
	Effect.tapErrorTag('SqlError', (error) =>
		Effect.logError('Delivery Postgres migration statement failed', {
			reason: error.reason._tag,
			retryable: error.isRetryable,
		}),
	),
	Effect.asVoid,
)

const deliveryLocators = Effect.gen(function* () {
	const sql = (yield* SqlClient.SqlClient).withoutTransforms()
	yield* sql`CREATE TABLE humanlayer_delivery_v1_locators (
		delivery_id text COLLATE "C" PRIMARY KEY,
		mailbox_key text COLLATE "C" NOT NULL REFERENCES humanlayer_delivery_v1_mailboxes(key)
	)`
}).pipe(Effect.asVoid)

export const migrate = Effect.gen(function* () {
	const sql = (yield* SqlClient.SqlClient).withoutTransforms()
	return yield* sql.withTransaction(
		Effect.gen(function* () {
			yield* sql`SELECT pg_advisory_xact_lock(1751936118, 1684368497)`
			yield* sql`CREATE TABLE IF NOT EXISTS humanlayer_delivery_v1_migrations (
			migration_id integer PRIMARY KEY,
			created_at timestamp with time zone NOT NULL DEFAULT now(),
			name text NOT NULL
		)`
			return yield* Migrator.make({})({
				table: 'humanlayer_delivery_v1_migrations',
				loader: Migrator.fromRecord({ '1_mailboxes': initial, '2_delivery_locators': deliveryLocators }),
			}).pipe(Effect.provideService(SqlClient.SqlClient, sql))
		}),
	)
}).pipe(
	Effect.tapErrorTag('SqlError', (error) =>
		Effect.logError('Delivery Postgres migration command failed', {
			reason: error.reason._tag,
			retryable: error.isRetryable,
		}),
	),
	Effect.tapErrorTag('MigrationError', (error) =>
		Effect.logError('Delivery Postgres migration failed', {
			reason: error.kind,
		}),
	),
	Effect.catchTags({
		SqlError: () => Effect.fail(new PostgresInitializationError()),
		MigrationError: () => Effect.fail(new PostgresInitializationError()),
	}),
	Effect.catchDefect(() =>
		Effect.logError('Delivery Postgres migration defect', { reason: 'MigrationDefect' }).pipe(
			Effect.andThen(Effect.fail(new PostgresInitializationError())),
		),
	),
	Effect.withSpan('delivery.postgres.migrate'),
)
