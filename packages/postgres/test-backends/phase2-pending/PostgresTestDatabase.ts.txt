import { PgClient } from '@effect/sql-pg'
import { Config, Context, Effect, Layer, Schema } from 'effect'
import { SqlClient } from 'effect/unstable/sql'

const SchemaNameRow = Schema.Struct({ schema_name: Schema.NonEmptyString })

export const postgresEmulatorTestsEnabled = import.meta.env.DATABASE_URL !== undefined

const schemaUrl = (url: string, schema: string) => {
	const parsed = new URL(url)
	parsed.searchParams.set('options', `-c search_path=${schema}`)
	return parsed.toString()
}

export class PostgresTestDatabase extends Context.Service<
	PostgresTestDatabase,
	{ readonly url: string; readonly schema: string }
>()('test/PostgresTestDatabase') {
	static readonly layer = Layer.effect(
		PostgresTestDatabase,
		Effect.gen(function* () {
			const url = yield* Config.string('DATABASE_URL')
			const sql = yield* SqlClient.SqlClient
			const rows = yield* sql`SELECT 'channels_app_' || replace(gen_random_uuid()::text, '-', '') AS schema_name`
			const row = yield* Schema.decodeUnknownEffect(SchemaNameRow)(rows.at(0))
			yield* sql`CREATE SCHEMA ${sql(row.schema_name)}`
			yield* Effect.addFinalizer(() =>
				sql`DROP SCHEMA IF EXISTS ${sql(row.schema_name)} CASCADE`.pipe(
					Effect.tapError((error) => Effect.logError('failed to drop app integration test schema', error)),
					Effect.ignore,
				),
			)
			return PostgresTestDatabase.of({ url: schemaUrl(url, row.schema_name), schema: row.schema_name })
		}),
	).pipe(Layer.provide(PgClient.layerConfig({ url: Config.redacted('DATABASE_URL') })))
}
