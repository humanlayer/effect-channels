import { PgClient } from '@effect/sql-pg'
import { Config, Effect, Layer, Redacted, Schema } from 'effect'
import { SqlClient } from 'effect/unstable/sql'

const SchemaName = Schema.Struct({ name: Schema.NonEmptyString })
const admin = PgClient.layerConfig({ url: Config.redacted('TEST_DATABASE_URL') })

export const database = Layer.unwrap(
	Effect.gen(function* () {
		const url = yield* Config.redacted('TEST_DATABASE_URL')
		const sql = yield* SqlClient.SqlClient
		const rows = yield* sql`SELECT 'slack_example_test_' || replace(gen_random_uuid()::text, '-', '') AS name`
		const { name } = yield* Schema.decodeUnknownEffect(SchemaName)(rows.at(0))
		yield* Effect.acquireRelease(sql`CREATE SCHEMA ${sql(name)}`, () =>
			sql`DROP SCHEMA ${sql(name)} CASCADE`.pipe(
				Effect.catchCause(() => Effect.logError('Failed to remove isolated Slack example test schema')),
			),
		)
		const scopedUrl = new URL(Redacted.value(url))
		scopedUrl.searchParams.set('options', `-c search_path=${name}`)
		return PgClient.layer({ url: Redacted.make(scopedUrl.toString()) })
	}),
).pipe(Layer.provide(admin))
