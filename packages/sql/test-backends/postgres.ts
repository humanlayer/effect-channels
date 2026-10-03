/**
 * The disposable PostgreSQL the SQL store's backend suites run against, and an empty store over it.
 *
 * Building `emptyStore` empties the store's tables, so the suites refuse to run unless
 * DELIVERY_BACKEND_TEST_CONFIRM says the database is disposable.
 */
import * as PgClient from '@effect/sql-pg/PgClient'
import { Config, Effect, Layer, Redacted, Schema } from 'effect'
import * as SqlClient from 'effect/sql/SqlClient'

import { DeliveryControlBackendSql, MailboxDeliverySql, MailboxProcessingBackendSql, migrate } from '../src'

const defaultDatabaseUrl = 'postgres://delivery_test:delivery_test@127.0.0.1:55439/delivery_adapter_test'

/** A new connection pool to the disposable database each time it is built. */
export const client = Layer.unwrap(
	Effect.gen(function* () {
		yield* Config.schema(Schema.Literal('disposable'), 'DELIVERY_BACKEND_TEST_CONFIRM')
		const url = yield* Config.Redacted('SQL_BACKEND_TEST_DATABASE_URL').pipe(
			Config.withDefault(Redacted.make(defaultDatabaseUrl)),
		)
		return PgClient.layer({ url, connectTimeout: '3 seconds', maxConnections: 4 })
	}),
)

/** Bring the tables up to date and empty them. */
export const emptyTables = Effect.gen(function* () {
	yield* migrate
	const sql = yield* SqlClient.SqlClient
	yield* sql`TRUNCATE delivery_next_admissions, delivery_next_claims, delivery_next_batches, delivery_next_mailboxes
		RESTART IDENTITY CASCADE`
})

/** The store's three services over `SqlClient`, the way `ChannelsSql.make` builds them. */
export const storeOverClient = (claimLimit = 10) =>
	Layer.mergeAll(MailboxDeliverySql, MailboxProcessingBackendSql({ claimLimit }), DeliveryControlBackendSql)

/** An empty store with a pool of its own. Each build empties the tables again. */
export const emptyStore = storeOverClient().pipe(Layer.provide(Layer.effectDiscard(emptyTables)), Layer.provide(client))
