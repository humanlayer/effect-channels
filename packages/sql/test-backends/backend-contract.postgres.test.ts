/**
 * Runs the shared mailbox store contract against the SQL store on a real, disposable PostgreSQL.
 *
 * Every contract test builds this layer afresh, and building it empties the store's tables,
 * so the suite refuses to run unless DELIVERY_BACKEND_TEST_CONFIRM says the database is disposable.
 */
import * as PgClient from '@effect/sql-pg/PgClient'
import { assert, it } from '@effect/vitest'
import { Config, Effect, Layer, Option, Redacted, Schema } from 'effect'
import { TestClock } from 'effect/testing'
import * as SqlClient from 'effect/unstable/sql/SqlClient'

import {
	claimAll,
	claimFrozen,
	deliver,
	findWaiting,
	leaseMs,
	mailboxBackendContract,
} from '../../delivery-next/test/backend-contract'
import { handoffUnsupportedContract } from '../../delivery-next/test/delivery-handoff-contract'
import { MailboxDeliverySql, MailboxProcessingBackendSql, migrate } from '../src'

const defaultDatabaseUrl = 'postgres://delivery_test:delivery_test@127.0.0.1:55439/delivery_adapter_test'

const client = Layer.unwrap(
	Effect.gen(function* () {
		yield* Config.schema(Schema.Literal('disposable'), 'DELIVERY_BACKEND_TEST_CONFIRM')
		const url = yield* Config.redacted('SQL_BACKEND_TEST_DATABASE_URL').pipe(
			Config.withDefault(Redacted.make(defaultDatabaseUrl)),
		)
		return PgClient.layer({ url, connectTimeout: '3 seconds', maxConnections: 4 })
	}),
)

const emptyTables = Layer.effectDiscard(
	Effect.gen(function* () {
		yield* migrate
		const sql = yield* SqlClient.SqlClient
		yield* sql`TRUNCATE delivery_next_admissions, delivery_next_claims, delivery_next_batches, delivery_next_mailboxes
			RESTART IDENTITY CASCADE`
	}),
)

const makeEmptyStore = () =>
	Layer.mergeAll(
		MailboxDeliverySql({ runMigrations: false }),
		MailboxProcessingBackendSql({ claimLimit: 10, runMigrations: false }),
	).pipe(Layer.provide(emptyTables), Layer.provide(client))

mailboxBackendContract('sql', makeEmptyStore)
handoffUnsupportedContract('sql', makeEmptyStore)

it.effect('sql: gives a claim that was live before the batch migration a batch of its own', () =>
	Effect.gen(function* () {
		yield* deliver('a')
		const claim = yield* claimAll(yield* findWaiting)
		const sql = yield* SqlClient.SqlClient
		yield* sql`UPDATE delivery_next_claims SET batch_id = NULL`
		yield* sql`DELETE FROM delivery_next_batches`
		yield* migrate
		yield* migrate
		yield* TestClock.adjust(leaseMs)
		const recovered = Option.getOrThrow(yield* claimFrozen)
		assert.strictEqual(recovered.attempt, 2)
		assert.match(recovered.batchId, /^legacy-[0-9a-f]{32}$/)
		assert.match(recovered.accessToken, /^[0-9a-f]{64}$/)
		assert.notStrictEqual(recovered.claimId, claim.claimId)
	}).pipe(Effect.provide(Layer.merge(makeEmptyStore(), client))),
)
