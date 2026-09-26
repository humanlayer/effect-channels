import { assert, it } from '@effect/vitest'
import { Config, Effect, Layer, Redacted, Schema } from 'effect'
import { TestClock } from 'effect/testing'
import * as SqlClient from 'effect/unstable/sql/SqlClient'

import { DeliveryQueue } from '../src/DeliveryQueue'
import { emptyMailbox } from '../src/Mailbox'
import { MailboxStore } from '../src/MailboxStore'
import { layer, migrate } from '../src/postgres'
import * as Client from '../src/postgres/client'
import { encodeState } from '../test/AdapterCommands'
import {
	interruptForReconstruction,
	policy,
	resumeAfterReconstruction,
	staleAttemptContract,
	storageContract,
} from './StoreContract'

const client = Layer.unwrap(
	Effect.gen(function* () {
		yield* Config.schema(Schema.Literal('disposable'), 'DELIVERY_BACKEND_TEST_CONFIRM')
		return Client.layer({
			host: '127.0.0.1',
			port: 55432,
			database: 'delivery_adapter_test',
			username: 'delivery_test',
			password: Redacted.make('delivery_test'),
			connectTimeout: '3 seconds',
			maxConnections: 4,
		})
	}),
)

const runtime = DeliveryQueue.layerMailboxStore.pipe(Layer.provideMerge(layer))

it.effect(
	'isolated PostgreSQL: concurrent startup, atomic storage/readiness, stale attempts and fresh-layer recovery',
	() =>
		Effect.gen(function* () {
			const sql = yield* SqlClient.SqlClient
			const existing = yield* sql`SELECT to_regclass('humanlayer_delivery_v1_migrations') AS name`
			assert.deepStrictEqual(
				existing,
				[{ name: null }],
				'Use a fresh disposable database; this suite will not reset an existing store',
			)
			const migrations = yield* Effect.all([migrate, migrate], { concurrency: 2 })
			assert.deepStrictEqual(
				migrations.map((result) => result.length).sort((left, right) => left - right),
				[0, 2],
			)
			assert.deepStrictEqual(yield* migrate, [])
			assert.deepStrictEqual(yield* sql`SELECT migration_id, name FROM humanlayer_delivery_v1_migrations`, [
				{ migration_id: 1, name: 'mailboxes' },
				{ migration_id: 2, name: 'delivery_locators' },
			])
			yield* storageContract.pipe(Effect.provide(layer))
			const rows =
				yield* sql`SELECT revision::double precision AS revision, state_json, ready_at FROM humanlayer_delivery_v1_mailboxes WHERE key = ${'contract%_!\\:cas'}`
			assert.deepStrictEqual(rows, [
				{ revision: 3, state_json: yield* encodeState(emptyMailbox()), ready_at: null },
			])
			yield* staleAttemptContract.pipe(Effect.provide(Layer.fresh(runtime)))
			const receipt = yield* interruptForReconstruction.pipe(Effect.provide(Layer.fresh(runtime)))
			yield* TestClock.adjust(policy.leaseMs)
			yield* resumeAfterReconstruction(receipt).pipe(Effect.provide(Layer.fresh(runtime)))
			yield* Effect.gen(function* () {
				const store = yield* MailboxStore
				assert.deepStrictEqual(yield* store.loadMailbox({ key: 'contract%_!\\:cas' }), {
					revision: 3,
					state: emptyMailbox(),
				})
			}).pipe(Effect.provide(Layer.fresh(runtime)))
		}).pipe(Effect.provide(client)),
	{ timeout: 30_000 },
)
