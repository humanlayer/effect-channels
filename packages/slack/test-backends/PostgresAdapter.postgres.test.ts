import { assert, it } from '@effect/vitest'
import { Config, Context, Effect, Layer, Redacted, Schema } from 'effect'
import * as SqlClient from 'effect/unstable/sql/SqlClient'

import { layer } from '../src/postgres.ts'
import * as Client from '../src/postgres/client.ts'
import { migrate } from '../src/postgres/migrations.ts'
import { SlackConnectionStore } from '../src/SlackConnectionStore.ts'
import { SubscriptionCreated } from '../src/SlackEvents.ts'
import { SlackSubscriptions } from '../src/SlackSubscriptions.ts'
import {
	installation,
	installationJson,
	proactiveThread,
	rootedThread,
	routeInput,
	workspaceId,
} from '../test/AdapterFixtures.ts'
import { botContract } from './BotContract.ts'
import { OtherConnections, OtherSubscriptions, replicaContract } from './StoreContract.ts'

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

const replicas = Layer.effectContext(
	Effect.gen(function* () {
		const [first, second] = yield* Effect.all([Layer.build(Layer.fresh(layer)), Layer.build(Layer.fresh(layer))], {
			concurrency: 2,
		})
		return first.pipe(
			Context.add(OtherConnections, Context.get(second, SlackConnectionStore)),
			Context.add(OtherSubscriptions, Context.get(second, SlackSubscriptions)),
		)
	}),
)

it.effect(
	'disposable Postgres: concurrent migration, cross-runtime routing, credential rotation, TTL and bounded cleanup',
	() =>
		Effect.gen(function* () {
			const sql = (yield* SqlClient.SqlClient).withoutTransforms()
			const existing = yield* sql`SELECT tablename FROM pg_tables WHERE schemaname = current_schema()`
			assert.deepStrictEqual(existing, [], 'Use a fresh disposable database; no existing tables are reset')
			const migrations = yield* Effect.all([migrate, migrate], { concurrency: 2 })
			assert.deepStrictEqual(
				migrations.map((result) => result.length).sort((a, b) => a - b),
				[0, 1],
			)
			assert.deepStrictEqual(yield* migrate, [])
			assert.deepStrictEqual(yield* sql`SELECT migration_id, name FROM humanlayer_slack_v1_migrations`, [
				{ migration_id: 1, name: 'slack_state' },
			])
			yield* replicaContract.pipe(Effect.provide(replicas))
			assert.deepStrictEqual(
				yield* sql`SELECT connection_json FROM humanlayer_slack_v1_connections WHERE workspace_id = ${workspaceId}`,
				[{ connection_json: installationJson }],
			)
			yield* Effect.gen(function* () {
				const connections = yield* SlackConnectionStore
				const subscriptions = yield* SlackSubscriptions
				assert.deepStrictEqual(yield* connections.get({ workspaceId }), installation)
				assert.strictEqual(
					(yield* subscriptions.resolveDirectMessageRoute(routeInput)).thread.id,
					rootedThread.id,
				)
				yield* sql`UPDATE humanlayer_slack_v1_subscriptions SET expires_at = statement_timestamp() - interval '1 second'`
				assert.strictEqual(yield* subscriptions.isSubscribed({ threadId: proactiveThread.id }), false)
				assert.deepStrictEqual(
					yield* subscriptions.subscribe({ threadId: proactiveThread.id }),
					SubscriptionCreated.make({}),
				)
				yield* sql`UPDATE humanlayer_slack_v1_routes SET expires_at = statement_timestamp() - interval '1 second'`
				assert.deepStrictEqual(yield* subscriptions.resolveDirectMessageRoute(routeInput), {
					thread: proactiveThread,
					subscribed: true,
				})
				assert.deepStrictEqual(
					yield* sql`SELECT
				(EXTRACT(EPOCH FROM (expires_at - statement_timestamp())) > 86390) AS fresh
				FROM humanlayer_slack_v1_routes WHERE event_id = ${routeInput.eventId}`,
					[{ fresh: true }],
				)
				yield* sql`INSERT INTO humanlayer_slack_v1_subscriptions (thread_id, expires_at, created)
				SELECT 'expired-' || n, statement_timestamp() - interval '1 second', true FROM generate_series(1, 300) n`
				yield* subscriptions.isSubscribed({ threadId: rootedThread.id })
				assert.deepStrictEqual(
					yield* sql`SELECT count(*)::integer AS count FROM humanlayer_slack_v1_subscriptions
				WHERE expires_at <= statement_timestamp()`,
					[{ count: 172 }],
				)
				assert.deepStrictEqual(yield* connections.get({ workspaceId }), installation)
			}).pipe(Effect.provide(Layer.fresh(layer)))
			yield* botContract.pipe(Effect.provide(Layer.fresh(layer)))
		}).pipe(Effect.provide(client)),
	{ timeout: 30_000 },
)
