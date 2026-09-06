import { assert, it } from '@effect/vitest'
import { Effect, Inspectable, Layer, Logger, Queue, Redacted, Schema } from 'effect'
import { ConnectionError, SqlError } from 'effect/unstable/sql/SqlError'

import { SubscriptionStoreError } from '../src/DomainErrors.ts'
import { connections, layer } from '../src/postgres.ts'
import { migrate } from '../src/postgres/migrations.ts'
import { SlackConnection } from '../src/Schema.ts'
import { SlackConnectionStore, SlackConnectionStoreError } from '../src/SlackConnectionStore.ts'
import { SubscriptionCreated, SubscriptionExisting } from '../src/SlackEvents.ts'
import { SlackSubscriptions } from '../src/SlackSubscriptions.ts'
import { sqlCommands } from './AdapterCommands.ts'
import {
	encodeRoute,
	installation,
	installationJson,
	proactiveThread,
	rootedThread,
	routeInput,
	workspaceId,
} from './AdapterFixtures.ts'

it.effect('Postgres command seam: the storage bundle migrates each owner once under its advisory lock', () =>
	Effect.gen(function* () {
		const fake = yield* sqlCommands
		yield* Queue.offer(fake.migrationReplies, Effect.succeed([]))
		yield* Effect.gen(function* () {
			yield* SlackConnectionStore
			yield* SlackSubscriptions
			const commands = yield* Queue.takeAll(fake.commands)
			const lock = commands.findIndex(({ sql }) => sql.includes('pg_advisory_xact_lock'))
			const bootstrap = commands.findIndex(({ sql }) => sql.includes('CREATE TABLE IF NOT EXISTS'))
			const probe = commands.findIndex(({ sql }) => sql.includes('::regclass'))
			assert.ok(lock > 0 && lock < bootstrap && bootstrap < probe)
			assert.strictEqual(commands.filter(({ sql }) => sql.includes('pg_advisory_xact_lock')).length, 2)
			assert.strictEqual(
				commands.filter(({ sql }) => sql.includes('pg_advisory_xact_lock(1751936118, 1936482678)')).length,
				1,
			)
			assert.strictEqual(
				commands.filter(({ sql }) => sql.includes('pg_advisory_xact_lock(1751936118, 1684368497)')).length,
				1,
			)
			assert.ok(commands.some(({ sql }) => sql.startsWith('CREATE TABLE humanlayer_slack_v1_connections')))
			assert.ok(commands.some(({ sql }) => sql.startsWith('CREATE TABLE humanlayer_slack_v1_routes')))
			assert.ok(commands.some(({ sql }) => sql.includes('humanlayer_slack_v1_subscriptions_expiry')))
			assert.strictEqual(commands.at(-1)?.sql, 'COMMIT')
		}).pipe(Effect.provide(layer.pipe(Layer.provide(fake.layer))))
	}),
)

it.effect(
	'Postgres command seam: authoritative get, atomic upsert and idempotent delete with private credential codec',
	() =>
		Effect.gen(function* () {
			const fake = yield* sqlCommands
			yield* Effect.gen(function* () {
				const store = yield* SlackConnectionStore
				yield* Queue.takeAll(fake.commands)
				yield* Queue.offer(fake.replies, Effect.succeed([]))
				assert.strictEqual(yield* store.get({ workspaceId }), undefined)
				yield* Queue.take(fake.commands)
				yield* Queue.offer(fake.replies, Effect.succeed([]))
				assert.strictEqual(yield* store.upsert({ workspaceId, connection: installation }), undefined)
				const insert = yield* Queue.take(fake.commands)
				assert.deepStrictEqual(insert.params, [workspaceId, installationJson])
				assert.ok(insert.sql.includes('ON CONFLICT (workspace_id) DO UPDATE'))
				assert.ok(!insert.sql.includes('private-token-sentinel'))
				for (let index = 0; index < 2; index++) {
					yield* Queue.offer(fake.replies, Effect.succeed([{ connection_json: installationJson }]))
					const loaded = yield* store.get({ workspaceId })
					assert.deepStrictEqual(loaded, installation)
					assert.ok(Redacted.isRedacted(loaded?.credentials.botToken))
					assert.ok(!Inspectable.toStringUnknown(loaded).includes('private-token-sentinel'))
					assert.ok((yield* Queue.take(fake.commands)).sql.startsWith('SELECT'))
				}
				for (let index = 0; index < 2; index++) {
					yield* Queue.offer(fake.replies, Effect.succeed([]))
					assert.strictEqual(yield* store.remove({ workspaceId }), undefined)
					assert.ok((yield* Queue.take(fake.commands)).sql.startsWith('DELETE'))
				}
				assert.ok(
					yield* Schema.encodeEffect(Schema.toCodecJson(SlackConnection))(installation).pipe(
						Effect.isFailure,
					),
				)
			}).pipe(Effect.provide(connections.pipe(Layer.provide(fake.layer))))
		}),
)

it.effect('Postgres command seam: bounded retention, TTL refresh, and frozen route conflict command', () =>
	Effect.gen(function* () {
		const fake = yield* sqlCommands
		yield* Effect.gen(function* () {
			const store = yield* SlackSubscriptions
			yield* Queue.takeAll(fake.commands)
			for (const created of [true, false]) {
				yield* Queue.offer(fake.replies, Effect.succeed([{ created }]))
				assert.deepStrictEqual(
					yield* store.subscribe({ threadId: rootedThread.id }),
					created ? SubscriptionCreated.make({}) : SubscriptionExisting.make({}),
				)
				const commands = yield* Queue.takeAll(fake.commands)
				assert.strictEqual(commands.length, 3)
				assert.ok(commands.slice(0, 2).every(({ sql }) => sql.includes('LIMIT 128 FOR UPDATE SKIP LOCKED')))
				assert.ok(commands[2]?.sql.includes("interval '720 hours'"))
				assert.ok(
					commands[2]?.sql.includes(
						'created = humanlayer_slack_v1_subscriptions.expires_at <= statement_timestamp()',
					),
				)
			}
			yield* Queue.offer(fake.replies, Effect.succeed([{ subscribed: false }]))
			assert.strictEqual(yield* store.isSubscribed({ threadId: rootedThread.id }), false)
			assert.ok(
				(yield* Queue.takeAll(fake.commands)).at(-1)?.sql.includes('AND expires_at > statement_timestamp()'),
			)
			yield* Queue.offer(fake.replies, Effect.succeed([]))
			yield* store.unsubscribe({ threadId: rootedThread.id })
			assert.ok(
				(yield* Queue.takeAll(fake.commands))
					.at(-1)
					?.sql.startsWith('DELETE FROM humanlayer_slack_v1_subscriptions WHERE thread_id ='),
			)
			const frozen = { thread: proactiveThread, subscribed: true }
			yield* Queue.offer(fake.replies, Effect.succeed([{ route_json: encodeRoute(frozen) }]))
			assert.deepStrictEqual(yield* store.resolveDirectMessageRoute(routeInput), frozen)
			const route = (yield* Queue.takeAll(fake.commands)).at(-1)
			assert.ok(route?.sql.includes('ON CONFLICT (tenant, channel_id, event_id) DO UPDATE'))
			assert.ok(route?.sql.includes('ELSE humanlayer_slack_v1_routes.route_json END'))
			assert.ok(route?.sql.includes("interval '24 hours'"))
			assert.deepStrictEqual(route?.params, [
				rootedThread.channel.tenant,
				rootedThread.channel.id,
				routeInput.eventId,
				rootedThread.id,
				encodeRoute({ thread: rootedThread, subscribed: true }),
				proactiveThread.id,
				encodeRoute(frozen),
				encodeRoute({ thread: rootedThread, subscribed: false }),
			])
		}).pipe(Effect.provide(layer.pipe(Layer.provide(fake.layer))))
	}),
)

it.effect('Postgres errors: reject malformed rows and capture reason before narrow without plaintext', () =>
	Effect.gen(function* () {
		const fake = yield* sqlCommands
		const logs: Array<string> = []
		const logger = Logger.layer([Logger.make((entry) => logs.push(JSON.stringify(entry.message)))])
		yield* Effect.gen(function* () {
			const store = yield* SlackConnectionStore
			const subs = yield* SlackSubscriptions
			for (const rows of [
				[{ connection_json: 'private-token-sentinel' }],
				[{ connection_json: '{"credentials":{"botToken":"private-token-sentinel"}}' }],
				[{ connection_json: installationJson }, { connection_json: installationJson }],
			]) {
				yield* Queue.offer(fake.replies, Effect.succeed(rows))
				assert.deepStrictEqual(
					yield* store.get({ workspaceId }).pipe(Effect.flip),
					new SlackConnectionStoreError({ operation: 'get' }),
				)
			}
			const failure = Effect.fail(
				new SqlError({
					reason: new ConnectionError({ cause: 'private-token-sentinel', message: 'private-token-sentinel' }),
				}),
			)
			yield* Queue.offer(fake.replies, failure)
			assert.deepStrictEqual(
				yield* store.upsert({ workspaceId, connection: installation }).pipe(Effect.flip),
				new SlackConnectionStoreError({ operation: 'upsert' }),
			)
			yield* Queue.offer(fake.replies, failure)
			assert.deepStrictEqual(
				yield* store.remove({ workspaceId }).pipe(Effect.flip),
				new SlackConnectionStoreError({ operation: 'remove' }),
			)
			yield* Queue.offer(fake.replies, Effect.succeed([{ route_json: 'private-token-sentinel' }]))
			assert.deepStrictEqual(
				yield* subs.resolveDirectMessageRoute(routeInput).pipe(Effect.flip),
				new SubscriptionStoreError({ operation: 'resolveDirectMessageRoute', threadId: rootedThread.id }),
			)
			yield* Queue.offer(fake.replies, Effect.succeed([{ created: 'true' }]))
			assert.deepStrictEqual(
				yield* subs.subscribe({ threadId: rootedThread.id }).pipe(Effect.flip),
				new SubscriptionStoreError({ operation: 'subscribe', threadId: rootedThread.id }),
			)
			assert.ok(logs.some((log) => log.includes('ConnectionError') && log.includes('retryable')))
			assert.ok(logs.some((log) => log.includes('SchemaError') && log.includes('issue')))
			assert.ok(logs.every((log) => !log.includes('private-token-sentinel')))
		}).pipe(Effect.provide(Layer.merge(layer.pipe(Layer.provide(fake.layer)), logger)))
	}),
)

it.effect(
	'Postgres initialization failures are safely narrowed and roll back, including rc.112 migration defects',
	() =>
		Effect.gen(function* () {
			for (const phase of ['lock', 'statement']) {
				const fake = yield* sqlCommands
				const logs: Array<string> = []
				const logger = Logger.layer([Logger.make((entry) => logs.push(JSON.stringify(entry.message)))])
				const failure = Effect.fail(
					new SqlError({ reason: new ConnectionError({ cause: 'private-migration-sentinel' }) }),
				)
				if (phase === 'lock') yield* Queue.offer(fake.lockReplies, failure)
				if (phase === 'statement') {
					yield* Queue.offer(fake.migrationReplies, Effect.succeed([]))
					yield* Queue.offerAll(fake.ddlReplies, [Effect.void, failure])
				}
				assert.deepStrictEqual(
					yield* migrate.pipe(Effect.provide(Layer.merge(fake.layer, logger)), Effect.flip),
					new SlackConnectionStoreError({ operation: 'initialize' }),
				)
				assert.strictEqual((yield* Queue.takeAll(fake.commands)).at(-1)?.sql, 'ROLLBACK')
				assert.ok(logs.some((log) => log.includes('ConnectionError')))
				assert.ok(logs.every((log) => !log.includes('private-migration-sentinel')))
			}
		}),
)
