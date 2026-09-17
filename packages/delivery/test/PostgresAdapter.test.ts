import { assert, it } from '@effect/vitest'
import { Effect, Layer, Logger, Queue } from 'effect'
import { ConnectionError, SqlError } from 'effect/unstable/sql/SqlError'

import { emptyMailbox } from '../src/Mailbox'
import { MailboxReadiness, MailboxStore, MailboxStoreError } from '../src/MailboxStore'
import { layer, migrate, PostgresInitializationError } from '../src/postgres'
import { encodeState, mailboxCodecCases, sqlCommands } from './AdapterCommands'

it.effect('Postgres fake command contract: lock precedes migrator creation; conditional writes include readiness', () =>
	Effect.gen(function* () {
		const fake = yield* sqlCommands
		yield* Effect.gen(function* () {
			const store = yield* MailboxStore
			const startup = yield* Queue.takeAll(fake.commands)
			const lock = startup.findIndex((command) => command.sql.includes('pg_advisory_xact_lock'))
			const create = startup.findIndex((command) => command.sql.includes('CREATE TABLE IF NOT EXISTS'))
			const probe = startup.findIndex((command) => command.sql.includes('::regclass'))
			assert.ok(lock > 0 && lock < create && create < probe)
			yield* Queue.offer(fake.replies, Effect.succeed([{ key: 'key' }]))
			assert.strictEqual(
				yield* store.commitMailbox({
					key: 'key',
					expectedRevision: null,
					nextState: { ...emptyMailbox(), readyAt: 42 },
				}),
				'committed',
			)
			const insert = (yield* Queue.takeAll(fake.commands)).find((command) =>
				command.sql.includes('ON CONFLICT (key) DO NOTHING RETURNING key'),
			)
			assert.isDefined(insert)
			assert.ok(insert.sql.includes('ON CONFLICT (key) DO NOTHING RETURNING key'))
			assert.deepStrictEqual(insert.params, ['key', 0, encodeState({ ...emptyMailbox(), readyAt: 42 }), 42])
			yield* Queue.offer(fake.replies, Effect.succeed([]))
			assert.strictEqual(
				yield* store.commitMailbox({ key: 'key', expectedRevision: 0, nextState: emptyMailbox() }),
				'conflict',
			)
			const update = (yield* Queue.takeAll(fake.commands)).find((command) =>
				command.sql.includes('WHERE key = $4 AND revision = $5 RETURNING key'),
			)
			assert.isDefined(update)
			assert.ok(update.sql.includes('WHERE key = $4 AND revision = $5 RETURNING key'))
			assert.deepStrictEqual(update.params, [1, encodeState(emptyMailbox()), null, 'key', 0])
		}).pipe(Effect.provide(layer.pipe(Layer.provide(fake.layer))))
	}),
)

it.effect('Postgres fake command contract: literal-prefix scan filters before its bound', () =>
	Effect.gen(function* () {
		const fake = yield* sqlCommands
		yield* Effect.gen(function* () {
			const readiness = yield* MailboxReadiness
			yield* Queue.takeAll(fake.commands)
			yield* Queue.offer(fake.replies, Effect.succeed([{ key: 'app%_!\\key' }]))
			assert.deepStrictEqual(yield* readiness.scanReady({ prefix: 'app%_!\\', now: 15, limit: 1 }), [
				'app%_!\\key',
			])
			const scan = yield* Queue.take(fake.commands)
			assert.ok(scan.sql.includes("WHERE key LIKE $1 ESCAPE '!' AND ready_at <= $2"))
			assert.ok(scan.sql.includes('ORDER BY ready_at, key LIMIT $3'))
			assert.deepStrictEqual(scan.params, ['app!%!_!!\\%', 15, 1])
			assert.deepStrictEqual(
				yield* readiness.scanReady({ prefix: '', now: 1, limit: 0 }).pipe(Effect.flip),
				MailboxStoreError.make({ operation: 'scan' }),
			)
			assert.strictEqual(yield* Queue.size(fake.commands), 0)
		}).pipe(Effect.provide(layer.pipe(Layer.provide(fake.layer))))
	}),
)

it.effect('Postgres locator upsert deduplicates one delivery shared by multiple outcomes', () =>
	Effect.gen(function* () {
		const fake = yield* sqlCommands
		yield* Effect.gen(function* () {
			const store = yield* MailboxStore
			yield* Queue.takeAll(fake.commands)
			const deliveryId = 'delivery:v2:shared-outcome'
			const state = {
				...emptyMailbox(),
				outcomes: [
					{ identity: 'first', kind: 'completed' as const, expiresAt: 1000, deliveryId },
					{ identity: 'second', kind: 'completed' as const, expiresAt: 1000, deliveryId },
				],
			}
			yield* Queue.offer(fake.replies, Effect.succeed([{ key: 'duplicate-locator' }]))
			yield* Queue.offer(fake.replies, Effect.succeed([{ delivery_id: deliveryId }]))
			assert.strictEqual(
				yield* store.commitMailbox({ key: 'duplicate-locator', expectedRevision: null, nextState: state }),
				'committed',
			)
			const locator = (yield* Queue.takeAll(fake.commands)).find((command) =>
				command.sql.startsWith('INSERT INTO humanlayer_delivery_v1_locators'),
			)
			assert.isDefined(locator)
			assert.deepStrictEqual(locator.params, ['duplicate-locator', `["${deliveryId}"]`])
		}).pipe(Effect.provide(layer.pipe(Layer.provide(fake.layer))))
	}),
)

it.effect('Postgres SQL seam decodes complete snapshots and rejects corruption with safe logs', () =>
	Effect.gen(function* () {
		const fake = yield* sqlCommands
		const logs: Array<string> = []
		const logger = Logger.layer([Logger.make((entry) => logs.push(JSON.stringify(entry.message)))])
		yield* Effect.gen(function* () {
			const store = yield* MailboxStore
			yield* Queue.offer(fake.replies, Effect.succeed([]))
			assert.strictEqual(yield* store.loadMailbox({ key: 'absent' }), undefined)
			for (const state of mailboxCodecCases) {
				yield* Queue.offer(
					fake.replies,
					Effect.succeed([{ revision: 7, state_json: encodeState(state), ready_at: state.readyAt }]),
				)
				assert.deepStrictEqual(yield* store.loadMailbox({ key: 'key' }), { revision: 7, state })
			}
			for (const state of [
				'private-payload-sentinel',
				encodeState(emptyMailbox()).replace('"version":5', '"version":99'),
			]) {
				yield* Queue.offer(fake.replies, Effect.succeed([{ revision: 7, state_json: state, ready_at: null }]))
				assert.deepStrictEqual(
					yield* store.loadMailbox({ key: 'key' }).pipe(Effect.flip),
					MailboxStoreError.make({ operation: 'load' }),
				)
			}
			yield* Queue.offer(
				fake.replies,
				Effect.succeed([{ revision: 7, state_json: encodeState(emptyMailbox()), ready_at: 12 }]),
			)
			assert.deepStrictEqual(
				yield* store.loadMailbox({ key: 'key' }).pipe(Effect.flip),
				MailboxStoreError.make({ operation: 'load' }),
			)
			yield* Queue.offer(
				fake.replies,
				Effect.fail(
					new SqlError({
						reason: new ConnectionError({
							cause: 'private-payload-sentinel',
							message: 'private-payload-sentinel',
						}),
					}),
				),
			)
			assert.deepStrictEqual(
				yield* store
					.commitMailbox({ key: 'key', expectedRevision: 7, nextState: emptyMailbox() })
					.pipe(Effect.flip),
				MailboxStoreError.make({ operation: 'commit' }),
			)
			assert.ok(logs.some((log) => log.includes('SchemaError')))
			assert.ok(logs.some((log) => log.includes('ConnectionError')))
			assert.ok(logs.every((log) => !log.includes('private-payload-sentinel')))
		}).pipe(Effect.provide(Layer.merge(layer.pipe(Layer.provide(fake.layer)), logger)))
	}),
)

it.effect(
	'Postgres migration command failure is logged safely, narrowed and rolled back before store acquisition',
	() =>
		Effect.gen(function* () {
			const fake = yield* sqlCommands
			const logs: Array<string> = []
			const logger = Logger.layer([Logger.make((entry) => logs.push(JSON.stringify(entry.message)))])
			yield* Queue.offer(
				fake.lockReplies,
				Effect.fail(new SqlError({ reason: new ConnectionError({ cause: 'private-migration-sentinel' }) })),
			)
			assert.deepStrictEqual(
				yield* migrate.pipe(Effect.provide(Layer.merge(fake.layer, logger)), Effect.flip),
				new PostgresInitializationError(),
			)
			const commands = yield* Queue.takeAll(fake.commands)
			assert.strictEqual(commands.at(-1)?.sql, 'ROLLBACK')
			assert.ok(commands.every((command) => !command.sql.includes('CREATE TABLE')))
			assert.ok(logs.some((log) => log.includes('ConnectionError')))
			assert.ok(logs.every((log) => !log.includes('private-migration-sentinel')))
		}),
)
