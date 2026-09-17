import { assert, it } from '@effect/vitest'
import { Effect, Layer, Logger, Queue, Schema } from 'effect'
import * as Redis from 'effect/unstable/persistence/Redis'

import { emptyMailbox, MailboxSnapshot, mailboxKey, mailboxPrefix } from '../src/Mailbox'
import { MailboxReadiness, MailboxStore, MailboxStoreError } from '../src/MailboxStore'
import { layer } from '../src/redis'
import { readyKey, readyKeys, recordKey } from '../src/redis/keys'
import { encodeKey, encodeSnapshot, mailboxCodecCases, redisCommands } from './AdapterCommands'

it.effect(
	'Redis fake command contract: CAS sends one same-slot script containing snapshot and every literal-prefix index',
	() =>
		Effect.gen(function* () {
			const fake = yield* redisCommands
			const address = {
				namespace: 'app%_!{other}',
				handlerId: 'reply',
				provider: 'test',
				installation: 'T',
				resourceKey: '😀:root',
			}
			const key = mailboxKey(address)
			yield* Effect.gen(function* () {
				const store = yield* MailboxStore
				yield* Queue.offer(fake.replies, Effect.succeed(1))
				assert.strictEqual(
					yield* store.commitMailbox({
						key,
						expectedRevision: null,
						nextState: { ...emptyMailbox(), readyAt: 0 },
					}),
					'committed',
				)
				const script = yield* Queue.take(fake.commands)
				assert.strictEqual(script.command, 'EVAL')
				assert.strictEqual(script.args[1], String(key.length + 2))
				const keys = script.args.slice(2, key.length + 4)
				assert.deepStrictEqual(keys, [recordKey({ key }), ...readyKeys({ key })])
				assert.ok(keys.every((entry) => entry.match(/\{([^}]+)\}/u)?.[1] === 'mailboxes'))
				assert.ok(keys.includes(readyKey({ prefix: mailboxPrefix(address) })))
				const args = script.args.slice(key.length + 4)
				assert.strictEqual(args[0], '')
				assert.strictEqual(args[1], '0')
				assert.deepStrictEqual(
					yield* Schema.decodeUnknownEffect(Schema.fromJsonString(MailboxSnapshot))(args[2]),
					{ revision: 0, state: { ...emptyMailbox(), readyAt: 0 } },
				)
				assert.deepStrictEqual(args.slice(3), ['0', encodeKey(key), '[]'])
				yield* Queue.offer(fake.replies, Effect.succeed(0))
				assert.strictEqual(
					yield* store.commitMailbox({ key, expectedRevision: 5, nextState: emptyMailbox() }),
					'conflict',
				)
				assert.deepStrictEqual((yield* Queue.take(fake.commands)).args.slice(-6), [
					'5',
					'6',
					encodeSnapshot({ revision: 6, state: emptyMailbox() }),
					'',
					encodeKey(key),
					'[]',
				])
			}).pipe(Effect.provide(layer.pipe(Layer.provide(fake.layer))))
		}),
)

it.effect(
	'Redis fake command contract: readiness is an indexed bounded literal-prefix command, never a global scan',
	() =>
		Effect.gen(function* () {
			const fake = yield* redisCommands
			yield* Effect.gen(function* () {
				const readiness = yield* MailboxReadiness
				for (const prefix of ['', 'app%_*?', '😀', '\ud83d']) {
					yield* Queue.offer(fake.replies, Effect.succeed([encodeKey(`${prefix}key`)]))
					assert.deepStrictEqual(yield* readiness.scanReady({ prefix, now: 0, limit: 1 }), [`${prefix}key`])
					assert.deepStrictEqual(yield* Queue.take(fake.commands), {
						command: 'ZRANGEBYSCORE',
						args: [readyKey({ prefix }), '-inf', '0', 'LIMIT', '0', '1'],
					})
				}
				yield* Queue.offer(fake.replies, Effect.succeed([encodeKey('unrelated')]))
				assert.deepStrictEqual(
					yield* readiness.scanReady({ prefix: 'mine', now: 0, limit: 1 }).pipe(Effect.flip),
					MailboxStoreError.make({ operation: 'scan' }),
				)
				assert.notStrictEqual(readyKey({ prefix: '\ud83d' }), readyKey({ prefix: '\ufffd' }))
			}).pipe(Effect.provide(layer.pipe(Layer.provide(fake.layer))))
		}),
)

it.effect('Redis seam decodes snapshots and rejects malformed replies without logging stored payloads', () =>
	Effect.gen(function* () {
		const fake = yield* redisCommands
		const logs: Array<string> = []
		const logger = Logger.layer([Logger.make((entry) => logs.push(JSON.stringify(entry.message)))])
		yield* Effect.gen(function* () {
			const store = yield* MailboxStore
			yield* Queue.offer(fake.replies, Effect.succeed([null, null]))
			assert.strictEqual(yield* store.loadMailbox({ key: 'key' }), undefined)
			for (const state of mailboxCodecCases) {
				yield* Queue.offer(fake.replies, Effect.succeed(['0', encodeSnapshot({ revision: 0, state })]))
				assert.deepStrictEqual(yield* store.loadMailbox({ key: 'key' }), { revision: 0, state })
			}
			for (const reply of [
				[],
				['0', null],
				['0', 'private-payload-sentinel'],
				['1', encodeSnapshot({ revision: 0, state: emptyMailbox() })],
			]) {
				yield* Queue.offer(fake.replies, Effect.succeed(reply))
				assert.deepStrictEqual(
					yield* store.loadMailbox({ key: 'key' }).pipe(Effect.flip),
					MailboxStoreError.make({ operation: 'load' }),
				)
			}
			yield* Queue.offer(fake.replies, Effect.succeed('1'))
			assert.deepStrictEqual(
				yield* store
					.commitMailbox({ key: 'key', expectedRevision: null, nextState: emptyMailbox() })
					.pipe(Effect.flip),
				MailboxStoreError.make({ operation: 'commit' }),
			)
			yield* Queue.offer(fake.replies, Effect.fail(new Redis.RedisError({ cause: 'private-payload-sentinel' })))
			assert.deepStrictEqual(
				yield* store.loadMailbox({ key: 'key' }).pipe(Effect.flip),
				MailboxStoreError.make({ operation: 'load' }),
			)
			assert.ok(logs.some((log) => log.includes('SchemaError')))
			assert.ok(logs.some((log) => log.includes('RedisError')))
			assert.ok(logs.every((log) => !log.includes('private-payload-sentinel')))
		}).pipe(Effect.provide(Layer.merge(layer.pipe(Layer.provide(fake.layer)), logger)))
	}),
)
