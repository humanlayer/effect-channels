import { assert, it } from '@effect/vitest'
import { Effect, Inspectable, Layer, Logger, Queue, Redacted } from 'effect'
import * as Redis from 'effect/unstable/persistence/Redis'

import { SubscriptionStoreError } from '../src/DomainErrors'
import { ThreadId } from '../src/Model'
import { layer } from '../src/redis'
import { connectionKey, routeKey, subscriptionKey } from '../src/redis/keys'
import { SlackConnectionStore, SlackConnectionStoreError } from '../src/SlackConnectionStore'
import { SubscriptionCreated, SubscriptionExisting } from '../src/SlackEvents'
import { SlackSubscriptions } from '../src/SlackSubscriptions'
import { redisCommands } from './AdapterCommands'
import {
	encodeRoute,
	installation,
	installationJson,
	proactiveThread,
	rootedThread,
	routeInput,
	workspaceId,
} from './AdapterFixtures'

it.effect('Redis command seam: direct authoritative connection commands, private codec, no expiry', () =>
	Effect.gen(function* () {
		const fake = yield* redisCommands
		yield* Effect.gen(function* () {
			const store = yield* SlackConnectionStore
			yield* SlackSubscriptions
			assert.strictEqual(yield* Queue.size(fake.commands), 0)
			yield* Queue.offer(fake.replies, Effect.succeed(null))
			assert.strictEqual(yield* store.get({ workspaceId }), undefined)
			assert.deepStrictEqual(yield* Queue.take(fake.commands), {
				command: 'GET',
				args: [connectionKey({ workspaceId })],
			})
			yield* Queue.offer(fake.replies, Effect.succeed('OK'))
			assert.strictEqual(yield* store.upsert({ workspaceId, connection: installation }), undefined)
			assert.deepStrictEqual(yield* Queue.take(fake.commands), {
				command: 'SET',
				args: [connectionKey({ workspaceId }), installationJson],
			})
			for (let index = 0; index < 2; index++) {
				yield* Queue.offer(fake.replies, Effect.succeed(installationJson))
				const loaded = yield* store.get({ workspaceId })
				assert.deepStrictEqual(loaded, installation)
				assert.ok(Redacted.isRedacted(loaded?.credentials.botToken))
				assert.ok(!Inspectable.toStringUnknown(loaded).includes('private-token-sentinel'))
				assert.strictEqual((yield* Queue.take(fake.commands)).command, 'GET')
			}
			for (const removed of [1, 0]) {
				yield* Queue.offer(fake.replies, Effect.succeed(removed))
				assert.strictEqual(yield* store.remove({ workspaceId }), undefined)
				assert.deepStrictEqual(yield* Queue.take(fake.commands), {
					command: 'DEL',
					args: [connectionKey({ workspaceId })],
				})
			}
		}).pipe(Effect.provide(layer.pipe(Layer.provide(fake.layer))))
	}),
)

it.effect('Redis command seam: direct subscription keys and same-slot route script with bounded TTLs', () =>
	Effect.gen(function* () {
		const fake = yield* redisCommands
		yield* Effect.gen(function* () {
			const store = yield* SlackSubscriptions
			for (const created of [1, 0]) {
				yield* Queue.offer(fake.replies, Effect.succeed(created))
				assert.deepStrictEqual(
					yield* store.subscribe({ threadId: rootedThread.id }),
					created === 1 ? SubscriptionCreated.make({}) : SubscriptionExisting.make({}),
				)
				const command = yield* Queue.take(fake.commands)
				assert.strictEqual(command.command, 'EVAL')
				assert.deepStrictEqual(command.args.slice(1), [
					'1',
					subscriptionKey({ threadId: rootedThread.id }),
					'2592000000',
				])
			}
			for (const subscribed of [null, '1']) {
				yield* Queue.offer(fake.replies, Effect.succeed(subscribed))
				assert.strictEqual(yield* store.isSubscribed({ threadId: rootedThread.id }), subscribed === '1')
				assert.deepStrictEqual(yield* Queue.take(fake.commands), {
					command: 'GET',
					args: [subscriptionKey({ threadId: rootedThread.id })],
				})
			}
			yield* Queue.offer(fake.replies, Effect.succeed(1))
			yield* store.unsubscribe({ threadId: rootedThread.id })
			assert.deepStrictEqual(yield* Queue.take(fake.commands), {
				command: 'DEL',
				args: [subscriptionKey({ threadId: rootedThread.id })],
			})
			const frozen = { thread: proactiveThread, subscribed: true }
			const frozenJson = yield* encodeRoute(frozen)
			yield* Queue.offer(fake.replies, Effect.succeed(frozenJson))
			assert.deepStrictEqual(yield* store.resolveDirectMessageRoute(routeInput), frozen)
			const command = yield* Queue.take(fake.commands)
			assert.strictEqual(command.command, 'EVAL')
			assert.strictEqual(command.args[1], '3')
			assert.deepStrictEqual(command.args.slice(2, 5), [
				routeKey(routeInput),
				subscriptionKey({ threadId: rootedThread.id }),
				subscriptionKey({ threadId: proactiveThread.id }),
			])
			assert.ok(command.args.slice(2, 5).every((key) => key.match(/\{([^}]+)\}/u)?.[1] === 'state'))
			assert.deepStrictEqual(command.args.slice(5), [
				yield* encodeRoute({ thread: rootedThread, subscribed: true }),
				frozenJson,
				yield* encodeRoute({ thread: rootedThread, subscribed: false }),
				'86400000',
			])
			assert.notStrictEqual(
				subscriptionKey({ threadId: ThreadId.make('\ud83d') }),
				subscriptionKey({ threadId: ThreadId.make('\ufffd') }),
			)
		}).pipe(Effect.provide(layer.pipe(Layer.provide(fake.layer))))
	}),
)

it.effect('Redis error seam: reject malformed replies and preserve safe transport metadata, never tokens', () =>
	Effect.gen(function* () {
		const fake = yield* redisCommands
		const logs: Array<string> = []
		const logger = Logger.layer([Logger.make((entry) => logs.push(JSON.stringify(entry.message)))])
		yield* Effect.gen(function* () {
			const store = yield* SlackConnectionStore
			const subs = yield* SlackSubscriptions
			for (const reply of [
				undefined,
				'private-token-sentinel',
				'{"credentials":{"botToken":"private-token-sentinel"}}',
			]) {
				yield* Queue.offer(fake.replies, Effect.succeed(reply))
				assert.deepStrictEqual(
					yield* store.get({ workspaceId }).pipe(Effect.flip),
					new SlackConnectionStoreError({ operation: 'get' }),
				)
			}
			yield* Queue.offer(fake.replies, Effect.succeed(null))
			assert.deepStrictEqual(
				yield* store.upsert({ workspaceId, connection: installation }).pipe(Effect.flip),
				new SlackConnectionStoreError({ operation: 'upsert' }),
			)
			yield* Queue.offer(
				fake.replies,
				Effect.fail(new Redis.RedisError({ cause: { code: 'ECONNRESET', message: 'private-token-sentinel' } })),
			)
			assert.deepStrictEqual(
				yield* store.remove({ workspaceId }).pipe(Effect.flip),
				new SlackConnectionStoreError({ operation: 'remove' }),
			)
			yield* Queue.offer(fake.replies, Effect.succeed(2))
			assert.deepStrictEqual(
				yield* subs.subscribe({ threadId: rootedThread.id }).pipe(Effect.flip),
				new SubscriptionStoreError({ operation: 'subscribe', threadId: rootedThread.id }),
			)
			yield* Queue.offer(fake.replies, Effect.succeed('private-token-sentinel'))
			assert.deepStrictEqual(
				yield* subs.resolveDirectMessageRoute(routeInput).pipe(Effect.flip),
				new SubscriptionStoreError({ operation: 'resolveDirectMessageRoute', threadId: rootedThread.id }),
			)
			yield* Queue.offer(fake.replies, Effect.succeed('invalid'))
			assert.deepStrictEqual(
				yield* subs.isSubscribed({ threadId: rootedThread.id }).pipe(Effect.flip),
				new SubscriptionStoreError({ operation: 'isSubscribed', threadId: rootedThread.id }),
			)
			yield* Queue.offer(
				fake.replies,
				Effect.fail(new Redis.RedisError({ cause: new Error('WRONGTYPE private-token-sentinel') })),
			)
			assert.deepStrictEqual(
				yield* subs.unsubscribe({ threadId: rootedThread.id }).pipe(Effect.flip),
				new SubscriptionStoreError({ operation: 'unsubscribe', threadId: rootedThread.id }),
			)
			assert.ok(logs.some((log) => log.includes('ECONNRESET')))
			assert.ok(logs.some((log) => log.includes('WRONGTYPE')))
			assert.ok(logs.some((log) => log.includes('SchemaError') && log.includes('issue')))
			assert.ok(logs.every((log) => !log.includes('private-token-sentinel')))
		}).pipe(Effect.provide(Layer.merge(layer.pipe(Layer.provide(fake.layer)), logger)))
	}),
)
