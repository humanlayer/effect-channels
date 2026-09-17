import { assert, it } from '@effect/vitest'
import { Config, Context, Effect, Layer, Schema } from 'effect'
import * as Redis from 'effect/unstable/persistence/Redis'

import { SubscriptionStoreError } from '../src/DomainErrors'
import { layer } from '../src/redis'
import * as Client from '../src/redis/client'
import { connectionKey, routeKey, subscriptionKey } from '../src/redis/keys'
import { SlackConnectionStore } from '../src/SlackConnectionStore'
import { SubscriptionCreated } from '../src/SlackEvents'
import { SlackSubscriptions } from '../src/SlackSubscriptions'
import {
	installation,
	installationJson,
	proactiveThread,
	rootedThread,
	routeInput,
	workspaceId,
} from '../test/AdapterFixtures'
import { botContract } from './BotContract'
import { OtherConnections, OtherSubscriptions, replicaContract } from './StoreContract'

const client = Layer.unwrap(
	Effect.gen(function* () {
		yield* Config.schema(Schema.Literal('disposable'), 'DELIVERY_BACKEND_TEST_CONFIRM')
		return Client.layer({
			socket: { host: '127.0.0.1', port: 56379, connectTimeout: 3000, reconnectStrategy: false },
		})
	}),
)

const replicas = Layer.effectContext(
	Effect.gen(function* () {
		const [first, second] = yield* Effect.all(
			[
				Layer.build(Layer.fresh(layer.pipe(Layer.provide(client)))),
				Layer.build(Layer.fresh(layer.pipe(Layer.provide(client)))),
			],
			{ concurrency: 2 },
		)
		return first.pipe(
			Context.add(OtherConnections, Context.get(second, SlackConnectionStore)),
			Context.add(OtherSubscriptions, Context.get(second, SlackSubscriptions)),
		)
	}),
)

it.effect(
	'disposable Redis: cross-client Lua routing, reconstruction, expiry, no connection TTL and NOSCRIPT reload',
	() =>
		Effect.gen(function* () {
			const redis = yield* Redis.Redis
			assert.strictEqual(
				yield* redis.send('DBSIZE'),
				0,
				'Use fresh disposable Redis; existing data is never reset',
			)
			yield* replicaContract.pipe(Effect.provide(replicas))
			assert.strictEqual(yield* redis.send('GET', connectionKey({ workspaceId })), installationJson)
			assert.strictEqual(yield* redis.send('PTTL', connectionKey({ workspaceId })), -1)
			yield* Effect.gen(function* () {
				const connections = yield* SlackConnectionStore
				const subscriptions = yield* SlackSubscriptions
				assert.deepStrictEqual(yield* connections.get({ workspaceId }), installation)
				assert.strictEqual(
					(yield* subscriptions.resolveDirectMessageRoute(routeInput)).thread.id,
					rootedThread.id,
				)
				const ttl = yield* Schema.decodeUnknownEffect(Schema.Int)(
					yield* redis.send('PTTL', routeKey(routeInput)),
				)
				assert.ok(ttl > 86390000 && ttl <= 86400000)
				const subTtl = yield* Schema.decodeUnknownEffect(Schema.Int)(
					yield* redis.send('PTTL', subscriptionKey({ threadId: proactiveThread.id })),
				)
				assert.ok(subTtl > 2591990000 && subTtl <= 2592000000)
				yield* redis.send('PEXPIRE', subscriptionKey({ threadId: proactiveThread.id }), '0')
				assert.strictEqual(yield* subscriptions.isSubscribed({ threadId: proactiveThread.id }), false)
				assert.deepStrictEqual(
					yield* subscriptions.subscribe({ threadId: proactiveThread.id }),
					SubscriptionCreated.make({}),
				)
				yield* redis.send('PEXPIRE', routeKey(routeInput), '0')
				assert.deepStrictEqual(yield* subscriptions.resolveDirectMessageRoute(routeInput), {
					thread: proactiveThread,
					subscribed: true,
				})
				yield* redis.send('SCRIPT', 'FLUSH', 'SYNC')
				assert.deepStrictEqual(yield* subscriptions.resolveDirectMessageRoute(routeInput), {
					thread: proactiveThread,
					subscribed: true,
				})
				assert.strictEqual(yield* redis.send('PTTL', connectionKey({ workspaceId })), -1)
				yield* redis.send('DEL', routeKey(routeInput), subscriptionKey({ threadId: proactiveThread.id }))
				yield* redis.send('LPUSH', subscriptionKey({ threadId: proactiveThread.id }), 'wrong-type')
				assert.deepStrictEqual(
					yield* subscriptions.resolveDirectMessageRoute(routeInput).pipe(Effect.flip),
					new SubscriptionStoreError({ operation: 'resolveDirectMessageRoute', threadId: rootedThread.id }),
				)
				assert.strictEqual(yield* redis.send('GET', routeKey(routeInput)), null)
				yield* redis.send('DEL', subscriptionKey({ threadId: proactiveThread.id }))
			}).pipe(Effect.provide(Layer.fresh(layer)))
			yield* botContract.pipe(Effect.provide(Layer.fresh(layer)))
		}).pipe(Effect.provide(client)),
	{ timeout: 30_000 },
)
