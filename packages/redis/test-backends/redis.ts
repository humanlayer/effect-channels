/**
 * The disposable Redis the Redis store's backend suites run against, and an empty store over it.
 *
 * Building `emptyStore` runs FLUSHDB, so the suites refuse to run unless DELIVERY_BACKEND_TEST_CONFIRM
 * says the Redis is disposable. The port comes from REDIS_CONTRACT_TEST_PORT.
 */
import * as NodeRedis from '@effect/platform-node/NodeRedis'
import { Config, Effect, Layer, Schema } from 'effect'
import * as Redis from 'effect/persistence/Redis'

import { DeliveryControlBackendRedis, MailboxDeliveryRedis, MailboxProcessingBackendRedis } from '../src'

/** A new connection to the disposable Redis each time it is built. */
export const client = Layer.unwrap(
	Effect.gen(function* () {
		yield* Config.schema(Schema.Literal('disposable'), 'DELIVERY_BACKEND_TEST_CONFIRM')
		const port = yield* Config.Port('REDIS_CONTRACT_TEST_PORT')
		return NodeRedis.layer({
			socket: { host: '127.0.0.1', port, connectTimeout: 3000, reconnectStrategy: false },
		})
	}),
)

/** Empty the whole database. */
export const flushDatabase = Effect.gen(function* () {
	yield* (yield* Redis.Redis).send('FLUSHDB')
})

/** The store's three services over `Redis`, the way `ChannelsRedis.make` builds them. */
export const storeOverClient = (claimLimit = 100) =>
	Layer.mergeAll(MailboxDeliveryRedis, MailboxProcessingBackendRedis({ claimLimit }), DeliveryControlBackendRedis)

/** An empty store with a connection of its own. Each build empties the database again. */
export const emptyStore = storeOverClient().pipe(
	Layer.provide(Layer.effectDiscard(flushDatabase)),
	Layer.provideMerge(client),
)
