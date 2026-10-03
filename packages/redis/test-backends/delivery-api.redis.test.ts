/**
 * The delivery API over Redis, end to end: `Channels.make` with `ChannelsRedis.make` storage serves a
 * provider webhook and `bot.deliveryApi`, polls the store on its own, and a remote worker finishes a
 * handed-off delivery through the generated client.
 */
import { deliveryApiScenario } from '../../delivery/test/delivery-api-scenario'
import { ChannelsRedis } from '../src'
import { client, flushDatabase } from './redis'

deliveryApiScenario('redis', {
	namespace: 'channels-redis-test',
	storage: ChannelsRedis.make({ claimLimit: 10, polling: { intervalMs: 10 } }),
	client,
	empty: flushDatabase,
})
