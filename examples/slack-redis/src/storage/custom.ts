import { layer as delivery } from '@humanlayer/channels-delivery/redis'
import { connections } from '@humanlayer/channels-slack/postgres'
import { subscriptions } from '@humanlayer/channels-slack/redis'
import { Layer } from 'effect'

import { postgresClient } from './postgres-client.ts'
import { redisClient } from './redis-client.ts'

export const storage = Layer.merge(
	connections.pipe(Layer.provide(postgresClient)),
	Layer.merge(subscriptions, delivery).pipe(Layer.provide(redisClient)),
)
