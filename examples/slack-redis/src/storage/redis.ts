import { layer } from '@humanlayer/channels-slack/redis'
import { Layer } from 'effect'

import { redisClient } from './redis-client.js'

export const storage = layer.pipe(Layer.provide(redisClient))
