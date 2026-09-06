import * as RedisClient from '@humanlayer/channels-slack/redis/client'
import { Config, Redacted } from 'effect'

export const redisClient = RedisClient.layerConfig({
	url: Config.redacted('REDIS_URL').pipe(Config.map(Redacted.value)),
})
