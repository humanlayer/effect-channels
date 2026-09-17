import assert from 'node:assert/strict'

import * as DeliveryPostgres from '@humanlayer/channels-delivery/postgres'
import * as DeliveryPostgresClient from '@humanlayer/channels-delivery/postgres/client'
import * as DeliveryRedis from '@humanlayer/channels-delivery/redis'
import * as DeliveryRedisClient from '@humanlayer/channels-delivery/redis/client'
import * as SlackPostgres from '@humanlayer/channels-slack/postgres'
import * as SlackPostgresClient from '@humanlayer/channels-slack/postgres/client'
import * as SlackRedis from '@humanlayer/channels-slack/redis'
import * as SlackRedisClient from '@humanlayer/channels-slack/redis/client'
import { Layer } from 'effect'

import { assertBuiltEntry, assertOneEffect } from './guard'

for (const name of ['@humanlayer/channels-delivery', '@humanlayer/channels-slack']) {
	for (const entry of ['postgres', 'redis', 'postgres/client', 'redis/client']) {
		assertBuiltEntry(`${name}/${entry}`, entry)
	}
}

for (const backend of [DeliveryPostgres, DeliveryRedis, SlackPostgres, SlackRedis]) {
	assert.ok(Layer.isLayer(backend.layer))
}

assert.equal(DeliveryPostgresClient.layer, SlackPostgresClient.layer)
assert.equal(DeliveryPostgresClient.layerConfig, SlackPostgresClient.layerConfig)
assert.equal(DeliveryRedisClient.layer, SlackRedisClient.layer)
assert.equal(DeliveryRedisClient.layerConfig, SlackRedisClient.layerConfig)
assertOneEffect()
