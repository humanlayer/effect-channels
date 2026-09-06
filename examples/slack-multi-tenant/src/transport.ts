import { NodeCrypto } from '@effect/platform-node'
import { PgClient } from '@effect/sql-pg'
import { layer as storage } from '@humanlayer/channels-slack/postgres'
import { Config, Layer } from 'effect'
import { FetchHttpClient } from 'effect/unstable/http'

const stores = storage.pipe(Layer.provide(PgClient.layerConfig({ url: Config.redacted('DATABASE_URL') })))

export const transport = Layer.mergeAll(stores, FetchHttpClient.layer, NodeCrypto.layer)
