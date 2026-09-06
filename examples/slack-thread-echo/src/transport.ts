import { NodeCrypto } from '@effect/platform-node'
import { layerFromConfig } from '@humanlayer/channels-slack/memory'
import { Layer } from 'effect'
import { FetchHttpClient } from 'effect/unstable/http'

export const transport = Layer.mergeAll(layerFromConfig, FetchHttpClient.layer, NodeCrypto.layer)
