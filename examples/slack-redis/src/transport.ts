import { NodeCrypto } from '@effect/platform-node'
import { Layer } from 'effect'
import { FetchHttpClient } from 'effect/unstable/http'

import { storage } from './storage.ts'

export const transport = Layer.mergeAll(storage, FetchHttpClient.layer, NodeCrypto.layer)
