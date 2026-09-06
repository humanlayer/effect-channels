import { NodeCrypto } from '@effect/platform-node'
import { SlackClient, SlackTenantCredentials } from '@humanlayer/channels-slack'
import { Layer } from 'effect'
import { FetchHttpClient } from 'effect/unstable/http'

export const transport = Layer.merge(SlackClient.layer, NodeCrypto.layer).pipe(
	Layer.provideMerge(SlackTenantCredentials.layerFromConfig),
	Layer.provide(FetchHttpClient.layer),
)
