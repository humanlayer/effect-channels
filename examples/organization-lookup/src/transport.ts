import { NodeCrypto } from '@effect/platform-node'
import { GitHubCredentials, GitHubCrypto } from '@humanlayer/channels-github'
import { connectionsFromConfig } from '@humanlayer/channels-slack/memory'
import { Layer } from 'effect'
import { FetchHttpClient } from 'effect/unstable/http'

export const credentials = Layer.merge(
	connectionsFromConfig,
	GitHubCredentials.layerConfig.pipe(Layer.provide(GitHubCrypto.layerWebCrypto)),
)

export const transport = credentials.pipe(
	Layer.provideMerge(FetchHttpClient.layer),
	Layer.merge(GitHubCrypto.layerWebCrypto),
	Layer.merge(NodeCrypto.layer),
)
