import { NodeCrypto } from '@effect/platform-node'
import { GitHubCredentials, GitHubCrypto } from '@humanlayer/channels-github'
import { subscriptions } from '@humanlayer/channels-github/memory'
import { layerFromConfig } from '@humanlayer/channels-slack/memory'
import { Layer } from 'effect'
import { FetchHttpClient } from 'effect/unstable/http'

const githubTransport = GitHubCredentials.layerConfig.pipe(
	Layer.provideMerge(GitHubCrypto.layerWebCrypto),
	Layer.provideMerge(FetchHttpClient.layer),
)

export const transport = Layer.mergeAll(githubTransport, subscriptions(), layerFromConfig, NodeCrypto.layer)
