import { GitHubCredentials, GitHubCrypto } from '@humanlayer/channels-github'
import { layer } from '@humanlayer/channels-github/memory'
import { Layer } from 'effect'
import { FetchHttpClient } from 'effect/unstable/http'

export const githubTransport = GitHubCredentials.layerConfig.pipe(
	Layer.provideMerge(GitHubCrypto.layerWebCrypto),
	Layer.provideMerge(FetchHttpClient.layer),
)
export const transport = Layer.merge(githubTransport, layer({ maxMailboxes: 10_000 }))
