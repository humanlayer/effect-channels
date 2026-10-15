import { Layer } from 'effect'
import * as FetchHttpClient from 'effect/http/FetchHttpClient'

import { GitHubApiClient } from './api/GitHubApiClient'
import { GitHubAppCredentials } from './api/GitHubAppCredentials'
import { GitHubAppSigner } from './api/GitHubAppSigner'
import { GitHubApi } from './GitHubApi'
import { GitHubGitCredentials } from './GitHubGitCredentials'
export { GitHubAppSigner } from './api/GitHubAppSigner'

/**
 * Live GitHub API and git credentials with injectable Effect HTTP transport and signer. Both share one
 * cache of installation tokens.
 */
export const GitHubApiLiveBase = Layer.merge(
	GitHubApi.layer.pipe(Layer.provide(GitHubApiClient.layer)),
	GitHubGitCredentials.layer,
).pipe(Layer.provide(GitHubAppCredentials.layer))

/** GitHub API and git credentials with Web Crypto signing and the standard Fetch transport. */
export const GitHubApiLive = GitHubApiLiveBase.pipe(
	Layer.provide(GitHubAppSigner.layer),
	Layer.provide(FetchHttpClient.layer),
)
