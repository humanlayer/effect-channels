/**
 * Credentials for git over HTTPS as the GitHub App: clone, fetch, pull, and push a repository the App is
 * installed on. They use the same installation tokens as the GitHub API.
 */
import { Context, Effect, Layer, Redacted } from 'effect'
import { Base64 } from 'effect/encoding'

import { narrowGitHubTransportError } from './api/GitHubApiErrors'
import { GitHubAppCredentials } from './api/GitHubAppCredentials'
import type { GitHubApiError } from './GitHubApiContract'
import type { GitHubRepositoryRef } from './GitHubModels'

export class GitHubGitCredentials extends Context.Service<
	GitHubGitCredentials,
	{
		/**
		 * The `Authorization` header value for git requests to the repository. Keep it in requests only: never
		 * in a remote URL, `.git/config`, a log, or anything a model sees.
		 */
		readonly authorization: (input: {
			readonly repository: GitHubRepositoryRef
		}) => Effect.Effect<Redacted.Redacted<string>, GitHubApiError>
	}
>()('@humanlayer/channels-github/GitHubGitCredentials', {
	make: Effect.gen(function* () {
		const credentials = yield* GitHubAppCredentials
		return {
			authorization: ({ repository }) =>
				credentials.installationToken(repository).pipe(
					Effect.map(basicAuthorization),
					Effect.catchTag('GitHubTransportError', (error) =>
						Effect.fail(narrowGitHubTransportError('create_git_credentials', error)),
					),
					Effect.withSpan('github.git_credentials.authorization'),
				),
		}
	}),
}) {
	static readonly layer = Layer.effect(this, this.make)
}

/** Git authenticates an installation token as the password of the user `x-access-token`. */
const basicAuthorization = (token: Redacted.Redacted<string>) =>
	Redacted.make(`Basic ${Base64.encode(`x-access-token:${Redacted.value(token)}`)}`)
