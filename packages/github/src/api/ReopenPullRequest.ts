import { Effect, Schema } from 'effect'

import type { GitHubPullRequestRequest } from '../GitHubApi'
import { GitHubApiClient } from './GitHubApiClient'
import { pullRequestInfo, repositoryPath } from './GitHubApiProjections'
import { PullRequest } from './GitHubApiSchemas'

export const ReopenPullRequestBody = Schema.Struct({ state: Schema.Literal('open') })

export const reopenPullRequest = Effect.fn('github.api.reopen_pull_request')(function* (
	input: GitHubPullRequestRequest,
) {
	const api = yield* GitHubApiClient
	const value = yield* api.call({
		operation: 'reopen_pull_request',
		ref: input.pullRequest,
		method: 'PATCH',
		path: `${repositoryPath(input.pullRequest)}/pulls/${input.pullRequest.number}`,
		schema: PullRequest,
		body: { schema: ReopenPullRequestBody, value: { state: 'open' } },
	})
	return pullRequestInfo(input.pullRequest, value)
})
