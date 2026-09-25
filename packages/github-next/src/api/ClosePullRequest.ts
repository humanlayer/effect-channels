import { Effect } from 'effect'

import type { GitHubPullRequestRequest } from '../GitHubApi'
import { GitHubApiClient } from './GitHubApiClient'
import { pullRequestInfo, repositoryPath } from './GitHubApiProjections'
import { PullRequest } from './GitHubApiSchemas'
export const closePullRequest = Effect.fn('github.api.close_pull_request')(function* (input: GitHubPullRequestRequest) {
	const api = yield* GitHubApiClient
	const value = yield* api.call({
		operation: 'close_pull_request',
		ref: input.pullRequest,
		method: 'PATCH',
		path: `${repositoryPath(input.pullRequest)}/pulls/${input.pullRequest.number}`,
		schema: PullRequest,
		body: { state: 'closed' },
	})
	return pullRequestInfo(input.pullRequest, value)
})
