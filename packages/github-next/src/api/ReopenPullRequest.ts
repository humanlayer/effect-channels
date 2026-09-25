import { Effect } from 'effect'

import type { GitHubPullRequestRequest } from '../GitHubApi'
import { GitHubApiClient } from './GitHubApiClient'
import { pullRequestInfo, repositoryPath } from './GitHubApiProjections'
import { PullRequest } from './GitHubApiSchemas'
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
		body: { state: 'open' },
	})
	return pullRequestInfo(input.pullRequest, value)
})
