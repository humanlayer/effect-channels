import { Effect } from 'effect'

import { GitHubApiError, type GitHubPullRequestRequest } from '../GitHubApi'
import { GitHubApiClient } from './GitHubApiClient'
import { pullRequestInfo, repositoryPath } from './GitHubApiProjections'
import { PullRequest } from './GitHubApiSchemas'
export const fetchPullRequest = Effect.fn('github.api.fetch_pull_request')(function* (input: GitHubPullRequestRequest) {
	const api = yield* GitHubApiClient
	const value = yield* api.call({
		operation: 'fetch_pull_request',
		ref: input.pullRequest,
		method: 'GET',
		path: `${repositoryPath(input.pullRequest)}/pulls/${input.pullRequest.number}`,
		schema: PullRequest,
	})
	if (value.number !== input.pullRequest.number)
		return yield* GitHubApiError.make({
			operation: 'fetch_pull_request',
			reason: 'invalid_response',
			retryable: false,
		})
	return pullRequestInfo(input.pullRequest, value)
})
