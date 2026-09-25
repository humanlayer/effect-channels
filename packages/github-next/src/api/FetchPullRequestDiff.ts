import { Effect } from 'effect'

import type { GitHubPullRequestRequest } from '../GitHubApi'
import { GitHubApiClient } from './GitHubApiClient'
import { repositoryPath } from './GitHubApiProjections'
export const fetchPullRequestDiff = Effect.fn('github.api.fetch_pull_request_diff')(function* (
	input: GitHubPullRequestRequest,
) {
	const api = yield* GitHubApiClient
	return yield* api.text({
		operation: 'fetch_pull_request_diff',
		ref: input.pullRequest,
		method: 'GET',
		path: `${repositoryPath(input.pullRequest)}/pulls/${input.pullRequest.number}`,
		accept: 'application/vnd.github.diff',
	})
})
