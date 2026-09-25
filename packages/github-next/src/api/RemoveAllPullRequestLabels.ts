import { Effect } from 'effect'

import type { GitHubPullRequestRequest } from '../GitHubApi'
import { GitHubApiClient } from './GitHubApiClient'
import { repositoryPath } from './GitHubApiProjections'
export const removeAllPullRequestLabels = Effect.fn('github.api.remove_all_pull_request_labels')(function* (
	input: GitHubPullRequestRequest,
) {
	const api = yield* GitHubApiClient
	yield* api.callVoid({
		operation: 'remove_all_pull_request_labels',
		ref: input.pullRequest,
		method: 'DELETE',
		path: `${repositoryPath(input.pullRequest)}/issues/${input.pullRequest.number}/labels`,
	})
})
