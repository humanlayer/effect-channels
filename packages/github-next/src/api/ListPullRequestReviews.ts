import { Effect } from 'effect'

import type { GitHubPullRequestRequest } from '../GitHubApi'
import { GitHubApiClient } from './GitHubApiClient'
import { repositoryPath, review } from './GitHubApiProjections'
import { Review } from './GitHubApiSchemas'
export const listPullRequestReviews = Effect.fn('github.api.list_pull_request_reviews')(function* (
	input: GitHubPullRequestRequest,
) {
	const api = yield* GitHubApiClient
	const values = yield* api.list({
		operation: 'list_pull_request_reviews',
		ref: input.pullRequest,
		path: `${repositoryPath(input.pullRequest)}/pulls/${input.pullRequest.number}/reviews`,
		schema: Review,
	})
	return values.map((value) => review(input.pullRequest, value))
})
