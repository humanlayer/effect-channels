import { Effect } from 'effect'

import type { GitHubPullRequestRequest } from '../GitHubApi'
import { GitHubApiClient } from './GitHubApiClient'
import { repositoryPath, reviewComment } from './GitHubApiProjections'
import { ReviewComment } from './GitHubApiSchemas'
export const listPullRequestReviewComments = Effect.fn('github.api.list_pull_request_review_comments')(function* (
	input: GitHubPullRequestRequest,
) {
	const api = yield* GitHubApiClient
	const values = yield* api.list({
		operation: 'list_pull_request_review_comments',
		ref: input.pullRequest,
		path: `${repositoryPath(input.pullRequest)}/pulls/${input.pullRequest.number}/comments`,
		schema: ReviewComment,
	})
	return values.map((value) => reviewComment(input.pullRequest, value))
})
