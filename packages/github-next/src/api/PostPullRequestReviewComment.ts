import { Effect } from 'effect'

import type { GitHubPostPullRequestReviewComment } from '../GitHubApi'
import { GitHubApiClient } from './GitHubApiClient'
import { repositoryPath, reviewComment, reviewCommentLocationBody } from './GitHubApiProjections'
import { ReviewComment } from './GitHubApiSchemas'
export const postPullRequestReviewComment = Effect.fn('github.api.post_pull_request_review_comment')(function* (
	input: GitHubPostPullRequestReviewComment,
) {
	const api = yield* GitHubApiClient
	const value = yield* api.call({
		operation: 'post_pull_request_review_comment',
		ref: input.pullRequest,
		method: 'POST',
		path: `${repositoryPath(input.pullRequest)}/pulls/${input.pullRequest.number}/comments`,
		schema: ReviewComment,
		body: {
			body: input.content.markdown,
			commit_id: input.commitId,
			path: input.path,
			...reviewCommentLocationBody(input.location),
		},
	})
	return reviewComment(input.pullRequest, value)
})
