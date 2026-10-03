import { Effect, Schema } from 'effect'

import type { GitHubPostPullRequestReviewComment } from '../GitHubApi'
import { GitHubDiffSide } from '../GitHubModels'
import { GitHubApiClient } from './GitHubApiClient'
import { repositoryPath, reviewComment, reviewCommentLocationBody } from './GitHubApiProjections'
import { ReviewComment } from './GitHubApiSchemas'

const ReviewCommentTarget = {
	body: Schema.String,
	commit_id: Schema.NonEmptyString,
	path: Schema.NonEmptyString,
}

/** Members run from most to least specific, because a union encodes with the first member that accepts the value. */
export const PostPullRequestReviewCommentBody = Schema.Union([
	Schema.Struct({
		...ReviewCommentTarget,
		start_line: Schema.Int,
		start_side: GitHubDiffSide,
		line: Schema.Int,
		side: GitHubDiffSide,
	}),
	Schema.Struct({ ...ReviewCommentTarget, line: Schema.Int, side: GitHubDiffSide }),
	Schema.Struct({ ...ReviewCommentTarget, subject_type: Schema.Literal('file') }),
])

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
			schema: PostPullRequestReviewCommentBody,
			value: {
				body: input.content.markdown,
				commit_id: input.commitId,
				path: input.path,
				...reviewCommentLocationBody(input.location),
			},
		},
	})
	return reviewComment(input.pullRequest, value)
})
