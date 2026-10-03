import { Effect, Schema } from 'effect'

import type { GitHubReplyToReviewComment } from '../GitHubApi'
import { GitHubApiClient } from './GitHubApiClient'
import { repositoryPath, reviewComment } from './GitHubApiProjections'
import { ReviewComment } from './GitHubApiSchemas'

const ReplyToReviewCommentBody = Schema.Struct({ body: Schema.String })

export const replyToReviewComment = Effect.fn('github.api.reply_to_review_comment')(function* (
	input: GitHubReplyToReviewComment,
) {
	const api = yield* GitHubApiClient
	const value = yield* api.call({
		operation: 'reply_to_review_comment',
		ref: input.pullRequest,
		method: 'POST',
		path: `${repositoryPath(input.pullRequest)}/pulls/${input.pullRequest.number}/comments/${input.comment.id}/replies`,
		schema: ReviewComment,
		body: { schema: ReplyToReviewCommentBody, value: { body: input.content.markdown } },
	})
	return reviewComment(input.pullRequest, value)
})
