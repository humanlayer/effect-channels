import { Effect, Schema } from 'effect'

import type { GitHubUpdateComment } from '../GitHubApi'
import { GitHubReviewCommentRef } from '../GitHubModels'
import { GitHubApiClient } from './GitHubApiClient'
import { commentPath, commentRepository, issueComment, reviewComment } from './GitHubApiProjections'
import { IssueComment, ReviewComment } from './GitHubApiSchemas'
export const updateComment = Effect.fn('github.api.update_comment')(function* (input: GitHubUpdateComment) {
	const api = yield* GitHubApiClient
	const ref = input.comment
	const repository = commentRepository(ref)
	if (Schema.is(GitHubReviewCommentRef)(ref)) {
		const value = yield* api.call({
			operation: 'update_comment',
			ref: repository,
			method: 'PATCH',
			path: commentPath(ref),
			schema: ReviewComment,
			body: { body: input.content.markdown },
		})
		return reviewComment(ref.pullRequest, value)
	}
	const value = yield* api.call({
		operation: 'update_comment',
		ref: repository,
		method: 'PATCH',
		path: commentPath(ref),
		schema: IssueComment,
		body: { body: input.content.markdown },
	})
	return issueComment(ref.discussion, value)
})
