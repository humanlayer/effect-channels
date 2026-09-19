import { Context, Effect, Schema } from 'effect'

import {
	GitHubCommentRef,
	GitHubContent,
	GitHubIssueCommentRef,
	type GitHubIssueInfo,
	GitHubIssueRef,
	type GitHubPullRequestInfo,
	GitHubPullRequestRef,
	GitHubReaction,
	GitHubReviewCommentRef,
} from './GitHubModels'
import type {
	GitHubComment,
	GitHubIssueComment,
	GitHubIssueComments,
	GitHubReviewComment,
	GitHubReviewComments,
	GitHubReviews,
} from './GitHubResources'

export const GitHubIssueRequest = Schema.Struct({ issue: GitHubIssueRef })
export interface GitHubIssueRequest extends Schema.Schema.Type<typeof GitHubIssueRequest> {}

export const GitHubPullRequestRequest = Schema.Struct({ pullRequest: GitHubPullRequestRef })
export interface GitHubPullRequestRequest extends Schema.Schema.Type<typeof GitHubPullRequestRequest> {}

export const GitHubPostIssueComment = Schema.Struct({
	issue: GitHubIssueRef,
	content: GitHubContent,
})
export interface GitHubPostIssueComment extends Schema.Schema.Type<typeof GitHubPostIssueComment> {}

export const GitHubPostPullRequestComment = Schema.Struct({
	pullRequest: GitHubPullRequestRef,
	content: GitHubContent,
})
export interface GitHubPostPullRequestComment extends Schema.Schema.Type<typeof GitHubPostPullRequestComment> {}

export const GitHubReplyToReviewComment = Schema.Struct({
	pullRequest: GitHubPullRequestRef,
	comment: GitHubReviewCommentRef,
	content: GitHubContent,
})
export interface GitHubReplyToReviewComment extends Schema.Schema.Type<typeof GitHubReplyToReviewComment> {}

export const GitHubUpdateIssueComment = Schema.Struct({
	comment: GitHubIssueCommentRef,
	content: GitHubContent,
})
export interface GitHubUpdateIssueComment extends Schema.Schema.Type<typeof GitHubUpdateIssueComment> {}

export const GitHubUpdateReviewComment = Schema.Struct({
	comment: GitHubReviewCommentRef,
	content: GitHubContent,
})
export interface GitHubUpdateReviewComment extends Schema.Schema.Type<typeof GitHubUpdateReviewComment> {}

export const GitHubUpdateComment = Schema.Union([GitHubUpdateIssueComment, GitHubUpdateReviewComment])
export type GitHubUpdateComment = typeof GitHubUpdateComment.Type

export const GitHubDeleteComment = Schema.Struct({ comment: GitHubCommentRef })
export interface GitHubDeleteComment extends Schema.Schema.Type<typeof GitHubDeleteComment> {}

export const GitHubReactionRequest = Schema.Struct({
	comment: GitHubCommentRef,
	reaction: GitHubReaction,
})
export interface GitHubReactionRequest extends Schema.Schema.Type<typeof GitHubReactionRequest> {}

export const GitHubApiOperation = Schema.Literals([
	'fetch_issue',
	'fetch_pull_request',
	'list_issue_comments',
	'list_pull_request_comments',
	'list_pull_request_reviews',
	'list_pull_request_review_comments',
	'post_issue_comment',
	'post_pull_request_comment',
	'reply_to_review_comment',
	'update_comment',
	'delete_comment',
	'add_reaction',
	'remove_reaction',
])
export type GitHubApiOperation = typeof GitHubApiOperation.Type

export const GitHubApiErrorReason = Schema.Literals([
	'authentication',
	'forbidden',
	'not_found',
	'rate_limited',
	'unavailable',
	'invalid_response',
])
export type GitHubApiErrorReason = typeof GitHubApiErrorReason.Type

export class GitHubApiError extends Schema.TaggedError<GitHubApiError>()('GitHubApiError', {
	operation: GitHubApiOperation,
	reason: GitHubApiErrorReason,
	retryable: Schema.Boolean,
	retryAfterMs: Schema.optionalKey(
		Schema.Int.check(Schema.isGreaterThanOrEqualTo(0), Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER)),
	),
}) {}

/** Application-wide GitHub operations. Transport, credentials, pagination, and decoding stay behind this service. */
export class GitHubApi extends Context.Service<
	GitHubApi,
	{
		readonly fetchIssue: (input: GitHubIssueRequest) => Effect.Effect<GitHubIssueInfo, GitHubApiError>
		readonly fetchPullRequest: (
			input: GitHubPullRequestRequest,
		) => Effect.Effect<GitHubPullRequestInfo, GitHubApiError>
		readonly listIssueComments: (input: GitHubIssueRequest) => Effect.Effect<GitHubIssueComments, GitHubApiError>
		readonly listPullRequestComments: (
			input: GitHubPullRequestRequest,
		) => Effect.Effect<GitHubIssueComments, GitHubApiError>
		readonly listPullRequestReviews: (
			input: GitHubPullRequestRequest,
		) => Effect.Effect<GitHubReviews, GitHubApiError>
		readonly listPullRequestReviewComments: (
			input: GitHubPullRequestRequest,
		) => Effect.Effect<GitHubReviewComments, GitHubApiError>
		readonly postIssueComment: (input: GitHubPostIssueComment) => Effect.Effect<GitHubIssueComment, GitHubApiError>
		readonly postPullRequestComment: (
			input: GitHubPostPullRequestComment,
		) => Effect.Effect<GitHubIssueComment, GitHubApiError>
		readonly replyToReviewComment: (
			input: GitHubReplyToReviewComment,
		) => Effect.Effect<GitHubReviewComment, GitHubApiError>
		readonly updateComment: (input: GitHubUpdateComment) => Effect.Effect<GitHubComment, GitHubApiError>
		readonly deleteComment: (input: GitHubDeleteComment) => Effect.Effect<void, GitHubApiError>
		readonly addReaction: (input: GitHubReactionRequest) => Effect.Effect<void, GitHubApiError>
		readonly removeReaction: (input: GitHubReactionRequest) => Effect.Effect<void, GitHubApiError>
	}
>()('@humanlayer/channels-github-next/GitHubApi') {}
