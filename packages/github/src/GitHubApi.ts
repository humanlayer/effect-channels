import { Context, Effect, Schema } from 'effect'

import {
	type GitHubAccessLevel,
	GitHubActionsJobRef,
	type GitHubActionsJobInfo,
	GitHubCheckRunRef,
	type GitHubCheckRunInfo,
	GitHubCommentRef,
	GitHubContent,
	GitHubIssueCommentRef,
	type GitHubIssueInfo,
	GitHubIssueRef,
	GitHubMergeMethod,
	type GitHubMergeResult,
	type GitHubPullRequestInfo,
	GitHubPullRequestRef,
	GitHubReaction,
	GitHubReactionTarget,
	GitHubReviewCommentLocation,
	GitHubRepositoryRef,
	GitHubReviewCommentRef,
} from './GitHubModels'
import type {
	GitHubActionsJob,
	GitHubCheckAnnotations,
	GitHubCheckRuns,
	GitHubComment,
	GitHubCommits,
	GitHubIssueComment,
	GitHubIssueComments,
	GitHubLabelsResult,
	GitHubPullRequestFiles,
	GitHubReviewComment,
	GitHubReviewComments,
	GitHubReviews,
} from './GitHubResources'

export const GitHubIssueRequest = Schema.Struct({ issue: GitHubIssueRef })
export interface GitHubIssueRequest extends Schema.Schema.Type<typeof GitHubIssueRequest> {}

export const GitHubRepositoryRequest = Schema.Struct({ repository: GitHubRepositoryRef })
export interface GitHubRepositoryRequest extends Schema.Schema.Type<typeof GitHubRepositoryRequest> {}

export const GitHubPullRequestRequest = Schema.Struct({ pullRequest: GitHubPullRequestRef })
export interface GitHubPullRequestRequest extends Schema.Schema.Type<typeof GitHubPullRequestRequest> {}

export const GitHubCheckRunRequest = Schema.Struct({ checkRun: GitHubCheckRunRef })
export interface GitHubCheckRunRequest extends Schema.Schema.Type<typeof GitHubCheckRunRequest> {}

export const GitHubActionsJobRequest = Schema.Struct({ job: GitHubActionsJobRef })
export interface GitHubActionsJobRequest extends Schema.Schema.Type<typeof GitHubActionsJobRequest> {}

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

export const GitHubPostReviewCommentOptions = Schema.Struct({
	content: GitHubContent,
	commitId: Schema.NonEmptyString,
	path: Schema.NonEmptyString,
	location: GitHubReviewCommentLocation,
})
export interface GitHubPostReviewCommentOptions extends Schema.Schema.Type<typeof GitHubPostReviewCommentOptions> {}

export const GitHubPostPullRequestReviewComment = Schema.Struct({
	pullRequest: GitHubPullRequestRef,
	...GitHubPostReviewCommentOptions.fields,
})
export interface GitHubPostPullRequestReviewComment extends Schema.Schema.Type<
	typeof GitHubPostPullRequestReviewComment
> {}

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

/** The bot's reaction on a comment, or on an issue or pull request itself. */
export const GitHubReactionRequest = Schema.Struct({
	target: GitHubReactionTarget,
	reaction: GitHubReaction,
})
export interface GitHubReactionRequest extends Schema.Schema.Type<typeof GitHubReactionRequest> {}

export const GitHubLabels = Schema.Array(Schema.NonEmptyString)
export type GitHubLabels = typeof GitHubLabels.Type

export const GitHubIssueLabelsRequest = Schema.Struct({
	issue: GitHubIssueRef,
	labels: GitHubLabels,
})
export interface GitHubIssueLabelsRequest extends Schema.Schema.Type<typeof GitHubIssueLabelsRequest> {}

export const GitHubPullRequestLabelsRequest = Schema.Struct({
	pullRequest: GitHubPullRequestRef,
	labels: GitHubLabels,
})
export interface GitHubPullRequestLabelsRequest extends Schema.Schema.Type<typeof GitHubPullRequestLabelsRequest> {}

export const GitHubRemoveIssueLabel = Schema.Struct({
	issue: GitHubIssueRef,
	label: Schema.NonEmptyString,
})
export interface GitHubRemoveIssueLabel extends Schema.Schema.Type<typeof GitHubRemoveIssueLabel> {}

export const GitHubRemovePullRequestLabel = Schema.Struct({
	pullRequest: GitHubPullRequestRef,
	label: Schema.NonEmptyString,
})
export interface GitHubRemovePullRequestLabel extends Schema.Schema.Type<typeof GitHubRemovePullRequestLabel> {}

export const GitHubIssueCloseReason = Schema.Literals(['completed', 'not_planned'])
export type GitHubIssueCloseReason = typeof GitHubIssueCloseReason.Type

export const GitHubCloseIssue = Schema.Struct({
	issue: GitHubIssueRef,
	reason: GitHubIssueCloseReason,
})
export interface GitHubCloseIssue extends Schema.Schema.Type<typeof GitHubCloseIssue> {}

export const GitHubMergeOptions = Schema.Struct({
	method: GitHubMergeMethod,
	expectedHeadSha: Schema.NonEmptyString,
	commitTitle: Schema.optionalKey(Schema.String),
	commitMessage: Schema.optionalKey(Schema.String),
})
export interface GitHubMergeOptions extends Schema.Schema.Type<typeof GitHubMergeOptions> {}

export const GitHubMergePullRequest = Schema.Struct({
	pullRequest: GitHubPullRequestRef,
	...GitHubMergeOptions.fields,
})
export interface GitHubMergePullRequest extends Schema.Schema.Type<typeof GitHubMergePullRequest> {}

/** Open pull requests whose head is `head`, a branch of `repository` itself. */
export const GitHubListPullRequestsForBranch = Schema.Struct({
	repository: GitHubRepositoryRef,
	head: Schema.NonEmptyString,
})
export interface GitHubListPullRequestsForBranch extends Schema.Schema.Type<typeof GitHubListPullRequestsForBranch> {}

/** A pull request from `head` into `base`, both branches of `repository`. */
export const GitHubCreatePullRequest = Schema.Struct({
	repository: GitHubRepositoryRef,
	head: Schema.NonEmptyString,
	base: Schema.NonEmptyString,
	title: Schema.NonEmptyString,
	body: Schema.String,
})
export interface GitHubCreatePullRequest extends Schema.Schema.Type<typeof GitHubCreatePullRequest> {}

export const GitHubListCheckRunsForRef = Schema.Struct({
	pullRequest: GitHubPullRequestRef,
	sha: Schema.NonEmptyString,
})
export interface GitHubListCheckRunsForRef extends Schema.Schema.Type<typeof GitHubListCheckRunsForRef> {}

/** A GitHub user, by login, in a repository. */
export const GitHubUserAccessRequest = Schema.Struct({
	repository: GitHubRepositoryRef,
	login: Schema.NonEmptyString,
})
export interface GitHubUserAccessRequest extends Schema.Schema.Type<typeof GitHubUserAccessRequest> {}

export const GitHubApiOperation = Schema.Literals([
	'fetch_issue',
	'fetch_pull_request',
	'list_issue_comments',
	'list_pull_request_comments',
	'list_pull_request_reviews',
	'list_pull_request_review_comments',
	'list_pull_request_files',
	'fetch_pull_request_diff',
	'list_pull_request_commits',
	'list_issue_labels',
	'list_repository_labels',
	'list_pull_request_labels',
	'add_issue_labels',
	'add_pull_request_labels',
	'set_issue_labels',
	'set_pull_request_labels',
	'remove_issue_label',
	'remove_pull_request_label',
	'remove_all_issue_labels',
	'remove_all_pull_request_labels',
	'post_issue_comment',
	'post_pull_request_comment',
	'post_pull_request_review_comment',
	'reply_to_review_comment',
	'update_comment',
	'delete_comment',
	'add_reaction',
	'remove_reaction',
	'close_issue',
	'reopen_issue',
	'close_pull_request',
	'reopen_pull_request',
	'merge_pull_request',
	'list_pull_requests_for_branch',
	'create_pull_request',
	'list_check_runs_for_ref',
	'fetch_check_run',
	'list_check_run_annotations',
	'resolve_check_run_actions_job',
	'fetch_actions_job',
	'download_actions_job_log',
	'fetch_user_access',
	'create_git_credentials',
])
export type GitHubApiOperation = typeof GitHubApiOperation.Type

export const GitHubApiErrorReason = Schema.Literals([
	'authentication',
	'forbidden',
	'not_found',
	'rate_limited',
	'validation',
	'stale_head',
	'not_mergeable',
	'rules_rejected',
	'unavailable',
	'invalid_response',
])
export type GitHubApiErrorReason = typeof GitHubApiErrorReason.Type

export class GitHubApiError extends Schema.TaggedError<GitHubApiError>()('GitHubApiError', {
	operation: GitHubApiOperation,
	reason: GitHubApiErrorReason,
	retryable: Schema.Boolean,
	status: Schema.optionalKey(Schema.Int),
	message: Schema.optionalKey(Schema.String),
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
		readonly listPullRequestFiles: (
			input: GitHubPullRequestRequest,
		) => Effect.Effect<GitHubPullRequestFiles, GitHubApiError>
		readonly fetchPullRequestDiff: (input: GitHubPullRequestRequest) => Effect.Effect<string, GitHubApiError>
		readonly listPullRequestCommits: (
			input: GitHubPullRequestRequest,
		) => Effect.Effect<GitHubCommits, GitHubApiError>
		readonly listIssueLabels: (input: GitHubIssueRequest) => Effect.Effect<GitHubLabelsResult, GitHubApiError>
		readonly listRepositoryLabels: (
			input: GitHubRepositoryRequest,
		) => Effect.Effect<GitHubLabelsResult, GitHubApiError>
		readonly listPullRequestLabels: (
			input: GitHubPullRequestRequest,
		) => Effect.Effect<GitHubLabelsResult, GitHubApiError>
		readonly addIssueLabels: (input: GitHubIssueLabelsRequest) => Effect.Effect<GitHubLabelsResult, GitHubApiError>
		readonly addPullRequestLabels: (
			input: GitHubPullRequestLabelsRequest,
		) => Effect.Effect<GitHubLabelsResult, GitHubApiError>
		readonly setIssueLabels: (input: GitHubIssueLabelsRequest) => Effect.Effect<GitHubLabelsResult, GitHubApiError>
		readonly setPullRequestLabels: (
			input: GitHubPullRequestLabelsRequest,
		) => Effect.Effect<GitHubLabelsResult, GitHubApiError>
		readonly removeIssueLabel: (input: GitHubRemoveIssueLabel) => Effect.Effect<GitHubLabelsResult, GitHubApiError>
		readonly removePullRequestLabel: (
			input: GitHubRemovePullRequestLabel,
		) => Effect.Effect<GitHubLabelsResult, GitHubApiError>
		readonly removeAllIssueLabels: (input: GitHubIssueRequest) => Effect.Effect<void, GitHubApiError>
		readonly removeAllPullRequestLabels: (input: GitHubPullRequestRequest) => Effect.Effect<void, GitHubApiError>
		readonly postIssueComment: (input: GitHubPostIssueComment) => Effect.Effect<GitHubIssueComment, GitHubApiError>
		readonly postPullRequestComment: (
			input: GitHubPostPullRequestComment,
		) => Effect.Effect<GitHubIssueComment, GitHubApiError>
		readonly postPullRequestReviewComment: (
			input: GitHubPostPullRequestReviewComment,
		) => Effect.Effect<GitHubReviewComment, GitHubApiError>
		readonly replyToReviewComment: (
			input: GitHubReplyToReviewComment,
		) => Effect.Effect<GitHubReviewComment, GitHubApiError>
		readonly updateComment: (input: GitHubUpdateComment) => Effect.Effect<GitHubComment, GitHubApiError>
		readonly deleteComment: (input: GitHubDeleteComment) => Effect.Effect<void, GitHubApiError>
		/** Add the bot's reaction to a comment, issue, or pull request. Already present counts as added. */
		readonly addReaction: (input: GitHubReactionRequest) => Effect.Effect<void, GitHubApiError>
		/** Remove the bot's own reaction from a comment, issue, or pull request. Already absent counts as removed. */
		readonly removeReaction: (input: GitHubReactionRequest) => Effect.Effect<void, GitHubApiError>
		readonly closeIssue: (input: GitHubCloseIssue) => Effect.Effect<GitHubIssueInfo, GitHubApiError>
		readonly reopenIssue: (input: GitHubIssueRequest) => Effect.Effect<GitHubIssueInfo, GitHubApiError>
		readonly closePullRequest: (
			input: GitHubPullRequestRequest,
		) => Effect.Effect<GitHubPullRequestInfo, GitHubApiError>
		readonly reopenPullRequest: (
			input: GitHubPullRequestRequest,
		) => Effect.Effect<GitHubPullRequestInfo, GitHubApiError>
		readonly mergePullRequest: (input: GitHubMergePullRequest) => Effect.Effect<GitHubMergeResult, GitHubApiError>
		/** The open pull requests from a branch of the repository itself, not from forks. */
		readonly listPullRequestsForBranch: (
			input: GitHubListPullRequestsForBranch,
		) => Effect.Effect<ReadonlyArray<GitHubPullRequestInfo>, GitHubApiError>
		readonly createPullRequest: (
			input: GitHubCreatePullRequest,
		) => Effect.Effect<GitHubPullRequestInfo, GitHubApiError>
		readonly listCheckRunsForRef: (
			input: GitHubListCheckRunsForRef,
		) => Effect.Effect<GitHubCheckRuns, GitHubApiError>
		readonly fetchCheckRun: (input: GitHubCheckRunRequest) => Effect.Effect<GitHubCheckRunInfo, GitHubApiError>
		readonly listCheckRunAnnotations: (
			input: GitHubCheckRunRequest,
		) => Effect.Effect<GitHubCheckAnnotations, GitHubApiError>
		readonly resolveActionsJob: (
			input: GitHubCheckRunRequest,
		) => Effect.Effect<GitHubActionsJob | null, GitHubApiError>
		readonly fetchActionsJob: (
			input: GitHubActionsJobRequest,
		) => Effect.Effect<GitHubActionsJobInfo, GitHubApiError>
		readonly downloadActionsJobLog: (input: GitHubActionsJobRequest) => Effect.Effect<string, GitHubApiError>
		/** A user's access to a repository. An account that is not a GitHub user fails with `not_found`. */
		readonly fetchUserAccess: (input: GitHubUserAccessRequest) => Effect.Effect<GitHubAccessLevel, GitHubApiError>
	}
>()('@humanlayer/channels-github/GitHubApi') {}
