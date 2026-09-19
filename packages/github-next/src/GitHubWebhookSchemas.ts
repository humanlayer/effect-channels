import { Schema } from 'effect'

import { GitHubId } from './GitHubIdentity'
import {
	GitHubInstallation,
	GitHubIssue,
	GitHubIssueComment,
	GitHubLabel,
	GitHubPullRequest,
	GitHubRepository,
	GitHubReview,
	GitHubReviewComment,
	GitHubTeam,
	GitHubUser,
} from './GitHubWebhookEventSchemas'

export const GitHubWebhookHeaders = Schema.Struct({
	'x-github-delivery': Schema.NonEmptyString,
	'x-github-event': Schema.NonEmptyString,
	'x-hub-signature-256': Schema.NonEmptyString,
})

export const GitHubWebhookEnvelope = Schema.Struct({ action: Schema.NonEmptyString })

const pullRequestEnvelope = {
	installation: GitHubInstallation,
	repository: GitHubRepository,
	pull_request: GitHubPullRequest,
}

export const GitHubIssuesWebhook = Schema.Struct({
	action: Schema.Literals([
		'opened',
		'edited',
		'closed',
		'reopened',
		'assigned',
		'unassigned',
		'labeled',
		'unlabeled',
	]),
	installation: GitHubInstallation,
	repository: GitHubRepository,
	issue: GitHubIssue,
	sender: GitHubUser,
	assignee: Schema.optionalKey(Schema.NullOr(GitHubUser)),
	label: Schema.optionalKey(Schema.NullOr(GitHubLabel)),
})
export type GitHubIssuesWebhook = typeof GitHubIssuesWebhook.Type

export const GitHubIssueCommentWebhook = Schema.Struct({
	action: Schema.Literals(['created', 'edited', 'deleted']),
	installation: GitHubInstallation,
	repository: GitHubRepository,
	issue: GitHubIssue,
	comment: GitHubIssueComment,
	sender: GitHubUser,
})
export type GitHubIssueCommentWebhook = typeof GitHubIssueCommentWebhook.Type

export const GitHubPullRequestWebhook = Schema.Struct({
	action: Schema.Literals([
		'opened',
		'edited',
		'closed',
		'reopened',
		'synchronize',
		'review_requested',
		'review_request_removed',
		'assigned',
		'unassigned',
		'labeled',
		'unlabeled',
		'converted_to_draft',
		'ready_for_review',
	]),
	...pullRequestEnvelope,
	sender: GitHubUser,
	assignee: Schema.optionalKey(Schema.NullOr(GitHubUser)),
	label: Schema.optionalKey(Schema.NullOr(GitHubLabel)),
	requested_reviewer: Schema.optionalKey(Schema.NullOr(GitHubUser)),
	requested_team: Schema.optionalKey(Schema.NullOr(GitHubTeam)),
})
export type GitHubPullRequestWebhook = typeof GitHubPullRequestWebhook.Type

export const GitHubPullRequestReviewWebhook = Schema.Struct({
	action: Schema.Literals(['submitted', 'edited', 'dismissed']),
	...pullRequestEnvelope,
	review: GitHubReview,
	sender: GitHubUser,
})
export type GitHubPullRequestReviewWebhook = typeof GitHubPullRequestReviewWebhook.Type

export const GitHubPullRequestReviewCommentWebhook = Schema.Struct({
	action: Schema.Literals(['created', 'edited', 'deleted']),
	...pullRequestEnvelope,
	comment: GitHubReviewComment,
	sender: GitHubUser,
})
export type GitHubPullRequestReviewCommentWebhook = typeof GitHubPullRequestReviewCommentWebhook.Type

export const GitHubPullRequestReviewThreadWebhook = Schema.Struct({
	action: Schema.Literals(['resolved', 'unresolved']),
	...pullRequestEnvelope,
	thread: Schema.Struct({ node_id: Schema.NonEmptyString, comments: Schema.Array(GitHubReviewComment) }),
	sender: Schema.optionalKey(GitHubUser),
})
export type GitHubPullRequestReviewThreadWebhook = typeof GitHubPullRequestReviewThreadWebhook.Type

export const GitHubCheckRunWebhook = Schema.Struct({
	action: Schema.Literal('completed'),
	installation: GitHubInstallation,
	repository: GitHubRepository,
	sender: GitHubUser,
	check_run: Schema.Struct({
		id: GitHubId,
		name: Schema.String,
		status: Schema.Literal('completed'),
		conclusion: Schema.Literals([
			'success',
			'failure',
			'timed_out',
			'cancelled',
			'action_required',
			'neutral',
			'skipped',
			'stale',
		]),
		details_url: Schema.NullOr(Schema.String),
		head_sha: Schema.NonEmptyString,
		check_suite: Schema.NullOr(Schema.Struct({ id: GitHubId })),
		started_at: Schema.NullOr(Schema.String),
		completed_at: Schema.NullOr(Schema.String),
		pull_requests: Schema.Array(Schema.Struct({ number: GitHubId })),
	}),
})
export type GitHubCheckRunWebhook = typeof GitHubCheckRunWebhook.Type

export const GitHubSupportedWebhook = Schema.Union([
	Schema.Struct({ event: Schema.Literal('issues'), payload: GitHubIssuesWebhook }),
	Schema.Struct({ event: Schema.Literal('issue_comment'), payload: GitHubIssueCommentWebhook }),
	Schema.Struct({ event: Schema.Literal('pull_request'), payload: GitHubPullRequestWebhook }),
	Schema.Struct({ event: Schema.Literal('pull_request_review'), payload: GitHubPullRequestReviewWebhook }),
	Schema.Struct({
		event: Schema.Literal('pull_request_review_comment'),
		payload: GitHubPullRequestReviewCommentWebhook,
	}),
	Schema.Struct({
		event: Schema.Literal('pull_request_review_thread'),
		payload: GitHubPullRequestReviewThreadWebhook,
	}),
	Schema.Struct({ event: Schema.Literal('check_run'), payload: GitHubCheckRunWebhook }),
])
export type GitHubSupportedWebhook = typeof GitHubSupportedWebhook.Type
