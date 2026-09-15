import { Schema } from 'effect'

import { GitHubId } from './GitHubIdentity'

export const GitHubUser = Schema.Struct({
	id: GitHubId,
	login: Schema.NonEmptyString,
	type: Schema.String,
})

export const GitHubIssue = Schema.Struct({
	id: GitHubId,
	number: GitHubId,
	title: Schema.String,
	body: Schema.NullOr(Schema.String),
	state: Schema.Literals(['open', 'closed']),
	html_url: Schema.String,
	user: GitHubUser,
	pull_request: Schema.optionalKey(Schema.Struct({ url: Schema.String })),
})

export const GitHubIssueComment = Schema.Struct({
	id: GitHubId,
	body: Schema.String,
	html_url: Schema.String,
	user: GitHubUser,
})

export const GitHubRepository = Schema.Struct({
	id: GitHubId,
	name: Schema.NonEmptyString,
	owner: Schema.Struct({ login: Schema.NonEmptyString }),
})

export const GitHubInstallation = Schema.Struct({ id: GitHubId })

export const GitHubPullRequest = Schema.Struct({
	...GitHubIssue.fields,
	user: Schema.NullOr(
		Schema.Struct({
			id: GitHubId,
			login: Schema.NonEmptyString,
			type: Schema.optionalKey(Schema.String),
		}),
	),
	merged: Schema.optionalKey(Schema.NullOr(Schema.Boolean)),
	draft: Schema.optionalKey(Schema.Boolean),
	head: Schema.optionalKey(Schema.Struct({ ref: Schema.String, sha: Schema.NonEmptyString })),
	base: Schema.optionalKey(Schema.Struct({ ref: Schema.String, sha: Schema.NonEmptyString })),
})

export const GitHubReview = Schema.Struct({
	id: GitHubId,
	node_id: Schema.NonEmptyString,
	body: Schema.NullOr(Schema.String),
	user: Schema.NullOr(GitHubUser),
	state: Schema.Literals([
		'approved',
		'changes_requested',
		'commented',
		'dismissed',
		'pending',
		// @emulators/github currently emits its internal enum casing in webhook payloads.
		'APPROVED',
		'CHANGES_REQUESTED',
		'COMMENTED',
		'DISMISSED',
		'PENDING',
	]),
	commit_id: Schema.String,
	html_url: Schema.String,
})

export const GitHubReviewComment = Schema.Struct({
	id: GitHubId,
	node_id: Schema.NonEmptyString,
	body: Schema.String,
	html_url: Schema.String,
	user: Schema.NullOr(GitHubUser),
	pull_request_review_id: Schema.NullOr(GitHubId),
	path: Schema.String,
	commit_id: Schema.String,
	original_commit_id: Schema.String,
	diff_hunk: Schema.String,
	pull_request_url: Schema.String,
	in_reply_to_id: Schema.optionalKey(Schema.NullOr(GitHubId)),
	line: Schema.optionalKey(Schema.NullOr(Schema.Number)),
	start_line: Schema.optionalKey(Schema.NullOr(Schema.Number)),
	side: Schema.optionalKey(Schema.Literals(['LEFT', 'RIGHT'])),
})
