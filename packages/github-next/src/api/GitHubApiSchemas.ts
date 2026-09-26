import { Schema } from 'effect'

import { GitHubId } from '../GitHubIdentity'
import { GitHubCheckConclusion, GitHubCheckStatus, GitHubDiffLine } from '../GitHubModels'

export const Participant = Schema.Struct({ id: GitHubId, login: Schema.NonEmptyString, type: Schema.String })
export const Issue = Schema.Struct({
	number: GitHubId,
	title: Schema.String,
	body: Schema.NullOr(Schema.String),
	state: Schema.Literals(['open', 'closed']),
	html_url: Schema.String,
	user: Participant,
})
export const PullRequest = Schema.Struct({
	number: GitHubId,
	title: Schema.String,
	body: Schema.NullOr(Schema.String),
	state: Schema.Literals(['open', 'closed']),
	html_url: Schema.String,
	user: Schema.NullOr(Participant),
	draft: Schema.Boolean,
	merged: Schema.Boolean,
	head: Schema.Struct({ ref: Schema.String, sha: Schema.NonEmptyString }),
	base: Schema.Struct({ ref: Schema.String, sha: Schema.NonEmptyString }),
})
export const IssueComment = Schema.Struct({
	id: GitHubId,
	body: Schema.String,
	html_url: Schema.String,
	user: Schema.NullOr(Participant),
})
export const Review = Schema.Struct({
	id: GitHubId,
	node_id: Schema.NonEmptyString,
	body: Schema.NullOr(Schema.String),
	user: Schema.NullOr(Participant),
	state: Schema.String,
	commit_id: Schema.String,
	html_url: Schema.String,
})
export const ReviewComment = Schema.Struct({
	id: GitHubId,
	node_id: Schema.NonEmptyString,
	body: Schema.String,
	html_url: Schema.String,
	user: Schema.NullOr(Participant),
	pull_request_review_id: Schema.NullOr(GitHubId),
	path: Schema.String,
	commit_id: Schema.String,
	original_commit_id: Schema.String,
	diff_hunk: Schema.String,
	in_reply_to_id: Schema.optionalKey(Schema.NullOr(GitHubId)),
	line: Schema.optionalKey(Schema.NullOr(GitHubDiffLine)),
	start_line: Schema.optionalKey(Schema.NullOr(GitHubDiffLine)),
	side: Schema.optionalKey(Schema.Literals(['LEFT', 'RIGHT'])),
})
export const Reaction = Schema.Struct({ id: GitHubId, content: Schema.String, user: Schema.NullOr(Participant) })

const NonNegativeInt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))
const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0))
export const CommitParticipant = Schema.NullOr(Schema.Union([Participant, Schema.Record(Schema.String, Schema.Never)]))
export const PullRequestFile = Schema.Struct({
	sha: Schema.NullOr(Schema.NonEmptyString),
	filename: Schema.NonEmptyString,
	previous_filename: Schema.optionalKey(Schema.NonEmptyString),
	status: Schema.Literals(['added', 'removed', 'modified', 'renamed', 'copied', 'changed', 'unchanged']),
	additions: NonNegativeInt,
	deletions: NonNegativeInt,
	changes: NonNegativeInt,
	blob_url: Schema.NullOr(Schema.String),
	raw_url: Schema.NullOr(Schema.String),
	contents_url: Schema.String,
	patch: Schema.optionalKey(Schema.String),
})
export const Commit = Schema.Struct({
	sha: Schema.NonEmptyString,
	commit: Schema.Struct({ message: Schema.String }),
	url: Schema.String,
	html_url: Schema.String,
	author: CommitParticipant,
	committer: CommitParticipant,
})
export const Label = Schema.Struct({
	id: Schema.optionalKey(GitHubId),
	name: Schema.NonEmptyString,
	color: Schema.String,
	description: Schema.NullOr(Schema.String),
})
export const MergeResult = Schema.Struct({ sha: Schema.String, merged: Schema.Boolean, message: Schema.String })
export const CheckRun = Schema.Struct({
	id: GitHubId,
	name: Schema.String,
	head_sha: Schema.NonEmptyString,
	status: GitHubCheckStatus,
	conclusion: Schema.NullOr(GitHubCheckConclusion),
	started_at: Schema.NullOr(Schema.String),
	completed_at: Schema.NullOr(Schema.String),
	url: Schema.String,
	html_url: Schema.NullOr(Schema.String),
	details_url: Schema.NullOr(Schema.String),
	check_suite: Schema.NullOr(Schema.Struct({ id: GitHubId })),
	output: Schema.Struct({
		title: Schema.NullOr(Schema.String),
		summary: Schema.NullOr(Schema.String),
		text: Schema.NullOr(Schema.String),
		annotations_count: NonNegativeInt,
	}),
})
export const CheckRunsPage = Schema.Struct({ check_runs: Schema.Array(CheckRun) })
export const CheckAnnotation = Schema.Struct({
	path: Schema.NonEmptyString,
	start_line: PositiveInt,
	end_line: PositiveInt,
	start_column: Schema.NullOr(PositiveInt),
	end_column: Schema.NullOr(PositiveInt),
	annotation_level: Schema.NullOr(Schema.Literals(['notice', 'warning', 'failure'])),
	title: Schema.NullOr(Schema.String),
	message: Schema.NullOr(Schema.String),
	raw_details: Schema.NullOr(Schema.String),
	blob_href: Schema.String,
})
export const ActionsJob = Schema.Struct({
	id: GitHubId,
	run_id: GitHubId,
	name: Schema.String,
	status: GitHubCheckStatus,
	conclusion: Schema.NullOr(GitHubCheckConclusion),
	head_sha: Schema.NonEmptyString,
	url: Schema.String,
	html_url: Schema.NullOr(Schema.String),
	started_at: Schema.NullOr(Schema.String),
	completed_at: Schema.NullOr(Schema.String),
	check_run_url: Schema.String,
	workflow_name: Schema.optionalKey(Schema.NullOr(Schema.String)),
	head_branch: Schema.optionalKey(Schema.NullOr(Schema.String)),
	steps: Schema.optionalKey(
		Schema.Array(
			Schema.Struct({
				name: Schema.String,
				status: GitHubCheckStatus,
				conclusion: Schema.NullOr(GitHubCheckConclusion),
				number: PositiveInt,
				started_at: Schema.optionalKey(Schema.NullOr(Schema.String)),
				completed_at: Schema.optionalKey(Schema.NullOr(Schema.String)),
			}),
		),
	),
})
export const ActionsJobsPage = Schema.Struct({ jobs: Schema.Array(ActionsJob) })
export const WorkflowRunsPage = Schema.Struct({ workflow_runs: Schema.Array(Schema.Struct({ id: GitHubId })) })
