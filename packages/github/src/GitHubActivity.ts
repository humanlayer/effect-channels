import type { EventDefinition } from '@humanlayer/channels-delivery'
import { Match, Schema } from 'effect'

import { GitHubCommentData, GitHubIssueData, GitHubUser } from './GitHubEvents.js'
import {
	GitHubDiscussionRef,
	GitHubId,
	GitHubIssueRef,
	GitHubPullRequestRef,
	issueResourceKey,
} from './GitHubResource.js'

const nativeAuthor = Schema.Struct({
	id: GitHubId,
	login: Schema.NonEmptyString,
	type: Schema.optionalKey(Schema.String),
})

export const GitHubPullRequestData = Schema.Struct({
	...GitHubIssueData.fields,
	user: Schema.NullOr(nativeAuthor),
	merged: Schema.optionalKey(Schema.NullOr(Schema.Boolean)),
	draft: Schema.optionalKey(Schema.Boolean),
	head: Schema.optionalKey(Schema.Struct({ ref: Schema.String, sha: Schema.NonEmptyString })),
	base: Schema.optionalKey(Schema.Struct({ ref: Schema.String, sha: Schema.NonEmptyString })),
})
export interface GitHubPullRequestData extends Schema.Schema.Type<typeof GitHubPullRequestData> {}
export const reviewRequestFields = {
	requested_reviewer: Schema.optionalKey(Schema.NullOr(nativeAuthor)),
	requested_team: Schema.optionalKey(
		Schema.NullOr(Schema.Struct({ id: GitHubId, name: Schema.String, slug: Schema.optionalKey(Schema.String) })),
	),
}
export const GitHubReviewData = Schema.Struct({
	id: GitHubId,
	node_id: Schema.NonEmptyString,
	body: Schema.NullOr(Schema.String),
	user: Schema.NullOr(nativeAuthor),
	state: Schema.Literals(['approved', 'changes_requested', 'commented', 'dismissed', 'pending']),
	commit_id: Schema.String,
	html_url: Schema.String,
})
export interface GitHubReviewData extends Schema.Schema.Type<typeof GitHubReviewData> {}
export const GitHubReviewCommentData = Schema.Struct({
	...GitHubCommentData.fields,
	user: Schema.NullOr(nativeAuthor),
	node_id: Schema.NonEmptyString,
	pull_request_review_id: Schema.NullOr(GitHubId),
	in_reply_to_id: Schema.optionalKey(GitHubId),
	path: Schema.String,
	commit_id: Schema.String,
	original_commit_id: Schema.String,
	diff_hunk: Schema.String,
	line: Schema.optionalKey(Schema.NullOr(Schema.Int)),
	start_line: Schema.optionalKey(Schema.NullOr(Schema.Int)),
	side: Schema.optionalKey(Schema.Literals(['LEFT', 'RIGHT'])),
	pull_request_url: Schema.String,
})
export interface GitHubReviewCommentData extends Schema.Schema.Type<typeof GitHubReviewCommentData> {}
export const GitHubReviewThreadData = Schema.Struct({
	node_id: Schema.NonEmptyString,
	comments: Schema.Array(GitHubReviewCommentData),
})
export interface GitHubReviewThreadData extends Schema.Schema.Type<typeof GitHubReviewThreadData> {}

const common = {
	deliveryId: Schema.NonEmptyString,
	sender: Schema.optionalKey(GitHubUser),
	changes: Schema.optionalKey(
		Schema.Struct({ body: Schema.optionalKey(Schema.Struct({ from: Schema.NullOr(Schema.String) })) }),
	),
}
const assignment = { assignee: Schema.optionalKey(Schema.NullOr(GitHubUser)) }
const label = {
	label: Schema.optionalKey(
		Schema.Struct({
			id: GitHubId,
			name: Schema.String,
			color: Schema.String,
			description: Schema.optionalKey(Schema.NullOr(Schema.String)),
		}),
	),
}
const issue = { ...common, resource: GitHubIssueRef, event: Schema.Literal('issues'), issue: GitHubIssueData }
const pr = {
	...common,
	resource: GitHubPullRequestRef,
	event: Schema.Literal('pull_request'),
	pull_request: GitHubPullRequestData,
}
const review = { ...common, resource: GitHubPullRequestRef, pull_request: GitHubPullRequestData }
const commitRef = Schema.Struct({ ref: Schema.String, sha: Schema.NonEmptyString })

export const GitHubCreationEvent = Schema.Union([
	Schema.Struct({ ...issue, action: Schema.Literal('opened') }),
	Schema.Struct({ ...pr, action: Schema.Literal('opened') }),
])
export type GitHubCreationEvent = typeof GitHubCreationEvent.Type

export const GitHubActivityEvent = Schema.Union([
	GitHubCreationEvent,
	Schema.Struct({ ...issue, action: Schema.Literals(['edited', 'closed', 'reopened']) }),
	Schema.Struct({ ...issue, ...assignment, action: Schema.Literals(['assigned', 'unassigned']) }),
	Schema.Struct({ ...issue, ...label, action: Schema.Literals(['labeled', 'unlabeled']) }),
	Schema.Struct({
		...common,
		resource: GitHubDiscussionRef,
		event: Schema.Literal('issue_comment'),
		action: Schema.Literals(['created', 'edited', 'deleted']),
		issue: GitHubIssueData,
		comment: GitHubCommentData,
	}),
	Schema.Struct({ ...pr, action: Schema.Literals(['edited', 'reopened']) }),
	Schema.Struct({
		...pr,
		action: Schema.Literal('closed'),
		pull_request: Schema.Struct({ ...GitHubPullRequestData.fields, merged: Schema.Boolean }),
	}),
	Schema.Struct({
		...pr,
		action: Schema.Literals(['converted_to_draft', 'ready_for_review']),
		pull_request: Schema.Struct({ ...GitHubPullRequestData.fields, draft: Schema.Boolean }),
	}),
	Schema.Struct({
		...pr,
		action: Schema.Literal('synchronize'),
		before: Schema.NonEmptyString,
		after: Schema.NonEmptyString,
		pull_request: Schema.Struct({ ...GitHubPullRequestData.fields, head: commitRef, base: commitRef }),
	}),
	Schema.Struct({ ...pr, ...assignment, action: Schema.Literals(['assigned', 'unassigned']) }),
	Schema.Struct({ ...pr, ...label, action: Schema.Literals(['labeled', 'unlabeled']) }),
	Schema.Struct({
		...pr,
		action: Schema.Literals(['review_requested', 'review_request_removed']),
		...reviewRequestFields,
	}).check(Schema.makeFilter((e) => e.requested_reviewer != null || e.requested_team != null)),
	Schema.Struct({
		...review,
		event: Schema.Literal('pull_request_review'),
		action: Schema.Literals(['submitted', 'edited', 'dismissed']),
		review: GitHubReviewData,
	}),
	Schema.Struct({
		...review,
		event: Schema.Literal('pull_request_review_comment'),
		action: Schema.Literals(['created', 'edited', 'deleted']),
		comment: GitHubReviewCommentData,
	}),
	Schema.Struct({
		...review,
		event: Schema.Literal('pull_request_review_thread'),
		action: Schema.Literals(['resolved', 'unresolved']),
		thread: GitHubReviewThreadData,
	}),
]).check(
	Schema.makeFilter((event) => {
		if (event.sender === undefined && event.event !== 'pull_request_review_thread') return false
		if (event.event === 'issues' || event.event === 'issue_comment')
			return (
				event.issue.number === event.resource.number &&
				(event.issue.pull_request === undefined) === (event.resource.kind === 'github.issue')
			)
		if (event.pull_request.number !== event.resource.number) return false
		if (event.event === 'pull_request' && event.action === 'synchronize')
			return event.pull_request.head.sha === event.after
		if (event.event === 'pull_request_review' && event.action === 'submitted')
			return ['approved', 'changes_requested', 'commented'].includes(event.review.state)
		const comments = Match.value(event).pipe(
			Match.when({ event: 'pull_request_review_comment' }, (event) => [event.comment]),
			Match.when({ event: 'pull_request_review_thread' }, (event) => event.thread.comments),
			Match.orElse(() => []),
		)
		const parent =
			`/repos/${event.resource.repository.owner}/${event.resource.repository.name}/pulls/${event.resource.number}`.toLowerCase()
		return comments.every(
			(comment) =>
				comment.pull_request_url.toLowerCase().endsWith(parent) && comment.in_reply_to_id !== comment.id,
		)
	}),
)
export type GitHubActivityEvent = typeof GitHubActivityEvent.Type

export const GitHubMentionEvent = Schema.Union([
	GitHubCreationEvent,
	Schema.Struct({ ...issue, action: Schema.Literal('edited') }),
	Schema.Struct({ ...pr, action: Schema.Literal('edited') }),
	Schema.Struct({
		...common,
		resource: GitHubDiscussionRef,
		event: Schema.Literal('issue_comment'),
		action: Schema.Literals(['created', 'edited']),
		issue: GitHubIssueData,
		comment: GitHubCommentData,
	}),
	Schema.Struct({
		...review,
		event: Schema.Literal('pull_request_review'),
		action: Schema.Literals(['submitted', 'edited']),
		review: GitHubReviewData,
	}),
	Schema.Struct({
		...review,
		event: Schema.Literal('pull_request_review_comment'),
		action: Schema.Literals(['created', 'edited']),
		comment: GitHubReviewCommentData,
	}),
])
export type GitHubMentionEvent = typeof GitHubMentionEvent.Type

export const activityEventDefinition = {
	name: 'github.activity',
	version: '1',
	provider: 'github',
	event: GitHubActivityEvent,
	resource: GitHubDiscussionRef,
	resourceKey: issueResourceKey,
	identify: (event: GitHubActivityEvent) => ({
		installation: String(event.resource.repository.installationId),
		eventId: event.deliveryId,
		resource: event.resource,
	}),
} satisfies EventDefinition<typeof GitHubActivityEvent, typeof GitHubDiscussionRef>

export const reviewCommentRootId = (comment: GitHubReviewCommentData) => comment.in_reply_to_id ?? comment.id
