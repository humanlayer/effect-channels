import { Schema } from 'effect'

import { GitHubId } from './GitHubIdentity'

export const GitHubEventId = Schema.NonEmptyString.pipe(Schema.brand('GitHubEventId'))
export type GitHubEventId = typeof GitHubEventId.Type

export const GitHubRepositoryRef = Schema.Struct({
	installationId: GitHubId,
	repositoryId: GitHubId,
	owner: Schema.NonEmptyString,
	repository: Schema.NonEmptyString,
})
export interface GitHubRepositoryRef extends Schema.Schema.Type<typeof GitHubRepositoryRef> {}

export const GitHubIssueRef = Schema.Struct({
	...GitHubRepositoryRef.fields,
	number: GitHubId,
})
export interface GitHubIssueRef extends Schema.Schema.Type<typeof GitHubIssueRef> {}

export const GitHubPullRequestRef = Schema.Struct({
	...GitHubRepositoryRef.fields,
	number: GitHubId,
})
export interface GitHubPullRequestRef extends Schema.Schema.Type<typeof GitHubPullRequestRef> {}

export const GitHubDiscussionRef = Schema.TaggedUnion({
	Issue: { ref: GitHubIssueRef },
	PullRequest: { ref: GitHubPullRequestRef },
})
export type GitHubDiscussionRef = typeof GitHubDiscussionRef.Type

export const GitHubIssueCommentRef = Schema.Struct({
	discussion: GitHubDiscussionRef,
	id: GitHubId,
})
export interface GitHubIssueCommentRef extends Schema.Schema.Type<typeof GitHubIssueCommentRef> {}

export const GitHubReviewCommentRef = Schema.Struct({
	pullRequest: GitHubPullRequestRef,
	id: GitHubId,
})
export interface GitHubReviewCommentRef extends Schema.Schema.Type<typeof GitHubReviewCommentRef> {}

export const GitHubCommentRef = Schema.Union([GitHubIssueCommentRef, GitHubReviewCommentRef])
export type GitHubCommentRef = typeof GitHubCommentRef.Type

export const GitHubParticipant = Schema.Struct({
	id: GitHubId,
	login: Schema.NonEmptyString,
	type: Schema.String,
})
export interface GitHubParticipant extends Schema.Schema.Type<typeof GitHubParticipant> {}

export const GitHubContent = Schema.Struct({ markdown: Schema.String })
export interface GitHubContent extends Schema.Schema.Type<typeof GitHubContent> {}

export const GitHubIssueState = Schema.Literals(['open', 'closed'])
export type GitHubIssueState = typeof GitHubIssueState.Type

export const GitHubIssueInfo = Schema.Struct({
	ref: GitHubIssueRef,
	title: Schema.String,
	body: Schema.NullOr(Schema.String),
	state: GitHubIssueState,
	url: Schema.String,
	author: GitHubParticipant,
})
export interface GitHubIssueInfo extends Schema.Schema.Type<typeof GitHubIssueInfo> {}

export const GitHubPullRequestInfo = Schema.Struct({
	ref: GitHubPullRequestRef,
	title: Schema.String,
	body: Schema.NullOr(Schema.String),
	state: GitHubIssueState,
	url: Schema.String,
	author: Schema.NullOr(GitHubParticipant),
	draft: Schema.Boolean,
	merged: Schema.Boolean,
	headRef: Schema.String,
	headSha: Schema.NonEmptyString,
	baseRef: Schema.String,
	baseSha: Schema.NonEmptyString,
})
export interface GitHubPullRequestInfo extends Schema.Schema.Type<typeof GitHubPullRequestInfo> {}

export const GitHubReviewState = Schema.Literals(['approved', 'changes_requested', 'commented', 'dismissed', 'pending'])
export type GitHubReviewState = typeof GitHubReviewState.Type

export const GitHubReviewRef = Schema.Struct({
	pullRequest: GitHubPullRequestRef,
	id: GitHubId,
	nodeId: Schema.NonEmptyString,
})
export interface GitHubReviewRef extends Schema.Schema.Type<typeof GitHubReviewRef> {}

export const GitHubReview = Schema.Struct({
	ref: GitHubReviewRef,
	body: Schema.NullOr(Schema.String),
	author: Schema.NullOr(GitHubParticipant),
	state: GitHubReviewState,
	commitId: Schema.String,
	url: Schema.String,
})
export interface GitHubReview extends Schema.Schema.Type<typeof GitHubReview> {}

export const GitHubReviewThread = Schema.Struct({
	nodeId: Schema.NonEmptyString,
	comments: Schema.Array(GitHubReviewCommentRef),
})
export interface GitHubReviewThread extends Schema.Schema.Type<typeof GitHubReviewThread> {}

export const GitHubLabel = Schema.Struct({
	id: Schema.optionalKey(GitHubId),
	name: Schema.NonEmptyString,
	color: Schema.String,
	description: Schema.NullOr(Schema.String),
})
export interface GitHubLabel extends Schema.Schema.Type<typeof GitHubLabel> {}

export const GitHubTeam = Schema.Struct({
	id: GitHubId,
	name: Schema.NonEmptyString,
	slug: Schema.NonEmptyString,
})
export interface GitHubTeam extends Schema.Schema.Type<typeof GitHubTeam> {}

export const GitHubReaction = Schema.Literals(['+1', '-1', 'laugh', 'confused', 'heart', 'hooray', 'rocket', 'eyes'])
export type GitHubReaction = typeof GitHubReaction.Type
