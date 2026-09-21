import { Schema } from 'effect'

import { GitHubId } from './GitHubIdentity'

const GitHubNonNegativeInt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))
const GitHubPositiveInt = Schema.Int.check(Schema.isGreaterThan(0))

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

export const GitHubCheckRunRef = Schema.Struct({
	...GitHubRepositoryRef.fields,
	id: GitHubId,
})
export interface GitHubCheckRunRef extends Schema.Schema.Type<typeof GitHubCheckRunRef> {}

export const GitHubActionsJobRef = Schema.Struct({
	...GitHubRepositoryRef.fields,
	id: GitHubId,
})
export interface GitHubActionsJobRef extends Schema.Schema.Type<typeof GitHubActionsJobRef> {}

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

export const GitHubPullRequestFileStatus = Schema.Literals([
	'added',
	'deleted',
	'modified',
	'renamed',
	'copied',
	'changed',
	'unchanged',
])
export type GitHubPullRequestFileStatus = typeof GitHubPullRequestFileStatus.Type

export const GitHubPullRequestFile = Schema.Struct({
	sha: Schema.NullOr(Schema.NonEmptyString),
	filename: Schema.NonEmptyString,
	previousFilename: Schema.optionalKey(Schema.NonEmptyString),
	status: GitHubPullRequestFileStatus,
	additions: GitHubNonNegativeInt,
	deletions: GitHubNonNegativeInt,
	changes: GitHubNonNegativeInt,
	blobUrl: Schema.NullOr(Schema.String),
	rawUrl: Schema.NullOr(Schema.String),
	contentsUrl: Schema.String,
	patch: Schema.optionalKey(Schema.String),
})
export interface GitHubPullRequestFile extends Schema.Schema.Type<typeof GitHubPullRequestFile> {}

export const GitHubCommit = Schema.Struct({
	sha: Schema.NonEmptyString,
	message: Schema.String,
	apiUrl: Schema.String,
	url: Schema.String,
	author: Schema.NullOr(GitHubParticipant),
	committer: Schema.NullOr(GitHubParticipant),
})
export interface GitHubCommit extends Schema.Schema.Type<typeof GitHubCommit> {}

export const GitHubDiffSide = Schema.Literals(['LEFT', 'RIGHT'])
export type GitHubDiffSide = typeof GitHubDiffSide.Type

const GitHubDiffLine = GitHubPositiveInt

export const GitHubReviewCommentLocation = Schema.TaggedUnion({
	Line: {
		line: GitHubDiffLine,
		side: GitHubDiffSide,
	},
	Range: {
		startLine: GitHubDiffLine,
		startSide: GitHubDiffSide,
		line: GitHubDiffLine,
		side: GitHubDiffSide,
	},
	File: {},
})
export type GitHubReviewCommentLocation = typeof GitHubReviewCommentLocation.Type

export const GitHubMergeMethod = Schema.Literals(['merge', 'squash', 'rebase'])
export type GitHubMergeMethod = typeof GitHubMergeMethod.Type

export const GitHubMergeResult = Schema.Struct({
	merged: Schema.Boolean,
	sha: Schema.String,
	message: Schema.String,
})
export interface GitHubMergeResult extends Schema.Schema.Type<typeof GitHubMergeResult> {}

export const GitHubCheckStatus = Schema.Literals([
	'queued',
	'in_progress',
	'completed',
	'waiting',
	'requested',
	'pending',
])
export type GitHubCheckStatus = typeof GitHubCheckStatus.Type

export const GitHubCheckConclusion = Schema.Literals([
	'success',
	'failure',
	'timed_out',
	'cancelled',
	'action_required',
	'neutral',
	'skipped',
	'stale',
	'startup_failure',
])
export type GitHubCheckConclusion = typeof GitHubCheckConclusion.Type

export const GitHubCheckRunInfo = Schema.Struct({
	ref: GitHubCheckRunRef,
	name: Schema.String,
	headSha: Schema.NonEmptyString,
	status: GitHubCheckStatus,
	conclusion: Schema.NullOr(GitHubCheckConclusion),
	startedAt: Schema.NullOr(Schema.String),
	completedAt: Schema.NullOr(Schema.String),
	apiUrl: Schema.String,
	url: Schema.NullOr(Schema.String),
	detailsUrl: Schema.NullOr(Schema.String),
	checkSuiteId: Schema.NullOr(GitHubId),
	outputTitle: Schema.NullOr(Schema.String),
	outputSummary: Schema.NullOr(Schema.String),
	outputText: Schema.NullOr(Schema.String),
	annotationCount: GitHubNonNegativeInt,
})
export interface GitHubCheckRunInfo extends Schema.Schema.Type<typeof GitHubCheckRunInfo> {}

export const GitHubCheckAnnotationLevel = Schema.Literals(['notice', 'warning', 'failure'])
export type GitHubCheckAnnotationLevel = typeof GitHubCheckAnnotationLevel.Type

export const GitHubCheckAnnotation = Schema.Struct({
	path: Schema.NonEmptyString,
	startLine: GitHubDiffLine,
	endLine: GitHubDiffLine,
	startColumn: Schema.NullOr(GitHubDiffLine),
	endColumn: Schema.NullOr(GitHubDiffLine),
	level: Schema.NullOr(GitHubCheckAnnotationLevel),
	title: Schema.NullOr(Schema.String),
	message: Schema.NullOr(Schema.String),
	rawDetails: Schema.NullOr(Schema.String),
	blobUrl: Schema.String,
})
export interface GitHubCheckAnnotation extends Schema.Schema.Type<typeof GitHubCheckAnnotation> {}

export const GitHubActionsJobStep = Schema.Struct({
	name: Schema.String,
	status: GitHubCheckStatus,
	conclusion: Schema.NullOr(GitHubCheckConclusion),
	number: GitHubPositiveInt,
	startedAt: Schema.NullOr(Schema.String),
	completedAt: Schema.NullOr(Schema.String),
})
export interface GitHubActionsJobStep extends Schema.Schema.Type<typeof GitHubActionsJobStep> {}

export const GitHubActionsJobInfo = Schema.Struct({
	ref: GitHubActionsJobRef,
	runId: GitHubId,
	name: Schema.String,
	status: GitHubCheckStatus,
	conclusion: Schema.NullOr(GitHubCheckConclusion),
	headSha: Schema.NonEmptyString,
	apiUrl: Schema.String,
	url: Schema.NullOr(Schema.String),
	startedAt: Schema.NullOr(Schema.String),
	completedAt: Schema.NullOr(Schema.String),
	checkRunUrl: Schema.String,
	workflowName: Schema.optionalKey(Schema.String),
	headBranch: Schema.optionalKey(Schema.NullOr(Schema.String)),
	steps: Schema.Array(GitHubActionsJobStep),
})
export interface GitHubActionsJobInfo extends Schema.Schema.Type<typeof GitHubActionsJobInfo> {}

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
