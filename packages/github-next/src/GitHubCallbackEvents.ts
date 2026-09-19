import { Schema } from 'effect'

import { GitHubId } from './GitHubIdentity'
import {
	GitHubEventId,
	GitHubLabel,
	GitHubParticipant,
	GitHubReview,
	GitHubReviewThread,
	GitHubTeam,
} from './GitHubModels'
import { GitHubIssue, GitHubIssueComment, GitHubPullRequest, GitHubReviewComment } from './GitHubResources'

const issueEventFields = {
	eventId: GitHubEventId,
	issue: GitHubIssue,
	actor: GitHubParticipant,
}

const pullRequestEventFields = {
	eventId: GitHubEventId,
	pullRequest: GitHubPullRequest,
	actor: GitHubParticipant,
}

export const GitHubIssueOpened = Schema.TaggedStruct('GitHubIssueOpened', {
	...issueEventFields,
	title: Schema.String,
	body: Schema.NullOr(Schema.String),
})
export type GitHubIssueOpened = typeof GitHubIssueOpened.Type

export const GitHubIssueEdited = Schema.TaggedStruct('GitHubIssueEdited', issueEventFields)
export type GitHubIssueEdited = typeof GitHubIssueEdited.Type
export const GitHubIssueClosed = Schema.TaggedStruct('GitHubIssueClosed', issueEventFields)
export type GitHubIssueClosed = typeof GitHubIssueClosed.Type
export const GitHubIssueReopened = Schema.TaggedStruct('GitHubIssueReopened', issueEventFields)
export type GitHubIssueReopened = typeof GitHubIssueReopened.Type
export const GitHubIssueAssigned = Schema.TaggedStruct('GitHubIssueAssigned', {
	...issueEventFields,
	assignee: Schema.NullOr(GitHubParticipant),
})
export type GitHubIssueAssigned = typeof GitHubIssueAssigned.Type
export const GitHubIssueUnassigned = Schema.TaggedStruct('GitHubIssueUnassigned', {
	...issueEventFields,
	assignee: Schema.NullOr(GitHubParticipant),
})
export type GitHubIssueUnassigned = typeof GitHubIssueUnassigned.Type
export const GitHubIssueLabeled = Schema.TaggedStruct('GitHubIssueLabeled', {
	...issueEventFields,
	label: Schema.NullOr(GitHubLabel),
})
export type GitHubIssueLabeled = typeof GitHubIssueLabeled.Type
export const GitHubIssueUnlabeled = Schema.TaggedStruct('GitHubIssueUnlabeled', {
	...issueEventFields,
	label: Schema.NullOr(GitHubLabel),
})
export type GitHubIssueUnlabeled = typeof GitHubIssueUnlabeled.Type

export const GitHubIssueCommentCreated = Schema.TaggedStruct('GitHubIssueCommentCreated', {
	...issueEventFields,
	comment: GitHubIssueComment,
})
export type GitHubIssueCommentCreated = typeof GitHubIssueCommentCreated.Type
export const GitHubIssueCommentEdited = Schema.TaggedStruct('GitHubIssueCommentEdited', {
	...issueEventFields,
	comment: GitHubIssueComment,
})
export type GitHubIssueCommentEdited = typeof GitHubIssueCommentEdited.Type
export const GitHubIssueCommentDeleted = Schema.TaggedStruct('GitHubIssueCommentDeleted', {
	...issueEventFields,
	comment: GitHubIssueComment,
})
export type GitHubIssueCommentDeleted = typeof GitHubIssueCommentDeleted.Type

export const GitHubIssueEvent = Schema.Union([
	GitHubIssueEdited,
	GitHubIssueClosed,
	GitHubIssueReopened,
	GitHubIssueAssigned,
	GitHubIssueUnassigned,
	GitHubIssueLabeled,
	GitHubIssueUnlabeled,
	GitHubIssueCommentCreated,
	GitHubIssueCommentEdited,
	GitHubIssueCommentDeleted,
])
export type GitHubIssueEvent = typeof GitHubIssueEvent.Type

export const GitHubPrOpened = Schema.TaggedStruct('GitHubPrOpened', {
	...pullRequestEventFields,
	title: Schema.String,
	body: Schema.NullOr(Schema.String),
})
export type GitHubPrOpened = typeof GitHubPrOpened.Type

export const GitHubPrEdited = Schema.TaggedStruct('GitHubPrEdited', pullRequestEventFields)
export type GitHubPrEdited = typeof GitHubPrEdited.Type
export const GitHubPrClosed = Schema.TaggedStruct('GitHubPrClosed', pullRequestEventFields)
export type GitHubPrClosed = typeof GitHubPrClosed.Type
export const GitHubPrMerged = Schema.TaggedStruct('GitHubPrMerged', pullRequestEventFields)
export type GitHubPrMerged = typeof GitHubPrMerged.Type
export const GitHubPrReopened = Schema.TaggedStruct('GitHubPrReopened', pullRequestEventFields)
export type GitHubPrReopened = typeof GitHubPrReopened.Type
export const GitHubPrSynchronized = Schema.TaggedStruct('GitHubPrSynchronized', pullRequestEventFields)
export type GitHubPrSynchronized = typeof GitHubPrSynchronized.Type
export const GitHubPrConvertedToDraft = Schema.TaggedStruct('GitHubPrConvertedToDraft', pullRequestEventFields)
export type GitHubPrConvertedToDraft = typeof GitHubPrConvertedToDraft.Type
export const GitHubPrReadyForReview = Schema.TaggedStruct('GitHubPrReadyForReview', pullRequestEventFields)
export type GitHubPrReadyForReview = typeof GitHubPrReadyForReview.Type
export const GitHubPrAssigned = Schema.TaggedStruct('GitHubPrAssigned', {
	...pullRequestEventFields,
	assignee: Schema.NullOr(GitHubParticipant),
})
export type GitHubPrAssigned = typeof GitHubPrAssigned.Type
export const GitHubPrUnassigned = Schema.TaggedStruct('GitHubPrUnassigned', {
	...pullRequestEventFields,
	assignee: Schema.NullOr(GitHubParticipant),
})
export type GitHubPrUnassigned = typeof GitHubPrUnassigned.Type
export const GitHubPrLabeled = Schema.TaggedStruct('GitHubPrLabeled', {
	...pullRequestEventFields,
	label: Schema.NullOr(GitHubLabel),
})
export type GitHubPrLabeled = typeof GitHubPrLabeled.Type
export const GitHubPrUnlabeled = Schema.TaggedStruct('GitHubPrUnlabeled', {
	...pullRequestEventFields,
	label: Schema.NullOr(GitHubLabel),
})
export type GitHubPrUnlabeled = typeof GitHubPrUnlabeled.Type
const reviewRequestFields = {
	...pullRequestEventFields,
	reviewer: Schema.NullOr(GitHubParticipant),
	team: Schema.NullOr(GitHubTeam),
}
export const GitHubPrReviewRequested = Schema.TaggedStruct('GitHubPrReviewRequested', reviewRequestFields)
export type GitHubPrReviewRequested = typeof GitHubPrReviewRequested.Type
export const GitHubPrReviewRequestRemoved = Schema.TaggedStruct('GitHubPrReviewRequestRemoved', reviewRequestFields)
export type GitHubPrReviewRequestRemoved = typeof GitHubPrReviewRequestRemoved.Type

export const GitHubPrCommentCreated = Schema.TaggedStruct('GitHubPrCommentCreated', {
	...pullRequestEventFields,
	comment: GitHubIssueComment,
})
export type GitHubPrCommentCreated = typeof GitHubPrCommentCreated.Type
export const GitHubPrCommentEdited = Schema.TaggedStruct('GitHubPrCommentEdited', {
	...pullRequestEventFields,
	comment: GitHubIssueComment,
})
export type GitHubPrCommentEdited = typeof GitHubPrCommentEdited.Type
export const GitHubPrCommentDeleted = Schema.TaggedStruct('GitHubPrCommentDeleted', {
	...pullRequestEventFields,
	comment: GitHubIssueComment,
})
export type GitHubPrCommentDeleted = typeof GitHubPrCommentDeleted.Type

const reviewFields = { ...pullRequestEventFields, review: GitHubReview }
export const GitHubPrReviewSubmitted = Schema.TaggedStruct('GitHubPrReviewSubmitted', reviewFields)
export type GitHubPrReviewSubmitted = typeof GitHubPrReviewSubmitted.Type
export const GitHubPrReviewEdited = Schema.TaggedStruct('GitHubPrReviewEdited', reviewFields)
export type GitHubPrReviewEdited = typeof GitHubPrReviewEdited.Type
export const GitHubPrReviewDismissed = Schema.TaggedStruct('GitHubPrReviewDismissed', reviewFields)
export type GitHubPrReviewDismissed = typeof GitHubPrReviewDismissed.Type

const reviewCommentFields = { ...pullRequestEventFields, comment: GitHubReviewComment }
export const GitHubPrReviewCommentCreated = Schema.TaggedStruct('GitHubPrReviewCommentCreated', reviewCommentFields)
export type GitHubPrReviewCommentCreated = typeof GitHubPrReviewCommentCreated.Type
export const GitHubPrReviewCommentEdited = Schema.TaggedStruct('GitHubPrReviewCommentEdited', reviewCommentFields)
export type GitHubPrReviewCommentEdited = typeof GitHubPrReviewCommentEdited.Type
export const GitHubPrReviewCommentDeleted = Schema.TaggedStruct('GitHubPrReviewCommentDeleted', reviewCommentFields)
export type GitHubPrReviewCommentDeleted = typeof GitHubPrReviewCommentDeleted.Type

const reviewThreadFields = { ...pullRequestEventFields, thread: GitHubReviewThread }
export const GitHubPrReviewThreadResolved = Schema.TaggedStruct('GitHubPrReviewThreadResolved', reviewThreadFields)
export type GitHubPrReviewThreadResolved = typeof GitHubPrReviewThreadResolved.Type
export const GitHubPrReviewThreadUnresolved = Schema.TaggedStruct('GitHubPrReviewThreadUnresolved', reviewThreadFields)
export type GitHubPrReviewThreadUnresolved = typeof GitHubPrReviewThreadUnresolved.Type

export const GitHubCheckStatus = Schema.Literals(['queued', 'in_progress', 'completed'])
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
])
export type GitHubCheckConclusion = typeof GitHubCheckConclusion.Type

export const GitHubPrCheckCompleted = Schema.TaggedStruct('GitHubPrCheckCompleted', {
	...pullRequestEventFields,
	name: Schema.String,
	status: Schema.Literal('completed'),
	conclusion: GitHubCheckConclusion,
	detailsUrl: Schema.NullOr(Schema.String),
	headSha: Schema.NonEmptyString,
	checkSuiteId: Schema.NullOr(GitHubId),
	startedAt: Schema.NullOr(Schema.String),
	completedAt: Schema.NullOr(Schema.String),
})
export type GitHubPrCheckCompleted = typeof GitHubPrCheckCompleted.Type

export const GitHubPrEvent = Schema.Union([
	GitHubPrEdited,
	GitHubPrClosed,
	GitHubPrMerged,
	GitHubPrReopened,
	GitHubPrSynchronized,
	GitHubPrConvertedToDraft,
	GitHubPrReadyForReview,
	GitHubPrAssigned,
	GitHubPrUnassigned,
	GitHubPrLabeled,
	GitHubPrUnlabeled,
	GitHubPrReviewRequested,
	GitHubPrReviewRequestRemoved,
	GitHubPrCommentCreated,
	GitHubPrCommentEdited,
	GitHubPrCommentDeleted,
	GitHubPrReviewSubmitted,
	GitHubPrReviewEdited,
	GitHubPrReviewDismissed,
	GitHubPrReviewCommentCreated,
	GitHubPrReviewCommentEdited,
	GitHubPrReviewCommentDeleted,
	GitHubPrReviewThreadResolved,
	GitHubPrReviewThreadUnresolved,
	GitHubPrCheckCompleted,
])
export type GitHubPrEvent = typeof GitHubPrEvent.Type

export const GitHubIssueCreated = Schema.TaggedStruct('GitHubIssueCreated', {
	issue: GitHubIssue,
	trigger: GitHubIssueOpened,
	events: Schema.Array(GitHubIssueEvent),
})
export type GitHubIssueCreated = typeof GitHubIssueCreated.Type

export const GitHubPrCreated = Schema.TaggedStruct('GitHubPrCreated', {
	pullRequest: GitHubPullRequest,
	trigger: GitHubPrOpened,
	events: Schema.Array(GitHubPrEvent),
})
export type GitHubPrCreated = typeof GitHubPrCreated.Type

export const GitHubIssueBodyMention = GitHubIssueOpened
export type GitHubIssueBodyMention = GitHubIssueOpened
export const GitHubIssueCommentMention = GitHubIssueCommentCreated
export type GitHubIssueCommentMention = GitHubIssueCommentCreated
export const GitHubIssueMention = Schema.Union([GitHubIssueBodyMention, GitHubIssueCommentMention])
export type GitHubIssueMention = typeof GitHubIssueMention.Type

export const GitHubPrBodyMention = GitHubPrOpened
export type GitHubPrBodyMention = GitHubPrOpened
export const GitHubPrCommentMention = GitHubPrCommentCreated
export type GitHubPrCommentMention = GitHubPrCommentCreated
export const GitHubPrReviewCommentMention = GitHubPrReviewCommentCreated
export type GitHubPrReviewCommentMention = GitHubPrReviewCommentCreated
export const GitHubPrMention = Schema.Union([GitHubPrBodyMention, GitHubPrCommentMention, GitHubPrReviewCommentMention])
export type GitHubPrMention = typeof GitHubPrMention.Type

export const GitHubIssueMentioned = Schema.TaggedStruct('GitHubIssueMentioned', {
	issue: GitHubIssue,
	trigger: GitHubIssueMention,
	events: Schema.Array(Schema.Union([GitHubIssueOpened, GitHubIssueEvent])),
})
export type GitHubIssueMentioned = typeof GitHubIssueMentioned.Type

export const GitHubPrMentioned = Schema.TaggedStruct('GitHubPrMentioned', {
	pullRequest: GitHubPullRequest,
	trigger: GitHubPrMention,
	events: Schema.Array(Schema.Union([GitHubPrOpened, GitHubPrEvent])),
})
export type GitHubPrMentioned = typeof GitHubPrMentioned.Type

export const GitHubMentioned = Schema.Union([GitHubIssueMentioned, GitHubPrMentioned])
export type GitHubMentioned = typeof GitHubMentioned.Type

export const GitHubSubscribedIssueEvents = Schema.TaggedStruct('GitHubSubscribedIssueEvents', {
	issue: GitHubIssue,
	events: Schema.NonEmptyArray(GitHubIssueEvent),
})
export type GitHubSubscribedIssueEvents = typeof GitHubSubscribedIssueEvents.Type

export const GitHubSubscribedPrEvents = Schema.TaggedStruct('GitHubSubscribedPrEvents', {
	pullRequest: GitHubPullRequest,
	events: Schema.NonEmptyArray(GitHubPrEvent),
})
export type GitHubSubscribedPrEvents = typeof GitHubSubscribedPrEvents.Type
