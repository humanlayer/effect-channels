import {
	type MailboxSubscriptionError,
	type MailboxSubscriptionResult,
	MailboxSubscriptions,
} from '@humanlayer/channels-delivery'
import { Effect, Schema } from 'effect'

import type {
	GitHubApiError,
	GitHubIssueCloseReason,
	GitHubLabels,
	GitHubMergeOptions,
	GitHubPostReviewCommentOptions,
} from './GitHubApi'
import { GitHubApi } from './GitHubApi'
import { GitHubId } from './GitHubIdentity'
import {
	type GitHubAccessLevel,
	GitHubActionsJobRef,
	type GitHubActionsJobInfo,
	GitHubCheckAnnotation,
	GitHubCheckRunRef,
	type GitHubCheckRunInfo,
	GitHubCommit,
	type GitHubContent,
	GitHubDiffLine,
	GitHubIssueCommentRef,
	type GitHubIssueInfo,
	GitHubIssueRef,
	GitHubLabel,
	type GitHubMergeResult,
	GitHubPullRequestFile,
	type GitHubPullRequestInfo,
	GitHubPullRequestRef,
	type GitHubReaction,
	GitHubReactionTarget,
	GitHubReview,
	GitHubReviewCommentRef,
	GitHubParticipant,
} from './GitHubModels'

const issueSpanAttributes = (ref: GitHubIssueRef) => ({
	'github.installation_id': ref.installationId,
	'github.repository_id': ref.repositoryId,
	'github.issue_number': ref.number,
})

const pullRequestSpanAttributes = (ref: GitHubPullRequestRef) => ({
	'github.installation_id': ref.installationId,
	'github.repository_id': ref.repositoryId,
	'github.pull_request_number': ref.number,
})

const checkRunSpanAttributes = (ref: GitHubCheckRunRef) => ({
	'github.installation_id': ref.installationId,
	'github.repository_id': ref.repositoryId,
	'github.check_run_id': ref.id,
})

const actionsJobSpanAttributes = (ref: GitHubActionsJobRef) => ({
	'github.installation_id': ref.installationId,
	'github.repository_id': ref.repositoryId,
	'github.actions_job_id': ref.id,
})

export class GitHubIssue extends Schema.TaggedClass<GitHubIssue>()('GitHubIssue', {
	ref: GitHubIssueRef,
	mailboxKey: Schema.NonEmptyString,
}) {
	subscribe(): Effect.Effect<MailboxSubscriptionResult, MailboxSubscriptionError, MailboxSubscriptions> {
		return Effect.flatMap(MailboxSubscriptions, (subscriptions) =>
			subscriptions.subscribe({ mailboxKey: this.mailboxKey }),
		).pipe(Effect.withSpan('github.issue.subscribe', { attributes: issueSpanAttributes(this.ref) }))
	}

	isSubscribed(): Effect.Effect<boolean, MailboxSubscriptionError, MailboxSubscriptions> {
		return Effect.flatMap(MailboxSubscriptions, (subscriptions) =>
			subscriptions.isSubscribed({ mailboxKey: this.mailboxKey }),
		).pipe(Effect.withSpan('github.issue.is_subscribed', { attributes: issueSpanAttributes(this.ref) }))
	}

	unsubscribe(): Effect.Effect<void, MailboxSubscriptionError, MailboxSubscriptions> {
		return Effect.flatMap(MailboxSubscriptions, (subscriptions) =>
			subscriptions.unsubscribe({ mailboxKey: this.mailboxKey }),
		).pipe(Effect.withSpan('github.issue.unsubscribe', { attributes: issueSpanAttributes(this.ref) }))
	}

	fetchInfo(): Effect.Effect<GitHubIssueInfo, GitHubApiError, GitHubApi> {
		return Effect.flatMap(GitHubApi, (api) => api.fetchIssue({ issue: this.ref })).pipe(
			Effect.withSpan('github.issue.fetch_info', { attributes: issueSpanAttributes(this.ref) }),
		)
	}

	listComments(): Effect.Effect<GitHubIssueComments, GitHubApiError, GitHubApi> {
		return Effect.flatMap(GitHubApi, (api) => api.listIssueComments({ issue: this.ref })).pipe(
			Effect.withSpan('github.issue.list_comments', { attributes: issueSpanAttributes(this.ref) }),
		)
	}

	postComment(content: GitHubContent): Effect.Effect<GitHubIssueComment, GitHubApiError, GitHubApi> {
		return Effect.flatMap(GitHubApi, (api) => api.postIssueComment({ issue: this.ref, content })).pipe(
			Effect.withSpan('github.issue.post_comment', { attributes: issueSpanAttributes(this.ref) }),
		)
	}

	close(reason: GitHubIssueCloseReason): Effect.Effect<GitHubIssueInfo, GitHubApiError, GitHubApi> {
		return Effect.flatMap(GitHubApi, (api) => api.closeIssue({ issue: this.ref, reason })).pipe(
			Effect.withSpan('github.issue.close', { attributes: issueSpanAttributes(this.ref) }),
		)
	}

	reopen(): Effect.Effect<GitHubIssueInfo, GitHubApiError, GitHubApi> {
		return Effect.flatMap(GitHubApi, (api) => api.reopenIssue({ issue: this.ref })).pipe(
			Effect.withSpan('github.issue.reopen', { attributes: issueSpanAttributes(this.ref) }),
		)
	}

	listLabels(): Effect.Effect<GitHubLabelsResult, GitHubApiError, GitHubApi> {
		return Effect.flatMap(GitHubApi, (api) => api.listIssueLabels({ issue: this.ref })).pipe(
			Effect.withSpan('github.issue.list_labels', { attributes: issueSpanAttributes(this.ref) }),
		)
	}

	addLabels(labels: GitHubLabels): Effect.Effect<GitHubLabelsResult, GitHubApiError, GitHubApi> {
		return Effect.flatMap(GitHubApi, (api) => api.addIssueLabels({ issue: this.ref, labels })).pipe(
			Effect.withSpan('github.issue.add_labels', { attributes: issueSpanAttributes(this.ref) }),
		)
	}

	setLabels(labels: GitHubLabels): Effect.Effect<GitHubLabelsResult, GitHubApiError, GitHubApi> {
		return Effect.flatMap(GitHubApi, (api) => api.setIssueLabels({ issue: this.ref, labels })).pipe(
			Effect.withSpan('github.issue.set_labels', { attributes: issueSpanAttributes(this.ref) }),
		)
	}

	removeLabel(label: string): Effect.Effect<GitHubLabelsResult, GitHubApiError, GitHubApi> {
		return Effect.flatMap(GitHubApi, (api) => api.removeIssueLabel({ issue: this.ref, label })).pipe(
			Effect.withSpan('github.issue.remove_label', { attributes: issueSpanAttributes(this.ref) }),
		)
	}

	removeAllLabels(): Effect.Effect<void, GitHubApiError, GitHubApi> {
		return Effect.flatMap(GitHubApi, (api) => api.removeAllIssueLabels({ issue: this.ref })).pipe(
			Effect.withSpan('github.issue.remove_all_labels', { attributes: issueSpanAttributes(this.ref) }),
		)
	}

	/** A user's access to this issue's repository, such as the author of an event. */
	fetchUserAccess(login: string): Effect.Effect<GitHubAccessLevel, GitHubApiError, GitHubApi> {
		return Effect.flatMap(GitHubApi, (api) => api.fetchUserAccess({ repository: this.ref, login })).pipe(
			Effect.withSpan('github.issue.fetch_user_access', { attributes: issueSpanAttributes(this.ref) }),
		)
	}
}

export class GitHubPullRequest extends Schema.TaggedClass<GitHubPullRequest>()('GitHubPullRequest', {
	ref: GitHubPullRequestRef,
	mailboxKey: Schema.NonEmptyString,
}) {
	subscribe(): Effect.Effect<MailboxSubscriptionResult, MailboxSubscriptionError, MailboxSubscriptions> {
		return Effect.flatMap(MailboxSubscriptions, (subscriptions) =>
			subscriptions.subscribe({ mailboxKey: this.mailboxKey }),
		).pipe(Effect.withSpan('github.pull_request.subscribe', { attributes: pullRequestSpanAttributes(this.ref) }))
	}

	isSubscribed(): Effect.Effect<boolean, MailboxSubscriptionError, MailboxSubscriptions> {
		return Effect.flatMap(MailboxSubscriptions, (subscriptions) =>
			subscriptions.isSubscribed({ mailboxKey: this.mailboxKey }),
		).pipe(
			Effect.withSpan('github.pull_request.is_subscribed', { attributes: pullRequestSpanAttributes(this.ref) }),
		)
	}

	unsubscribe(): Effect.Effect<void, MailboxSubscriptionError, MailboxSubscriptions> {
		return Effect.flatMap(MailboxSubscriptions, (subscriptions) =>
			subscriptions.unsubscribe({ mailboxKey: this.mailboxKey }),
		).pipe(Effect.withSpan('github.pull_request.unsubscribe', { attributes: pullRequestSpanAttributes(this.ref) }))
	}

	fetchInfo(): Effect.Effect<GitHubPullRequestInfo, GitHubApiError, GitHubApi> {
		return Effect.flatMap(GitHubApi, (api) => api.fetchPullRequest({ pullRequest: this.ref })).pipe(
			Effect.withSpan('github.pull_request.fetch_info', { attributes: pullRequestSpanAttributes(this.ref) }),
		)
	}

	listComments(): Effect.Effect<GitHubIssueComments, GitHubApiError, GitHubApi> {
		return Effect.flatMap(GitHubApi, (api) => api.listPullRequestComments({ pullRequest: this.ref })).pipe(
			Effect.withSpan('github.pull_request.list_comments', { attributes: pullRequestSpanAttributes(this.ref) }),
		)
	}

	listReviews(): Effect.Effect<GitHubReviews, GitHubApiError, GitHubApi> {
		return Effect.flatMap(GitHubApi, (api) => api.listPullRequestReviews({ pullRequest: this.ref })).pipe(
			Effect.withSpan('github.pull_request.list_reviews', { attributes: pullRequestSpanAttributes(this.ref) }),
		)
	}

	listReviewComments(): Effect.Effect<GitHubReviewComments, GitHubApiError, GitHubApi> {
		return Effect.flatMap(GitHubApi, (api) => api.listPullRequestReviewComments({ pullRequest: this.ref })).pipe(
			Effect.withSpan('github.pull_request.list_review_comments', {
				attributes: pullRequestSpanAttributes(this.ref),
			}),
		)
	}

	postComment(content: GitHubContent): Effect.Effect<GitHubIssueComment, GitHubApiError, GitHubApi> {
		return Effect.flatMap(GitHubApi, (api) => api.postPullRequestComment({ pullRequest: this.ref, content })).pipe(
			Effect.withSpan('github.pull_request.post_comment', { attributes: pullRequestSpanAttributes(this.ref) }),
		)
	}

	postReviewComment(
		input: GitHubPostReviewCommentOptions,
	): Effect.Effect<GitHubReviewComment, GitHubApiError, GitHubApi> {
		return Effect.flatMap(GitHubApi, (api) =>
			api.postPullRequestReviewComment({
				pullRequest: this.ref,
				content: input.content,
				commitId: input.commitId,
				path: input.path,
				location: input.location,
			}),
		).pipe(
			Effect.withSpan('github.pull_request.post_review_comment', {
				attributes: pullRequestSpanAttributes(this.ref),
			}),
		)
	}

	listFiles(): Effect.Effect<GitHubPullRequestFiles, GitHubApiError, GitHubApi> {
		return Effect.flatMap(GitHubApi, (api) => api.listPullRequestFiles({ pullRequest: this.ref })).pipe(
			Effect.withSpan('github.pull_request.list_files', { attributes: pullRequestSpanAttributes(this.ref) }),
		)
	}

	fetchDiff(): Effect.Effect<string, GitHubApiError, GitHubApi> {
		return Effect.flatMap(GitHubApi, (api) => api.fetchPullRequestDiff({ pullRequest: this.ref })).pipe(
			Effect.withSpan('github.pull_request.fetch_diff', { attributes: pullRequestSpanAttributes(this.ref) }),
		)
	}

	listCommits(): Effect.Effect<GitHubCommits, GitHubApiError, GitHubApi> {
		return Effect.flatMap(GitHubApi, (api) => api.listPullRequestCommits({ pullRequest: this.ref })).pipe(
			Effect.withSpan('github.pull_request.list_commits', { attributes: pullRequestSpanAttributes(this.ref) }),
		)
	}

	listLabels(): Effect.Effect<GitHubLabelsResult, GitHubApiError, GitHubApi> {
		return Effect.flatMap(GitHubApi, (api) => api.listPullRequestLabels({ pullRequest: this.ref })).pipe(
			Effect.withSpan('github.pull_request.list_labels', { attributes: pullRequestSpanAttributes(this.ref) }),
		)
	}

	addLabels(labels: GitHubLabels): Effect.Effect<GitHubLabelsResult, GitHubApiError, GitHubApi> {
		return Effect.flatMap(GitHubApi, (api) => api.addPullRequestLabels({ pullRequest: this.ref, labels })).pipe(
			Effect.withSpan('github.pull_request.add_labels', { attributes: pullRequestSpanAttributes(this.ref) }),
		)
	}

	setLabels(labels: GitHubLabels): Effect.Effect<GitHubLabelsResult, GitHubApiError, GitHubApi> {
		return Effect.flatMap(GitHubApi, (api) => api.setPullRequestLabels({ pullRequest: this.ref, labels })).pipe(
			Effect.withSpan('github.pull_request.set_labels', { attributes: pullRequestSpanAttributes(this.ref) }),
		)
	}

	removeLabel(label: string): Effect.Effect<GitHubLabelsResult, GitHubApiError, GitHubApi> {
		return Effect.flatMap(GitHubApi, (api) => api.removePullRequestLabel({ pullRequest: this.ref, label })).pipe(
			Effect.withSpan('github.pull_request.remove_label', { attributes: pullRequestSpanAttributes(this.ref) }),
		)
	}

	removeAllLabels(): Effect.Effect<void, GitHubApiError, GitHubApi> {
		return Effect.flatMap(GitHubApi, (api) => api.removeAllPullRequestLabels({ pullRequest: this.ref })).pipe(
			Effect.withSpan('github.pull_request.remove_all_labels', {
				attributes: pullRequestSpanAttributes(this.ref),
			}),
		)
	}

	listCheckRuns(): Effect.Effect<GitHubCheckRuns, GitHubApiError, GitHubApi> {
		const ref = this.ref
		return Effect.gen(function* () {
			const api = yield* GitHubApi
			const pullRequest = yield* api.fetchPullRequest({ pullRequest: ref })
			return yield* api.listCheckRunsForRef({ pullRequest: ref, sha: pullRequest.headSha })
		}).pipe(
			Effect.withSpan('github.pull_request.list_check_runs', {
				attributes: pullRequestSpanAttributes(this.ref),
			}),
		)
	}

	listCheckRunsForRef(sha: string): Effect.Effect<GitHubCheckRuns, GitHubApiError, GitHubApi> {
		return Effect.flatMap(GitHubApi, (api) => api.listCheckRunsForRef({ pullRequest: this.ref, sha })).pipe(
			Effect.withSpan('github.pull_request.list_check_runs_for_ref', {
				attributes: pullRequestSpanAttributes(this.ref),
			}),
		)
	}

	close(): Effect.Effect<GitHubPullRequestInfo, GitHubApiError, GitHubApi> {
		return Effect.flatMap(GitHubApi, (api) => api.closePullRequest({ pullRequest: this.ref })).pipe(
			Effect.withSpan('github.pull_request.close', { attributes: pullRequestSpanAttributes(this.ref) }),
		)
	}

	reopen(): Effect.Effect<GitHubPullRequestInfo, GitHubApiError, GitHubApi> {
		return Effect.flatMap(GitHubApi, (api) => api.reopenPullRequest({ pullRequest: this.ref })).pipe(
			Effect.withSpan('github.pull_request.reopen', { attributes: pullRequestSpanAttributes(this.ref) }),
		)
	}

	merge(options: GitHubMergeOptions): Effect.Effect<GitHubMergeResult, GitHubApiError, GitHubApi> {
		return Effect.flatMap(GitHubApi, (api) => api.mergePullRequest({ pullRequest: this.ref, ...options })).pipe(
			Effect.withSpan('github.pull_request.merge', { attributes: pullRequestSpanAttributes(this.ref) }),
		)
	}

	/** A user's access to this pull request's repository, such as the author of an event. */
	fetchUserAccess(login: string): Effect.Effect<GitHubAccessLevel, GitHubApiError, GitHubApi> {
		return Effect.flatMap(GitHubApi, (api) => api.fetchUserAccess({ repository: this.ref, login })).pipe(
			Effect.withSpan('github.pull_request.fetch_user_access', {
				attributes: pullRequestSpanAttributes(this.ref),
			}),
		)
	}
}

export class GitHubCheckRun extends Schema.TaggedClass<GitHubCheckRun>()('GitHubCheckRun', {
	ref: GitHubCheckRunRef,
}) {
	fetchInfo(): Effect.Effect<GitHubCheckRunInfo, GitHubApiError, GitHubApi> {
		return Effect.flatMap(GitHubApi, (api) => api.fetchCheckRun({ checkRun: this.ref })).pipe(
			Effect.withSpan('github.check_run.fetch_info', { attributes: checkRunSpanAttributes(this.ref) }),
		)
	}

	listAnnotations(): Effect.Effect<GitHubCheckAnnotations, GitHubApiError, GitHubApi> {
		return Effect.flatMap(GitHubApi, (api) => api.listCheckRunAnnotations({ checkRun: this.ref })).pipe(
			Effect.withSpan('github.check_run.list_annotations', { attributes: checkRunSpanAttributes(this.ref) }),
		)
	}

	resolveActionsJob(): Effect.Effect<GitHubActionsJob | null, GitHubApiError, GitHubApi> {
		return Effect.flatMap(GitHubApi, (api) => api.resolveActionsJob({ checkRun: this.ref })).pipe(
			Effect.withSpan('github.check_run.resolve_actions_job', { attributes: checkRunSpanAttributes(this.ref) }),
		)
	}
}

export class GitHubActionsJob extends Schema.TaggedClass<GitHubActionsJob>()('GitHubActionsJob', {
	ref: GitHubActionsJobRef,
}) {
	fetchInfo(): Effect.Effect<GitHubActionsJobInfo, GitHubApiError, GitHubApi> {
		return Effect.flatMap(GitHubApi, (api) => api.fetchActionsJob({ job: this.ref })).pipe(
			Effect.withSpan('github.actions_job.fetch_info', { attributes: actionsJobSpanAttributes(this.ref) }),
		)
	}

	downloadLog(): Effect.Effect<string, GitHubApiError, GitHubApi> {
		return Effect.flatMap(GitHubApi, (api) => api.downloadActionsJobLog({ job: this.ref })).pipe(
			Effect.withSpan('github.actions_job.download_log', { attributes: actionsJobSpanAttributes(this.ref) }),
		)
	}
}

export class GitHubIssueComment extends Schema.TaggedClass<GitHubIssueComment>()('GitHubIssueComment', {
	ref: GitHubIssueCommentRef,
	body: Schema.String,
	url: Schema.String,
	author: Schema.NullOr(GitHubParticipant),
}) {
	update(content: GitHubContent): Effect.Effect<GitHubComment, GitHubApiError, GitHubApi> {
		return Effect.flatMap(GitHubApi, (api) => api.updateComment({ comment: this.ref, content })).pipe(
			Effect.withSpan('github.comment.update'),
		)
	}

	delete(): Effect.Effect<void, GitHubApiError, GitHubApi> {
		return Effect.flatMap(GitHubApi, (api) => api.deleteComment({ comment: this.ref })).pipe(
			Effect.withSpan('github.comment.delete'),
		)
	}

	addReaction(reaction: GitHubReaction): Effect.Effect<void, GitHubApiError, GitHubApi> {
		return Effect.flatMap(GitHubApi, (api) => api.addReaction({ target: GitHubReactionTarget.cases.Comment.make({ comment: this.ref }), reaction })).pipe(
			Effect.withSpan('github.comment.add_reaction'),
		)
	}

	removeReaction(reaction: GitHubReaction): Effect.Effect<void, GitHubApiError, GitHubApi> {
		return Effect.flatMap(GitHubApi, (api) => api.removeReaction({ target: GitHubReactionTarget.cases.Comment.make({ comment: this.ref }), reaction })).pipe(
			Effect.withSpan('github.comment.remove_reaction'),
		)
	}
}

export class GitHubReviewComment extends Schema.TaggedClass<GitHubReviewComment>()('GitHubReviewComment', {
	ref: GitHubReviewCommentRef,
	nodeId: Schema.NonEmptyString,
	body: Schema.String,
	url: Schema.String,
	author: Schema.NullOr(GitHubParticipant),
	reviewId: Schema.NullOr(GitHubId),
	path: Schema.String,
	commitId: Schema.String,
	originalCommitId: Schema.String,
	diffHunk: Schema.String,
	inReplyToId: Schema.optionalKey(Schema.NullOr(GitHubId)),
	line: Schema.optionalKey(Schema.NullOr(GitHubDiffLine)),
	startLine: Schema.optionalKey(Schema.NullOr(GitHubDiffLine)),
	side: Schema.optionalKey(Schema.Literals(['LEFT', 'RIGHT'])),
}) {
	reply(content: GitHubContent): Effect.Effect<GitHubReviewComment, GitHubApiError, GitHubApi> {
		return Effect.flatMap(GitHubApi, (api) =>
			api.replyToReviewComment({ pullRequest: this.ref.pullRequest, comment: this.ref, content }),
		).pipe(Effect.withSpan('github.review_comment.reply'))
	}

	update(content: GitHubContent): Effect.Effect<GitHubComment, GitHubApiError, GitHubApi> {
		return Effect.flatMap(GitHubApi, (api) => api.updateComment({ comment: this.ref, content })).pipe(
			Effect.withSpan('github.review_comment.update'),
		)
	}

	delete(): Effect.Effect<void, GitHubApiError, GitHubApi> {
		return Effect.flatMap(GitHubApi, (api) => api.deleteComment({ comment: this.ref })).pipe(
			Effect.withSpan('github.review_comment.delete'),
		)
	}

	addReaction(reaction: GitHubReaction): Effect.Effect<void, GitHubApiError, GitHubApi> {
		return Effect.flatMap(GitHubApi, (api) => api.addReaction({ target: GitHubReactionTarget.cases.Comment.make({ comment: this.ref }), reaction })).pipe(
			Effect.withSpan('github.review_comment.add_reaction'),
		)
	}

	removeReaction(reaction: GitHubReaction): Effect.Effect<void, GitHubApiError, GitHubApi> {
		return Effect.flatMap(GitHubApi, (api) => api.removeReaction({ target: GitHubReactionTarget.cases.Comment.make({ comment: this.ref }), reaction })).pipe(
			Effect.withSpan('github.review_comment.remove_reaction'),
		)
	}
}

export const GitHubComment = Schema.Union([GitHubIssueComment, GitHubReviewComment])
export type GitHubComment = typeof GitHubComment.Type

export const GitHubIssueComments = Schema.Array(GitHubIssueComment)
export type GitHubIssueComments = typeof GitHubIssueComments.Type

export const GitHubReviews = Schema.Array(GitHubReview)
export type GitHubReviews = typeof GitHubReviews.Type

export const GitHubReviewComments = Schema.Array(GitHubReviewComment)
export type GitHubReviewComments = typeof GitHubReviewComments.Type

export const GitHubLabelsResult = Schema.Array(GitHubLabel)
export type GitHubLabelsResult = typeof GitHubLabelsResult.Type

export const GitHubPullRequestFiles = Schema.Array(GitHubPullRequestFile)
export type GitHubPullRequestFiles = typeof GitHubPullRequestFiles.Type

export const GitHubCommits = Schema.Array(GitHubCommit)
export type GitHubCommits = typeof GitHubCommits.Type

export const GitHubCheckRuns = Schema.Array(GitHubCheckRun)
export type GitHubCheckRuns = typeof GitHubCheckRuns.Type

export const GitHubCheckAnnotations = Schema.Array(GitHubCheckAnnotation)
export type GitHubCheckAnnotations = typeof GitHubCheckAnnotations.Type
