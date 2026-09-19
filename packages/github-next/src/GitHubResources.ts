import {
	type MailboxSubscriptionError,
	type MailboxSubscriptionResult,
	MailboxSubscriptions,
} from '@humanlayer/channels-delivery-next'
import { Effect, Schema } from 'effect'

import type { GitHubApiError } from './GitHubApi'
import { GitHubApi } from './GitHubApi'
import { GitHubId } from './GitHubIdentity'
import {
	type GitHubContent,
	GitHubIssueCommentRef,
	type GitHubIssueInfo,
	GitHubIssueRef,
	type GitHubPullRequestInfo,
	GitHubPullRequestRef,
	type GitHubReaction,
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
		return Effect.flatMap(GitHubApi, (api) => api.addReaction({ comment: this.ref, reaction })).pipe(
			Effect.withSpan('github.comment.add_reaction'),
		)
	}

	removeReaction(reaction: GitHubReaction): Effect.Effect<void, GitHubApiError, GitHubApi> {
		return Effect.flatMap(GitHubApi, (api) => api.removeReaction({ comment: this.ref, reaction })).pipe(
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
	line: Schema.optionalKey(Schema.NullOr(Schema.Number)),
	startLine: Schema.optionalKey(Schema.NullOr(Schema.Number)),
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
		return Effect.flatMap(GitHubApi, (api) => api.addReaction({ comment: this.ref, reaction })).pipe(
			Effect.withSpan('github.review_comment.add_reaction'),
		)
	}

	removeReaction(reaction: GitHubReaction): Effect.Effect<void, GitHubApiError, GitHubApi> {
		return Effect.flatMap(GitHubApi, (api) => api.removeReaction({ comment: this.ref, reaction })).pipe(
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
