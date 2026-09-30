/**
 * Where a GitHub delivery's output goes, saved before the callback runs so a later attempt or a remote
 * worker can reach the same issue or pull request. The delivery core stores these as opaque JSON; only
 * GitHub reads them.
 */
import type { DeliveryOperationKind } from '@humanlayer/channels-delivery-next'
import { Schema } from 'effect'

import { GitHubIssueCommentRef, GitHubIssueRef, GitHubPullRequestRef, GitHubReviewCommentRef } from './GitHubModels'

/** The version of {@link GitHubDeliveryDestination} and {@link GitHubActivationTarget}. Bump when either changes shape. */
export const gitHubPresentationVersion = 1

/** The issue or pull request a delivery replies on. */
export const GitHubDeliveryDestination = Schema.TaggedUnion({
	GitHubIssue: { issue: GitHubIssueRef },
	GitHubPullRequest: { pullRequest: GitHubPullRequestRef },
})
export type GitHubDeliveryDestination = typeof GitHubDeliveryDestination.Type

/**
 * What started a delivery, for reactions: the mentioning comment, or the issue or pull request itself
 * when it was opened or mentioned the bot in its body. Subscribed batches have none.
 */
export const GitHubActivationTarget = Schema.TaggedUnion({
	GitHubIssue: { issue: GitHubIssueRef },
	GitHubPullRequest: { pullRequest: GitHubPullRequestRef },
	GitHubIssueComment: { comment: GitHubIssueCommentRef },
	GitHubReviewComment: { comment: GitHubReviewCommentRef },
})
export type GitHubActivationTarget = typeof GitHubActivationTarget.Type

/** JSON codecs for storing the destination and activation target. */
export const GitHubDeliveryDestinationJson = Schema.toCodecJson(GitHubDeliveryDestination)
export const GitHubActivationTargetJson = Schema.toCodecJson(GitHubActivationTarget)

/**
 * The output operations a GitHub issue or pull request supports. `SetActivity` (the `eyes` reaction)
 * is left out until GitHub has an output processor that shows it.
 */
export const gitHubDiscussionSupportedOperations: ReadonlyArray<DeliveryOperationKind> = [
	'PresentOutcome',
	'CreateMessage',
	'UpdateMessage',
	'DeleteMessage',
	'SetMessageReaction',
	'RenderPlan',
	'AddExternalLink',
]
