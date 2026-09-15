/** GitHub's post-admission event processor. */
import {
	type DeliveryAdmission,
	ProviderEventExecutionFailed,
	ProviderEventHandled,
	ProviderEventIgnored,
	ProviderEventInvalid,
	type ProviderEventProcessor,
} from '@humanlayer/channels-delivery-next'
import { Effect, Match, Predicate, Schema } from 'effect'

import type {
	GitHubIssueCommentWebhook,
	GitHubIssuesWebhook,
	GitHubPullRequestReviewCommentWebhook,
	GitHubPullRequestReviewThreadWebhook,
	GitHubPullRequestReviewWebhook,
	GitHubPullRequestWebhook,
} from './GitHubWebhookSchemas'
import { GitHubSupportedWebhook } from './GitHubWebhookSchemas'

type GitHubEventHandler<A, E, R> = (event: A) => Effect.Effect<void, E, R>

export const RetryabilityMetadata = Schema.Struct({
	retryability: Schema.Literals(['retryable', 'non_retryable']),
})
export type RetryabilityMetadata = typeof RetryabilityMetadata.Type

export type GitHubEventProcessorOptions<E, R> = {
	readonly namespace: string
	readonly handlers: {
		readonly onIssue?: GitHubEventHandler<GitHubIssuesWebhook, E, R>
		readonly onIssueComment?: GitHubEventHandler<GitHubIssueCommentWebhook, E, R>
		readonly onPullRequest?: GitHubEventHandler<GitHubPullRequestWebhook, E, R>
		readonly onPullRequestReview?: GitHubEventHandler<GitHubPullRequestReviewWebhook, E, R>
		readonly onPullRequestReviewComment?: GitHubEventHandler<GitHubPullRequestReviewCommentWebhook, E, R>
		readonly onPullRequestReviewThread?: GitHubEventHandler<GitHubPullRequestReviewThreadWebhook, E, R>
	}
}

const decodeGitHubWebhook = (admission: DeliveryAdmission) =>
	Schema.decodeUnknownEffect(GitHubSupportedWebhook)(admission.payload, { onExcessProperty: 'preserve' }).pipe(
		Effect.tapError((error) => Effect.logError('Stored GitHub webhook could not be decoded', error)),
		Effect.mapError(() => ProviderEventInvalid.make({ provider: 'github', reason: 'invalid_payload' })),
	)

const processGitHubEvent = <E, R>(options: GitHubEventProcessorOptions<E, R>) =>
	Effect.fn('github.process_event')(function* (admission: DeliveryAdmission) {
		const webhook = yield* decodeGitHubWebhook(admission)
		const callback = Match.value(webhook).pipe(
			Match.when({ event: 'issues' }, ({ payload }) => ({
				name: 'onIssue',
				span: 'github.process_issue',
				effect: options.handlers.onIssue?.(payload),
			})),
			Match.when({ event: 'issue_comment' }, ({ payload }) => ({
				name: 'onIssueComment',
				span: 'github.process_issue_comment',
				effect: options.handlers.onIssueComment?.(payload),
			})),
			Match.when({ event: 'pull_request' }, ({ payload }) => ({
				name: 'onPullRequest',
				span: 'github.process_pull_request',
				effect: options.handlers.onPullRequest?.(payload),
			})),
			Match.when({ event: 'pull_request_review' }, ({ payload }) => ({
				name: 'onPullRequestReview',
				span: 'github.process_pull_request_review',
				effect: options.handlers.onPullRequestReview?.(payload),
			})),
			Match.when({ event: 'pull_request_review_comment' }, ({ payload }) => ({
				name: 'onPullRequestReviewComment',
				span: 'github.process_pull_request_review_comment',
				effect: options.handlers.onPullRequestReviewComment?.(payload),
			})),
			Match.when({ event: 'pull_request_review_thread' }, ({ payload }) => ({
				name: 'onPullRequestReviewThread',
				span: 'github.process_pull_request_review_thread',
				effect: options.handlers.onPullRequestReviewThread?.(payload),
			})),
			Match.exhaustive,
		)

		if (Predicate.isUndefined(callback.effect)) {
			return ProviderEventIgnored.make({ reason: 'callback_not_configured' })
		}

		return yield* callback.effect.pipe(
			Effect.tapError((error) => Effect.logError(`GitHub ${callback.name} callback failed`, error)),
			Effect.mapError((error) =>
				ProviderEventExecutionFailed.make({
					provider: 'github',
					retryable: !Schema.is(RetryabilityMetadata)(error) || error.retryability === 'retryable',
					safeCode: 'callback_failed',
				}),
			),
			Effect.as(ProviderEventHandled.make({})),
			Effect.withSpan(callback.span),
		)
	})

export const makeGitHubEventProcessor = <E = never, R = never>(
	options: GitHubEventProcessorOptions<E, R>,
): ProviderEventProcessor<R> => ({
	namespace: options.namespace,
	providerName: 'github',
	process: processGitHubEvent(options),
})
