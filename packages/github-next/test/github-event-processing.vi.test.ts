import { describe, it } from '@effect/vitest'
import {
	DeliveryAdmission,
	processProviderEvent,
	ProviderEventExecutionFailed,
	ProviderEventHandled,
	ProviderEventIgnored,
	ProviderEventInvalid,
} from '@humanlayer/channels-delivery-next'
import { Effect } from 'effect'
import { vi } from 'vitest'

import { makeGitHubEventProcessor } from '../src/GitHubEventProcessor'
import {
	issueCommentPayload,
	issuePayload,
	admitStoredGitHubWebhook,
	pullRequestPayload,
	pullRequestReviewCommentPayload,
	pullRequestReviewPayload,
	pullRequestReviewThreadPayload,
} from './fixtures'

const admission = (event: string, payload: unknown) =>
	DeliveryAdmission.make({
		namespace: 'github-processing-test',
		provider: 'github',
		installationId: '100',
		resourceId: 'github:v1:200:pull-request:42',
		eventId: 'delivery-1',
		payload: { event, payload },
	})

describe('GitHub event processing', () => {
	it.effect('uses the callback configuration active when processing begins', ({ expect }) =>
		Effect.gen(function* () {
			const payload = issuePayload('opened')
			const queued = yield* admitStoredGitHubWebhook('github-processing-test', 'issues', payload)
			const callbackA = vi.fn(() => Effect.void)
			const callbackB = vi.fn(() => Effect.void)
			let activeProcessors = [
				makeGitHubEventProcessor({
					namespace: 'github-processing-test',
					handlers: { onIssue: callbackA },
				}),
			]
			activeProcessors = [
				makeGitHubEventProcessor({
					namespace: 'github-processing-test',
					handlers: { onIssue: callbackB },
				}),
			]

			expect(yield* processProviderEvent(activeProcessors)(queued)).toEqual(ProviderEventHandled.make({}))
			expect(callbackA).not.toHaveBeenCalled()
			expect(callbackB).toHaveBeenCalledOnce()
			expect(callbackB).toHaveBeenCalledWith(payload)
		}),
	)

	it.effect('routes every GitHub event family to its configured callback', ({ expect }) =>
		Effect.gen(function* () {
			const onIssue = vi.fn(() => Effect.void)
			const onIssueComment = vi.fn(() => Effect.void)
			const onPullRequest = vi.fn(() => Effect.void)
			const onPullRequestReview = vi.fn(() => Effect.void)
			const onPullRequestReviewComment = vi.fn(() => Effect.void)
			const onPullRequestReviewThread = vi.fn(() => Effect.void)
			const processor = makeGitHubEventProcessor({
				namespace: 'github-processing-test',
				handlers: {
					onIssue,
					onIssueComment,
					onPullRequest,
					onPullRequestReview,
					onPullRequestReviewComment,
					onPullRequestReviewThread,
				},
			})
			const cases = [
				['issues', issuePayload('opened')],
				['issue_comment', issueCommentPayload()],
				['pull_request', pullRequestPayload()],
				['pull_request_review', pullRequestReviewPayload()],
				['pull_request_review_comment', pullRequestReviewCommentPayload()],
				['pull_request_review_thread', pullRequestReviewThreadPayload()],
			] as const

			for (const [event, payload] of cases) {
				expect(yield* processor.process(admission(event, payload))).toEqual(ProviderEventHandled.make({}))
			}
			expect(onIssue).toHaveBeenCalledOnce()
			expect(onIssueComment).toHaveBeenCalledOnce()
			expect(onPullRequest).toHaveBeenCalledOnce()
			expect(onPullRequestReview).toHaveBeenCalledOnce()
			expect(onPullRequestReviewComment).toHaveBeenCalledOnce()
			expect(onPullRequestReviewThread).toHaveBeenCalledOnce()
		}),
	)

	it.effect('ignores an event when its callback is not configured', ({ expect }) =>
		Effect.gen(function* () {
			const processor = makeGitHubEventProcessor({ namespace: 'github-processing-test', handlers: {} })
			expect(yield* processor.process(admission('issues', issuePayload('opened')))).toEqual(
				ProviderEventIgnored.make({ reason: 'callback_not_configured' }),
			)
		}),
	)

	it.effect('rejects an invalid stored payload', ({ expect }) =>
		Effect.gen(function* () {
			const processor = makeGitHubEventProcessor({ namespace: 'github-processing-test', handlers: {} })
			const error = yield* Effect.flip(processor.process(admission('issues', { action: 'opened' })))
			expect(error).toEqual(ProviderEventInvalid.make({ provider: 'github', reason: 'invalid_payload' }))
		}),
	)

	it.effect('narrows callback failures and preserves non-retryable metadata', ({ expect }) =>
		Effect.gen(function* () {
			const processor = makeGitHubEventProcessor({
				namespace: 'github-processing-test',
				handlers: {
					onIssue: () => Effect.fail({ retryability: 'non_retryable' as const }),
				},
			})
			const error = yield* Effect.flip(processor.process(admission('issues', issuePayload('opened'))))
			expect(error).toEqual(
				ProviderEventExecutionFailed.make({
					provider: 'github',
					retryable: false,
					safeCode: 'callback_failed',
				}),
			)
		}),
	)

	it.effect('treats callback failures without metadata as retryable', ({ expect }) =>
		Effect.gen(function* () {
			const processor = makeGitHubEventProcessor({
				namespace: 'github-processing-test',
				handlers: { onIssue: () => Effect.fail('callback failed') },
			})
			const error = yield* Effect.flip(processor.process(admission('issues', issuePayload('opened'))))
			expect(error).toEqual(
				ProviderEventExecutionFailed.make({
					provider: 'github',
					retryable: true,
					safeCode: 'callback_failed',
				}),
			)
		}),
	)
})
