import { describe, it } from '@effect/vitest'
import { DeliveryAdmission, ProviderWebhookEvent, ProviderWebhookIgnored } from '@humanlayer/channels-delivery'
import { Effect } from 'effect'

import {
	makeGitHubTestProvider,
	pullRequestIssueCommentPayload,
	pullRequestPayload,
	pullRequestReviewCommentPayload,
	pullRequestReviewPayload,
	pullRequestReviewThreadPayload,
	signedGitHubInput,
} from './fixtures'

const provider = makeGitHubTestProvider('github-pr-test')

const expected = (event: string, payload: DeliveryAdmission['payload'], deliveryId: string) =>
	ProviderWebhookEvent.make({
		event: DeliveryAdmission.make({
			namespace: 'github-pr-test',
			provider: 'github',
			installationId: '100',
			resourceId: 'github:v1:200:pull-request:42',
			eventId: deliveryId,
			payload: { event, payload },
		}),
	})

describe('GitHub pull request admission', () => {
	it.effect('orders pull request lifecycle events in the pull request mailbox', ({ expect }) =>
		Effect.gen(function* () {
			for (const action of [
				'opened',
				'edited',
				'closed',
				'reopened',
				'synchronize',
				'review_requested',
				'review_request_removed',
				'assigned',
				'unassigned',
				'labeled',
				'unlabeled',
				'converted_to_draft',
				'ready_for_review',
			]) {
				const payload = pullRequestPayload(action)
				const deliveryId = `pr-${action}`
				expect(yield* provider.handle(signedGitHubInput('pull_request', payload, deliveryId))).toEqual(
					expected('pull_request', payload, deliveryId),
				)
			}
		}),
	)

	it.effect('orders reviews in the same pull request mailbox', ({ expect }) =>
		Effect.gen(function* () {
			for (const action of ['submitted', 'edited', 'dismissed']) {
				const payload = pullRequestReviewPayload(action)
				const deliveryId = `review-${action}`
				expect(yield* provider.handle(signedGitHubInput('pull_request_review', payload, deliveryId))).toEqual(
					expected('pull_request_review', payload, deliveryId),
				)
			}
		}),
	)

	it.effect('orders inline review comments in the same pull request mailbox', ({ expect }) =>
		Effect.gen(function* () {
			for (const action of ['created', 'edited', 'deleted']) {
				const payload = pullRequestReviewCommentPayload(action)
				const deliveryId = `comment-${action}`
				expect(
					yield* provider.handle(signedGitHubInput('pull_request_review_comment', payload, deliveryId)),
				).toEqual(expected('pull_request_review_comment', payload, deliveryId))
			}
		}),
	)

	it.effect('orders review thread events in the same pull request mailbox', ({ expect }) =>
		Effect.gen(function* () {
			for (const action of ['resolved', 'unresolved']) {
				const payload = pullRequestReviewThreadPayload(action)
				const deliveryId = `thread-${action}`
				expect(
					yield* provider.handle(signedGitHubInput('pull_request_review_thread', payload, deliveryId)),
				).toEqual(expected('pull_request_review_thread', payload, deliveryId))
			}
		}),
	)

	it.effect('uses the same mailbox for issue comments on the pull request', ({ expect }) =>
		Effect.gen(function* () {
			const payload = pullRequestIssueCommentPayload()
			expect(yield* provider.handle(signedGitHubInput('issue_comment', payload, 'conversation-1'))).toEqual(
				expected('issue_comment', payload, 'conversation-1'),
			)
		}),
	)

	it.effect('ignores unsupported pull request actions', ({ expect }) =>
		Effect.gen(function* () {
			const payload = pullRequestPayload('auto_merge_enabled')
			expect(yield* provider.handle(signedGitHubInput('pull_request', payload, 'unsupported-1'))).toEqual(
				ProviderWebhookIgnored.make({}),
			)
		}),
	)
})
