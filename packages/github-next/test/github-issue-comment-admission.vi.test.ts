import { describe, it } from '@effect/vitest'
import {
	DeliveryAdmission,
	ProviderWebhookEvent,
	ProviderWebhookIgnored,
	WebhookAuthenticationError,
	WebhookPayloadInvalidError,
} from '@humanlayer/channels-delivery-next'
import { Effect } from 'effect'
import { Headers } from 'effect/unstable/http'

import { issueCommentPayload, makeGitHubTestProvider, signedGitHubBody, signedGitHubInput } from './fixtures'

const provider = makeGitHubTestProvider('github-test')

const expectedEvent = (
	payload: ReturnType<typeof issueCommentPayload>,
	deliveryId: string,
	kind: 'issue' | 'pull-request',
) =>
	ProviderWebhookEvent.make({
		event: DeliveryAdmission.make({
			namespace: 'github-test',
			provider: 'github',
			installationId: '100',
			resourceId: `github:v1:200:${kind}:${payload.issue.number}`,
			eventId: deliveryId,
			payload: { event: 'issue_comment', payload },
		}),
	})

describe('GitHub issue comment admission', () => {
	it.effect('admits an issue comment', ({ expect }) =>
		Effect.gen(function* () {
			for (const action of ['created', 'edited', 'deleted']) {
				const payload = issueCommentPayload({ action })
				const deliveryId = `issue-comment-${action}`
				expect(yield* provider.handle(signedGitHubInput('issue_comment', payload, deliveryId))).toEqual(
					expectedEvent(payload, deliveryId, 'issue'),
				)
			}
		}),
	)

	it.effect('uses one resource identity for comments on the same issue', ({ expect }) =>
		Effect.gen(function* () {
			const payload = issueCommentPayload()
			const first = yield* provider.handle(signedGitHubInput('issue_comment', payload, 'delivery-1'))
			const second = yield* provider.handle(signedGitHubInput('issue_comment', payload, 'delivery-2'))
			expect(first).toEqual(expectedEvent(payload, 'delivery-1', 'issue'))
			expect(second).toEqual(expectedEvent(payload, 'delivery-2', 'issue'))
		}),
	)

	it.effect('identifies an issue comment on a pull request as a pull-request resource', ({ expect }) =>
		Effect.gen(function* () {
			const payload = issueCommentPayload({ pullRequest: true })
			expect(yield* provider.handle(signedGitHubInput('issue_comment', payload))).toEqual(
				expectedEvent(payload, 'delivery-1', 'pull-request'),
			)
		}),
	)

	it.effect('rejects missing authentication headers', ({ expect }) =>
		Effect.gen(function* () {
			const error = yield* Effect.flip(provider.handle({ headers: Headers.empty, body: new Uint8Array() }))
			expect(error).toEqual(WebhookAuthenticationError.make({ reason: 'invalid_signature_headers' }))
		}),
	)

	it.effect('rejects an invalid signature', ({ expect }) =>
		Effect.gen(function* () {
			const input = signedGitHubInput('issue_comment', issueCommentPayload())
			const error = yield* Effect.flip(
				provider.handle({
					...input,
					headers: Headers.set(input.headers, 'x-hub-signature-256', `sha256=${'0'.repeat(64)}`),
				}),
			)
			expect(error).toEqual(WebhookAuthenticationError.make({ reason: 'invalid_signature' }))
		}),
	)

	it.effect('rejects malformed JSON for a supported event', ({ expect }) =>
		Effect.gen(function* () {
			const body = new TextEncoder().encode('{')
			const error = yield* Effect.flip(provider.handle(signedGitHubBody('issue_comment', body)))
			expect(error).toEqual(WebhookPayloadInvalidError.make({ reason: 'invalid_json' }))
		}),
	)

	it.effect('rejects a malformed issue comment payload', ({ expect }) =>
		Effect.gen(function* () {
			const error = yield* Effect.flip(provider.handle(signedGitHubInput('issue_comment', { action: 'created' })))
			expect(error).toEqual(WebhookPayloadInvalidError.make({ reason: 'invalid_issue_comment' }))
		}),
	)

	it.effect('ignores a valid unsupported event after verifying its signature', ({ expect }) =>
		Effect.gen(function* () {
			expect(yield* provider.handle(signedGitHubInput('push', { ref: 'refs/heads/main' }))).toEqual(
				ProviderWebhookIgnored.make({}),
			)
		}),
	)
})
