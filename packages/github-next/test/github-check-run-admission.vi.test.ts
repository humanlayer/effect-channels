import { describe, it } from '@effect/vitest'
import {
	DeliveryAdmission,
	ProviderWebhookEvent,
	ProviderWebhookEvents,
	ProviderWebhookIgnored,
} from '@humanlayer/channels-delivery-next'
import { Effect } from 'effect'

import { checkRunPayload, makeGitHubTestProvider, signedGitHubInput } from './fixtures'

const provider = makeGitHubTestProvider('github-check-test')

describe('GitHub check run admission', () => {
	it.effect('admits a completed check associated with exactly one pull request', ({ expect }) =>
		Effect.gen(function* () {
			const payload = checkRunPayload([42])
			expect(yield* provider.handle(signedGitHubInput('check_run', payload, 'check-delivery'))).toEqual(
				ProviderWebhookEvent.make({
					event: DeliveryAdmission.make({
						namespace: 'github-check-test',
						provider: 'github',
						installationId: '100',
						resourceId: 'github:v1:200:pull-request:42',
						eventId: 'check-delivery:pull-request:200:42',
						payload: { event: 'check_run', payload },
					}),
				}),
			)
		}),
	)

	it.effect('fans a check out to every associated pull request mailbox', ({ expect }) =>
		Effect.gen(function* () {
			const payload = checkRunPayload([42, 57])
			const admissionFor = (number: number) =>
				DeliveryAdmission.make({
					namespace: 'github-check-test',
					provider: 'github',
					installationId: '100',
					resourceId: `github:v1:200:pull-request:${number}`,
					eventId: `check-delivery:pull-request:200:${number}`,
					payload: { event: 'check_run', payload },
				})
			expect(yield* provider.handle(signedGitHubInput('check_run', payload, 'check-delivery'))).toEqual(
				ProviderWebhookEvents.make({
					events: [admissionFor(42), admissionFor(57)],
				}),
			)
		}),
	)

	it.effect('ignores a completed check with no pull request association', ({ expect }) =>
		Effect.gen(function* () {
			expect(yield* provider.handle(signedGitHubInput('check_run', checkRunPayload([])))).toEqual(
				ProviderWebhookIgnored.make({}),
			)
		}),
	)
})
