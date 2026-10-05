import { describe, it } from '@effect/vitest'
import { DeliveryAdmission, ProviderWebhookEvent } from '@humanlayer/channels-delivery'
import { Effect } from 'effect'

import { issuePayload, makeGitHubTestProvider, signedGitHubInput } from './fixtures'

const provider = makeGitHubTestProvider('github-issue-test')

describe('GitHub issue admission', () => {
	it.effect('orders every supported issue action in the issue mailbox', ({ expect }) =>
		Effect.gen(function* () {
			for (const action of [
				'opened',
				'edited',
				'closed',
				'reopened',
				'assigned',
				'unassigned',
				'labeled',
				'unlabeled',
			]) {
				const payload = issuePayload(action)
				const deliveryId = `issue-${action}`
				const outcome = yield* provider.handle(signedGitHubInput('issues', payload, deliveryId))
				expect(outcome).toEqual(
					ProviderWebhookEvent.make({
						event: DeliveryAdmission.make({
							namespace: 'github-issue-test',
							provider: 'github',
							installationId: '100',
							resourceId: 'github:v1:200:issue:42',
							eventId: deliveryId,
							payload: { event: 'issues', payload },
						}),
					}),
				)
			}
		}),
	)
})
