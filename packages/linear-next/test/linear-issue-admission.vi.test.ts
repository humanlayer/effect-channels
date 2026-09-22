import { describe, it } from '@effect/vitest'
import { DeliveryAdmission, ProviderWebhookEvent } from '@humanlayer/channels-delivery-next'
import { Effect } from 'effect'

import { issueCreatePayload, linearOrganizationId, makeLinearTestProvider, signedLinearInput } from './fixtures'

describe('Linear issue admission', () => {
	it.effect('addresses Issue.create to its stable workspace issue mailbox', ({ expect }) =>
		Effect.gen(function* () {
			const outcome = yield* makeLinearTestProvider('linear-admission-test').handle(signedLinearInput())
			expect(outcome).toEqual(
				ProviderWebhookEvent.make({
					event: DeliveryAdmission.make({
						namespace: 'linear-admission-test',
						provider: 'linear',
						installationId: linearOrganizationId,
						resourceId: 'linear:v1:issue:b33fb278-fbe0-45e4-b4eb-94b0839f51b9',
						eventId: '6d601ea0-cafe-4e33-8db1-d7c21fc6a773',
						payload: issueCreatePayload,
					}),
				}),
			)
		}),
	)
})
