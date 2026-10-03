import { describe, it } from '@effect/vitest'
import { ProviderWebhookEvent, ProviderWebhookResponse } from '@humanlayer/channels-delivery'
import { Effect, Schema } from 'effect'

import { appUserNotificationPayloads, makeLinearTestProvider, signedLinearInput } from './fixtures'

describe('Linear app notification admission', () => {
	it.effect('admits every documented action to its issue mailbox', ({ expect }) =>
		Effect.gen(function* () {
			for (const [index, payload] of appUserNotificationPayloads.entries()) {
				const result = yield* makeLinearTestProvider().handle(
					signedLinearInput(payload, payload.type, `notification-delivery-${index}`),
				)
				expect(Schema.is(ProviderWebhookEvent)(result)).toBe(true)
				if (Schema.is(ProviderWebhookEvent)(result)) {
					expect(result.event.resourceId).toBe(`linear:v1:issue:${payload.notification.issueId}`)
					expect(result.event.payload).toMatchObject({ type: 'AppUserNotification', action: payload.action })
				}
			}
		}),
	)

	it.effect('admits the agent-guide shape without webhook metadata or omitted nullable issue fields', ({ expect }) =>
		Effect.gen(function* () {
			const fixture = appUserNotificationPayloads[0]
			expect(fixture).not.toHaveProperty('webhookId')
			expect(fixture).not.toHaveProperty('webhookTimestamp')
			const { description: _description, ...issue } = fixture.notification.issue
			const payload = {
				...fixture,
				notification: { ...fixture.notification, issue },
			}
			const result = yield* makeLinearTestProvider().handle(
				signedLinearInput(payload, payload.type, 'agent-guide-delivery'),
			)
			expect(Schema.is(ProviderWebhookEvent)(result)).toBe(true)
		}),
	)

	it.effect('rejects a notification for another app user', ({ expect }) =>
		Effect.gen(function* () {
			const payload = { ...appUserNotificationPayloads[0], appUserId: 'another-app-user' }
			const result = yield* makeLinearTestProvider().handle(signedLinearInput(payload, payload.type))
			expect(result).toEqual(ProviderWebhookResponse.make({ status: 403, body: null, headers: {} }))
		}),
	)
})
