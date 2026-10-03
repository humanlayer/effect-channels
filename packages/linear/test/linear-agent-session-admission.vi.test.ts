import { describe, it } from '@effect/vitest'
import {
	ProviderWebhookEvent,
	ProviderWebhookResponse,
	WebhookPayloadInvalidError,
} from '@humanlayer/channels-delivery'
import { Effect, Schema } from 'effect'

import { LinearStoredAgentSessionWebhook } from '../src/LinearWebhookSchemas'
import { agentSessionPayloads, makeLinearTestProvider, signedLinearInput } from './fixtures'

describe('Linear Agent Session webhook admission', () => {
	it.effect('admits created and prompted events to one session mailbox with logical event IDs', ({ expect }) =>
		Effect.gen(function* () {
			const created = yield* makeLinearTestProvider().handle(
				signedLinearInput(agentSessionPayloads[0], 'AgentSessionEvent', 'delivery-created'),
			)
			const prompted = yield* makeLinearTestProvider().handle(
				signedLinearInput(agentSessionPayloads[1], 'AgentSessionEvent', 'delivery-prompted'),
			)
			expect(Schema.is(ProviderWebhookEvent)(created)).toBe(true)
			expect(Schema.is(ProviderWebhookEvent)(prompted)).toBe(true)
			if (Schema.is(ProviderWebhookEvent)(created) && Schema.is(ProviderWebhookEvent)(prompted)) {
				expect(created.event.resourceId).toBe('linear:v1:agent-session:71000000-0000-4000-8000-000000000001')
				expect(prompted.event.resourceId).toBe(created.event.resourceId)
				expect(created.event.eventId).toBe('agent-session-created:71000000-0000-4000-8000-000000000001')
				expect(prompted.event.eventId).toBe('agent-session-prompted:73000000-0000-4000-8000-000000000001')
				expect(Schema.is(LinearStoredAgentSessionWebhook)(created.event.payload)).toBe(true)
				if (Schema.is(LinearStoredAgentSessionWebhook)(created.event.payload)) {
					expect(created.event.payload.deliveryId).toBe('delivery-created')
					expect(created.event.payload.webhook.webhookId).toBe('70000000-0000-4000-8000-000000000001')
					expect(created.event.payload.deliveryId).not.toBe(created.event.payload.webhook.webhookId)
				}
			}
		}),
	)

	it.effect('keeps logical admission identity stable across transport delivery IDs', ({ expect }) =>
		Effect.gen(function* () {
			const first = yield* makeLinearTestProvider().handle(
				signedLinearInput(agentSessionPayloads[0], 'AgentSessionEvent', 'transport-one'),
			)
			const replay = yield* makeLinearTestProvider().handle(
				signedLinearInput(agentSessionPayloads[0], 'AgentSessionEvent', 'transport-two'),
			)
			expect(Schema.is(ProviderWebhookEvent)(first)).toBe(true)
			expect(Schema.is(ProviderWebhookEvent)(replay)).toBe(true)
			if (Schema.is(ProviderWebhookEvent)(first) && Schema.is(ProviderWebhookEvent)(replay))
				expect(replay.event.eventId).toBe(first.event.eventId)
			if (
				Schema.is(ProviderWebhookEvent)(first) &&
				Schema.is(ProviderWebhookEvent)(replay) &&
				Schema.is(LinearStoredAgentSessionWebhook)(first.event.payload) &&
				Schema.is(LinearStoredAgentSessionWebhook)(replay.event.payload)
			) {
				expect(first.event.payload.deliveryId).toBe('transport-one')
				expect(replay.event.payload.deliveryId).toBe('transport-two')
				expect(replay.event.payload.webhook.webhookId).toBe(first.event.payload.webhook.webhookId)
			}
		}),
	)

	it.effect('rejects nested session identity mismatches before admission', ({ expect }) =>
		Effect.gen(function* () {
			const payload = structuredClone(agentSessionPayloads[0])
			payload.agentSession.appUserId = 'different-app-user'
			const outcome = yield* makeLinearTestProvider()
				.handle(signedLinearInput(payload, payload.type))
				.pipe(Effect.flip)
			expect(outcome).toEqual(WebhookPayloadInvalidError.make({ reason: 'session_identity_mismatch' }))
			const foreign = structuredClone(agentSessionPayloads[0])
			foreign.appUserId = 'different-app-user'
			const response = yield* makeLinearTestProvider().handle(signedLinearInput(foreign, foreign.type))
			expect(response).toEqual(ProviderWebhookResponse.make({ status: 403, body: null, headers: {} }))
		}),
	)
})
