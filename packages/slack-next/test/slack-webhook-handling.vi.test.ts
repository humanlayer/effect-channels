import { NodeCrypto } from '@effect/platform-node'
import { describe, it } from '@effect/vitest'
import {
	DeliveryAdmission,
	ProviderWebhookEvent,
	ProviderWebhookIgnored,
	ProviderWebhookResponse,
	WebhookAuthenticationError,
	WebhookPayloadInvalidError,
} from '@humanlayer/channels-delivery-next'
import { Effect, Redacted } from 'effect'
import { Headers } from 'effect/unstable/http'

import { SlackReactionThreadResolver } from '../src/SlackReactionThreadResolver'
import { makeSlackWebhookProvider } from '../src/SlackWebhookProvider'
import { signedSlackInput } from './fixtures'

const signingSecret = 'webhook-test-secret'
const provider = makeSlackWebhookProvider({
	namespace: 'webhook-test',
	signingSecret: Redacted.make(signingSecret),
})
const handle = (input: Parameters<typeof provider.handle>[0]) =>
	provider.handle(input).pipe(
		Effect.provideService(SlackReactionThreadResolver, {
			resolve: () => Effect.die(new Error('This webhook must not resolve reaction threads')),
		}),
		Effect.provide(NodeCrypto.layer),
	)

describe('Slack webhook handling', () => {
	it.effect('returns the URL verification challenge', ({ expect }) =>
		Effect.gen(function* () {
			const outcome = yield* handle(
				signedSlackInput(signingSecret, { type: 'url_verification', challenge: 'challenge-value' }),
			)
			expect(outcome).toEqual(
				ProviderWebhookResponse.make({
					status: 200,
					body: new TextEncoder().encode('challenge-value'),
					headers: { 'content-type': 'text/plain; charset=utf-8' },
				}),
			)
		}),
	)

	it.effect('rejects missing signature headers', ({ expect }) =>
		Effect.gen(function* () {
			const error = yield* Effect.flip(handle({ headers: Headers.empty, body: new Uint8Array() }))
			expect(error).toEqual(WebhookAuthenticationError.make({ reason: 'invalid_signature_headers' }))
		}),
	)

	it.effect('rejects an invalid signature', ({ expect }) =>
		Effect.gen(function* () {
			const input = signedSlackInput(signingSecret, { type: 'unknown' })
			const error = yield* Effect.flip(
				handle({
					...input,
					headers: Headers.set(input.headers, 'x-slack-signature', `v0=${'0'.repeat(64)}`),
				}),
			)
			expect(error).toEqual(WebhookAuthenticationError.make({ reason: 'invalid_signature' }))
		}),
	)

	it.effect('rejects malformed JSON after signature verification', ({ expect }) =>
		Effect.gen(function* () {
			const body = new TextEncoder().encode('{')
			const signed = signedSlackInput(signingSecret, {}, '0')
			const bodyText = new TextDecoder().decode(body)
			const signature = createHmac('sha256', signingSecret).update(`v0:0:${bodyText}`).digest('hex')
			const error = yield* Effect.flip(
				handle({
					body,
					headers: Headers.set(signed.headers, 'x-slack-signature', `v0=${signature}`),
				}),
			)
			expect(error).toEqual(WebhookPayloadInvalidError.make({ reason: 'invalid_json' }))
		}),
	)

	it.effect('ignores a valid unsupported event', ({ expect }) =>
		Effect.gen(function* () {
			const outcome = yield* handle(
				signedSlackInput(signingSecret, {
					type: 'event_callback',
					team_id: 'T_TEST',
					event_id: 'Ev_UNSUPPORTED',
					event_time: 1_700_000_000,
					event: { type: 'channel_created' },
				}),
			)
			expect(outcome).toEqual(ProviderWebhookIgnored.make({}))
		}),
	)

	it.effect('rejects a malformed supported event', ({ expect }) =>
		Effect.gen(function* () {
			const error = yield* Effect.flip(
				handle(
					signedSlackInput(signingSecret, {
						type: 'event_callback',
						team_id: 'T_TEST',
						event_id: 'Ev_BAD_MENTION',
						event_time: 1_700_000_000,
						event: { type: 'app_mention' },
					}),
				),
			)
			expect(error).toEqual(WebhookPayloadInvalidError.make({ reason: 'invalid_app_mention' }))
		}),
	)

	it.effect('admits agent session stopped events to their Slack thread', ({ expect }) =>
		Effect.gen(function* () {
			const payload = {
				type: 'event_callback',
				team_id: 'T_TEST',
				event_id: 'Ev_STOPPED',
				event_time: 1_700_000_000,
				event: {
					type: 'agent_session_stopped',
					channel: 'C_TEST',
					thread_ts: '1700000000.000001',
					user: 'U_TEST',
					event_ts: '1700000002.000001',
					streaming_message_ts: ['1700000001.000001'],
				},
			} as const
			expect(yield* handle(signedSlackInput(signingSecret, payload))).toEqual(
				ProviderWebhookEvent.make({
					admission: DeliveryAdmission.make({
						namespace: 'webhook-test',
						provider: 'slack',
						installationId: 'T_TEST',
						resourceId: 'slack:v1:T_TEST:C_TEST:1700000000.000001',
						eventId: 'Ev_STOPPED',
						payload,
					}),
				}),
			)
		}),
	)
})
import { createHmac } from 'node:crypto'
