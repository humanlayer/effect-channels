import { NodeCrypto } from '@effect/platform-node'
import { describe, it } from '@effect/vitest'
import { DeliveryAdmission, ProviderWebhookEvent, ProviderWebhookIgnored } from '@humanlayer/channels-delivery-next'
import { Effect, Redacted } from 'effect'

import { SlackReactionThreadResolver } from '../src/SlackReactionThreadResolver'
import { makeSlackWebhookProvider } from '../src/SlackWebhookProvider'
import { signedSlackInput } from './fixtures'

const signingSecret = 'message-test-secret'
const message = (channel: string, channelType: 'channel' | 'im' | 'mpim', threadTs?: string) => ({
	type: 'event_callback',
	team_id: 'T_TEST',
	event_id: `Ev_${channelType}`,
	event_time: 1_700_000_000,
	event: {
		type: 'message',
		user: 'U_TEST',
		text: 'hello agent',
		ts: '1700000001.000001',
		...(threadTs === undefined ? {} : { thread_ts: threadTs }),
		channel,
		channel_type: channelType,
	},
})

const handleMessage = (payload: unknown) =>
	makeSlackWebhookProvider({ namespace: 'message-test', signingSecret: Redacted.make(signingSecret) })
		.handle(signedSlackInput(signingSecret, payload))
		.pipe(
			Effect.provideService(SlackReactionThreadResolver, {
				resolve: () => Effect.die(new Error('Messages must not resolve reaction threads')),
			}),
			Effect.provide(NodeCrypto.layer),
		)

const expectedEvent = (payload: ReturnType<typeof message>, resourceTs: string) =>
	ProviderWebhookEvent.make({
		admission: DeliveryAdmission.make({
			namespace: 'message-test',
			provider: 'slack',
			installationId: 'T_TEST',
			resourceId: `slack:v1:T_TEST:${payload.event.channel}:${resourceTs}`,
			eventId: payload.event_id,
			payload,
		}),
	})

describe('Slack message admission', () => {
	it.effect('admits an ordinary channel message', ({ expect }) =>
		Effect.gen(function* () {
			const payload = message('C_PUBLIC', 'channel')
			expect(yield* handleMessage(payload)).toEqual(expectedEvent(payload, payload.event.ts))
		}),
	)

	it.effect('admits a direct message', ({ expect }) =>
		Effect.gen(function* () {
			const payload = message('D_DIRECT', 'im')
			expect(yield* handleMessage(payload)).toEqual(expectedEvent(payload, payload.event.ts))
		}),
	)

	it.effect('admits a multi-person direct message', ({ expect }) =>
		Effect.gen(function* () {
			const payload = message('G_GROUP', 'mpim')
			expect(yield* handleMessage(payload)).toEqual(expectedEvent(payload, payload.event.ts))
		}),
	)

	it.effect('orders a threaded message with its root message', ({ expect }) =>
		Effect.gen(function* () {
			const rootTs = '1700000000.000001'
			const payload = message('C_PUBLIC', 'channel', rootTs)
			expect(yield* handleMessage(payload)).toEqual(expectedEvent(payload, rootTs))
		}),
	)

	it.effect('ignores edited, deleted, and bot message subtypes', ({ expect }) =>
		Effect.gen(function* () {
			for (const subtype of ['message_changed', 'message_deleted', 'bot_message']) {
				const ordinary = message('C_PUBLIC', 'channel')
				const payload = { ...ordinary, event: { ...ordinary.event, subtype } }
				expect(yield* handleMessage(payload)).toEqual(ProviderWebhookIgnored.make({}))
			}
		}),
	)
})
