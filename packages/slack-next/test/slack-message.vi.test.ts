import { NodeCrypto } from '@effect/platform-node'
import { describe, it } from '@effect/vitest'
import { DeliveryAdmission, ProviderWebhookEvent, ProviderWebhookIgnored } from '@humanlayer/channels-delivery-next'
import { Effect, Layer, Predicate, Redacted, Schema } from 'effect'

import { SlackApi } from '../src/SlackApi'
import { makeSlackWebhookProvider } from '../src/SlackWebhookProvider'
import { signedSlackInput } from './fixtures'

const signingSecret = 'message-test-secret'
const message = (channel: string, channelType: 'channel' | 'im' | 'mpim', threadTs?: string) => {
	const event = {
		type: 'message',
		user: 'U_TEST',
		text: 'hello agent',
		ts: '1700000001.000001',
		channel,
		channel_type: channelType,
	}
	return {
		type: 'event_callback',
		team_id: 'T_TEST',
		event_id: `Ev_${channelType}`,
		event_time: 1_700_000_000,
		event: Predicate.isUndefined(threadTs) ? event : { ...event, thread_ts: threadTs },
	}
}

const provider = makeSlackWebhookProvider({ namespace: 'message-test', signingSecret: Redacted.make(signingSecret) })

const handleMessage = (payload: Schema.Json) =>
	signedSlackInput(signingSecret, payload).pipe(
		Effect.flatMap(provider.handle),
		Effect.provide(
			Layer.merge(
				NodeCrypto.layer,
				Layer.mock(SlackApi, {
					resolveReactionThread: () => Effect.die(new Error('Messages must not resolve reaction threads')),
				}),
			),
		),
	)

const expectedEvent = (payload: ReturnType<typeof message>, resourceTs: string) =>
	ProviderWebhookEvent.make({
		event: DeliveryAdmission.make({
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

	it.effect('admits edited and deleted messages using their thread roots', ({ expect }) =>
		Effect.gen(function* () {
			const ordinary = message('C_PUBLIC', 'channel')
			const updated = {
				...ordinary,
				event_id: 'Ev_UPDATED',
				event: {
					...ordinary.event,
					subtype: 'message_changed',
					message: {
						user: 'U_TEST',
						text: 'edited text',
						ts: ordinary.event.ts,
						thread_ts: '1700000000.000001',
					},
				},
			}
			const deleted = {
				...ordinary,
				event_id: 'Ev_DELETED',
				event: {
					...ordinary.event,
					subtype: 'message_deleted',
					deleted_ts: ordinary.event.ts,
					previous_message: {
						user: 'U_TEST',
						text: 'deleted text',
						ts: ordinary.event.ts,
						thread_ts: '1700000000.000001',
					},
				},
			}

			expect(yield* handleMessage(updated)).toEqual(expectedEvent(updated, '1700000000.000001'))
			expect(yield* handleMessage(deleted)).toEqual(expectedEvent(deleted, '1700000000.000001'))
		}),
	)

	it.effect('ignores unsupported message subtypes', ({ expect }) =>
		Effect.gen(function* () {
			const ordinary = message('C_PUBLIC', 'channel')
			const payload = { ...ordinary, event: { ...ordinary.event, subtype: 'bot_message' } }
			expect(yield* handleMessage(payload)).toEqual(ProviderWebhookIgnored.make({}))
		}),
	)
})
