import { NodeCrypto } from '@effect/platform-node'
import { describe, it } from '@effect/vitest'
import { DeliveryAdmission, ProviderWebhookEvent } from '@humanlayer/channels-delivery-next'
import { Effect, Layer, Predicate, Redacted, Schema } from 'effect'

import { SlackApi } from '../src/SlackApi'
import { makeSlackWebhookProvider } from '../src/SlackWebhookProvider'
import { signedSlackInput } from './fixtures'

const signingSecret = 'mention-test-secret'
const rootMentionEvent = {
	type: 'app_mention',
	user: 'U_TEST',
	text: '<@U_BOT> hello',
	ts: '1700000001.000001',
	channel: 'C_TEST',
}

const appMention = (threadTs?: string) => ({
	type: 'event_callback',
	team_id: 'T_TEST',
	event_id: 'Ev_MENTION',
	event_time: 1_700_000_000,
	event: Predicate.isUndefined(threadTs) ? rootMentionEvent : { ...rootMentionEvent, thread_ts: threadTs },
})

const handleMention = (payload: Schema.Json) =>
	makeSlackWebhookProvider({
		namespace: 'mention-test',
		signingSecret: Redacted.make(signingSecret),
	})
		.handle(signedSlackInput(signingSecret, payload))
		.pipe(
			Effect.provide(
				Layer.merge(
					NodeCrypto.layer,
					Layer.mock(SlackApi, {
						resolveReactionThread: () =>
							Effect.die(new Error('App mentions must not resolve reaction threads')),
					}),
				),
			),
		)

describe('Slack app mention admission', () => {
	it.effect('uses the message timestamp for a root mention', ({ expect }) =>
		Effect.gen(function* () {
			const outcome = yield* handleMention(appMention())

			expect(outcome).toEqual(
				ProviderWebhookEvent.make({
					event: DeliveryAdmission.make({
						namespace: 'mention-test',
						provider: 'slack',
						installationId: 'T_TEST',
						resourceId: 'slack:v1:T_TEST:C_TEST:1700000001.000001',
						eventId: 'Ev_MENTION',
						payload: appMention(),
					}),
				}),
			)
		}),
	)

	it.effect('uses thread_ts for a mention in an existing thread', ({ expect }) =>
		Effect.gen(function* () {
			const outcome = yield* handleMention(appMention('1700000000.000001'))

			const payload = appMention('1700000000.000001')
			expect(outcome).toEqual(
				ProviderWebhookEvent.make({
					event: DeliveryAdmission.make({
						namespace: 'mention-test',
						provider: 'slack',
						installationId: 'T_TEST',
						resourceId: 'slack:v1:T_TEST:C_TEST:1700000000.000001',
						eventId: 'Ev_MENTION',
						payload,
					}),
				}),
			)
		}),
	)
})
