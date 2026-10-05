import { NodeCrypto } from '@effect/platform-node'
import { describe, it } from '@effect/vitest'
import { DeliveryAdmission, ProviderWebhookEvent } from '@humanlayer/channels-delivery'
import { Effect, Layer, Redacted } from 'effect'

import { SlackApi, SlackApiError } from '../src/SlackApi'
import { SlackMessageTs } from '../src/SlackIdentity'
import { makeSlackWebhookProvider } from '../src/SlackWebhookProvider'
import { signedSlackInput } from './fixtures'

const signingSecret = 'reaction-test-secret'
const reaction = {
	type: 'event_callback',
	team_id: 'T_TEST',
	event_id: 'Ev_REACTION',
	event_time: 1_700_000_000,
	event: {
		type: 'reaction_added',
		user: 'U_TEST',
		reaction: 'eyes',
		item: { type: 'message', channel: 'C_TEST', ts: '1700000001.000001' },
		event_ts: '1700000002.000001',
	},
}

const provider = makeSlackWebhookProvider({
	namespace: 'reaction-test',
	signingSecret: Redacted.make(signingSecret),
})

const handleReaction = (resolveReactionThread: (typeof SlackApi.Service)['resolveReactionThread']) =>
	signedSlackInput(signingSecret, reaction).pipe(
		Effect.flatMap(provider.handle),
		Effect.provide(Layer.merge(NodeCrypto.layer, Layer.mock(SlackApi, { resolveReactionThread }))),
	)

describe('Slack reaction thread resolution', () => {
	it.effect('uses the resolved root timestamp for reaction mailbox identity', ({ expect }) =>
		Effect.gen(function* () {
			const outcome = yield* handleReaction(() => Effect.succeed(SlackMessageTs.make('1700000000.000001')))

			expect(outcome).toEqual(
				ProviderWebhookEvent.make({
					event: DeliveryAdmission.make({
						namespace: 'reaction-test',
						provider: 'slack',
						installationId: 'T_TEST',
						resourceId: 'slack:v1:T_TEST:C_TEST:1700000000.000001',
						eventId: 'Ev_REACTION',
						payload: reaction,
					}),
				}),
			)
		}),
	)

	it.effect('falls back to the reacted message timestamp when resolution is unavailable', ({ expect }) =>
		Effect.gen(function* () {
			const outcome = yield* handleReaction(() =>
				Effect.fail(
					SlackApiError.make({ operation: 'resolve_reaction_thread', message: 'Could not reach Slack' }),
				),
			)

			expect(outcome).toEqual(
				ProviderWebhookEvent.make({
					event: DeliveryAdmission.make({
						namespace: 'reaction-test',
						provider: 'slack',
						installationId: 'T_TEST',
						resourceId: 'slack:v1:T_TEST:C_TEST:1700000001.000001',
						eventId: 'Ev_REACTION',
						payload: reaction,
					}),
				}),
			)
		}),
	)
})
