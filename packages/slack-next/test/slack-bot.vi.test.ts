import * as NodeCrypto from '@effect/platform-node/NodeCrypto'
import { describe, it } from '@effect/vitest'
import { Channels, ChannelsMemory, QueueDeliveryMode } from '@humanlayer/channels-delivery-next'
import { Config, Deferred, Effect, Layer, Redacted } from 'effect'
import { TestClock } from 'effect/testing'
import { HttpRouter, HttpServerRequest } from 'effect/unstable/http'

import {
	SlackApi,
	SlackAppMentionEvent,
	SlackBot,
	SlackChannelId,
	SlackMessageTs,
	SlackParticipant,
	SlackTeamId,
	SlackUserId,
} from '../src'
import { SlackAppMentionEnvelope } from '../src/SlackWebhookSchemas'
import { signedSlackInput, slackEmulatorSigningSecret } from './fixtures'

const mention = SlackAppMentionEnvelope.make({
	type: 'event_callback',
	team_id: SlackTeamId.make('T_BOT_TEST'),
	event_id: 'Ev-slack-bot-mention',
	event_time: 0,
	event: SlackAppMentionEvent.make({
		type: 'app_mention',
		user: SlackUserId.make('U_ALICE'),
		text: '<@U_BOT> hello',
		ts: SlackMessageTs.make('1700000001.000001'),
		channel: SlackChannelId.make('C_BOT_TEST'),
	}),
})

describe('SlackBot.make', () => {
	it.effect('carries a signed mention through Channels.make to onNewMention', ({ expect }) =>
		Effect.gen(function* () {
			const mentioned = yield* Deferred.make<{ readonly threadTs: string; readonly subscribed: boolean }>()
			const bot = Channels.make({
				namespace: 'slack-bot-test',
				basePath: '/api/channels',
				providers: [
					SlackBot.make({
						signingSecret: Config.succeed(Redacted.make(slackEmulatorSigningSecret)),
						deliveryMode: QueueDeliveryMode.make({}),
						slackApi: Layer.mock(SlackApi, {
							resolveParticipant: (request) =>
								Effect.succeed(
									SlackParticipant.make({
										userId: SlackUserId.make(request.userId ?? 'U_ALICE'),
										userName: 'alice',
										fullName: 'Alice Example',
										isBot: false,
										isMe: false,
									}),
								),
						}),
						handlers: {
							onNewMention: (event) =>
								Effect.gen(function* () {
									yield* event.thread.subscribe()
									const subscribed = yield* event.thread.isSubscribed()
									yield* Deferred.succeed(mentioned, {
										threadTs: event.thread.ref.threadTs,
										subscribed,
									})
								}),
						},
					}),
				],
				eventProcessing: { concurrency: 1, leaseMs: 30_000 },
				storage: ChannelsMemory.make({ polling: { intervalMs: 1_000 } }),
			})
			const fetch = yield* HttpRouter.toHttpEffect(bot.routes).pipe(Effect.provide(NodeCrypto.layer))
			const signed = yield* signedSlackInput(slackEmulatorSigningSecret, mention)

			const response = yield* fetch.pipe(
				Effect.provideService(
					HttpServerRequest.HttpServerRequest,
					HttpServerRequest.fromWeb(
						new Request('http://localhost/api/channels/integrations/slack/webhook', {
							method: 'POST',
							headers: signed.headers,
							body: new TextDecoder().decode(signed.body),
						}),
					),
				),
			)
			yield* TestClock.adjust(1_000)

			expect(response.status).toBe(200)
			expect(yield* Deferred.await(mentioned)).toEqual({ threadTs: '1700000001.000001', subscribed: true })
		}),
	)
})
