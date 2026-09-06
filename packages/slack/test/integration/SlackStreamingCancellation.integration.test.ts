import { assert, layer } from '@effect/vitest'
import { ConfigProvider, Deferred, Effect, Queue, Stream } from 'effect'
import * as FetchHttpClient from 'effect/unstable/http/FetchHttpClient'

import { PostedMessageResponse, SlackEmulator, slackEmulatorAliceToken } from './support/SlackEmulator.ts'
import { ChannelsStorage, makeSlackTestHost, defaultDeliveryPolicy, slack } from './support/SlackTestHost.ts'

layer(SlackEmulator.layer, { timeout: '30 seconds' })('Slack stream cancellation integration', (it) => {
	it.effect('interrupts the active handler before delivering one signed stop callback', () =>
		Effect.gen(function* () {
			const emulator = yield* SlackEmulator
			const started = yield* Deferred.make<void>()
			const order = yield* Queue.unbounded<string>()
			const app = makeSlackTestHost({
				providers: [slack()],
				storage: ChannelsStorage.memory(),
				policy: { ...defaultDeliveryPolicy, heartbeatMs: 25, leaseMs: 1_000 },
				onNewMention: (thread) =>
					thread
						.stream(
							Stream.fromEffect(Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never))),
						)
						.pipe(Effect.ensuring(Queue.offer(order, 'finalized')), Effect.asVoid),
				onConversationStopped: () => Queue.offer(order, 'stopped').pipe(Effect.asVoid),
				advanced: {
					httpClient: FetchHttpClient.layer,
					slackApiOrigin: new URL(`${emulator.emulator.url}/api`),
					configProvider: ConfigProvider.fromUnknown({
						SLACK_SIGNING_SECRET: 'channels-emulator-signing-secret',
						SLACK_BOT_TOKEN: 'xoxb-channels-emulator',
						SLACK_BOT_USER_ID: 'U_CHANNELS_BOT',
						SLACK_BOT_ID: 'B_CHANNELS_BOT',
					}),
				},
			})
			yield* Effect.addFinalizer(() => Effect.promise(() => app.close()))
			const root = yield* emulator.call(
				slackEmulatorAliceToken,
				'chat.postMessage',
				{ channel: emulator.publicChannelId, text: 'please stream' },
				PostedMessageResponse,
			)
			const mention = emulator.signedWebhook({
				type: 'event_callback',
				team_id: emulator.teamId,
				event_id: 'Ev_STREAM_START',
				event_time: 1_788_000_000,
				event: {
					type: 'app_mention',
					channel: emulator.publicChannelId,
					ts: root.ts,
					text: '<@U_CHANNELS_BOT> please stream',
					user: emulator.aliceUserId,
				},
			})
			assert.strictEqual((yield* Effect.promise(() => app.handle(mention))).status, 200)
			yield* Deferred.await(started)
			const stoppedEvent = {
				type: 'event_callback' as const,
				team_id: emulator.teamId,
				event_id: 'Ev_STREAM_STOP',
				event_time: 1_788_000_001,
				event: {
					type: 'agent_session_stopped' as const,
					channel: emulator.publicChannelId,
					thread_ts: root.ts,
					user: emulator.aliceUserId,
					event_ts: '1788000001.000001',
					streaming_message_ts: [],
				},
			}
			assert.strictEqual(
				(yield* Effect.promise(() => app.handle(emulator.signedWebhook(stoppedEvent)))).status,
				200,
			)
			assert.strictEqual(yield* Queue.take(order), 'finalized')
			assert.strictEqual(yield* Queue.take(order), 'stopped')
			assert.strictEqual(
				(yield* Effect.promise(() => app.handle(emulator.signedWebhook(stoppedEvent)))).status,
				200,
			)
			assert.strictEqual(yield* Queue.size(order), 0)
		}),
	)
})
