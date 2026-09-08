import { assert, layer } from '@effect/vitest'
import { Slack, MarkdownContent, type Thread } from '@humanlayer/channels-slack'
import { SlackConnection, SlackConnectionCredentials } from '@humanlayer/channels-slack'
import { Clock, ConfigProvider, Effect, Queue, Redacted } from 'effect'

import {
	OpenConversationResponse,
	PostedMessageResponse,
	SlackEmulator,
	slackEmulatorAliceToken,
	slackEmulatorBotId,
	slackEmulatorBotToken,
	slackEmulatorBotUserId,
	slackEmulatorSigningSecret,
} from './support/SlackEmulator.js'
import { ChannelsStorage, makeSlackTestHost, slack } from './support/SlackTestHost.js'

layer(SlackEmulator.layer, { timeout: '30 seconds' })('Slack direct-message integration', (it) => {
	it.effect('runs DM, MPIM, proactive open, reply, and history through the emulator', () =>
		Effect.gen(function* () {
			const emulator = yield* SlackEmulator
			const eventTime = Math.floor((yield* Clock.currentTimeMillis) / 1000)
			const deliveries = yield* Queue.unbounded<{
				readonly id: string
				readonly isDm: boolean
				readonly historyCount: number
			}>()
			const proactive = yield* Queue.unbounded<Thread>()
			const connection = SlackConnection.make({
				credentials: SlackConnectionCredentials.make({
					botToken: Redacted.make(slackEmulatorBotToken),
					botUserId: slackEmulatorBotUserId,
					botId: slackEmulatorBotId,
				}),
			})
			const app = makeSlackTestHost({
				providers: [slack({ loadConnection: () => Effect.succeed(connection) })],
				storage: ChannelsStorage.memory(),
				onDirectMessage: (thread, message) =>
					Effect.gen(function* () {
						const channels = yield* Slack
						const history = yield* thread.listMessages({ direction: 'forward', limit: 10 })
						yield* Queue.offer(deliveries, {
							id: thread.ref.id,
							isDm: thread.isDM,
							historyCount: history.messages.length,
						})
						yield* thread.post(MarkdownContent.make({ markdown: `dm echo: ${message.text}` }))
						const opened = yield* channels.openDM({
							provider: 'slack',
							tenant: thread.ref.channel.tenant,
							user: message.author,
						})
						yield* Queue.offer(proactive, opened)
					}),
				advanced: {
					slackApiOrigin: new URL(`${emulator.emulator.url}/api`),
					configProvider: ConfigProvider.fromUnknown({
						SLACK_SIGNING_SECRET: slackEmulatorSigningSecret,
						SLACK_BOT_USER_ID: slackEmulatorBotUserId,
						SLACK_BOT_ID: slackEmulatorBotId,
					}),
				},
			})
			yield* Effect.addFinalizer(() => Effect.promise(() => app.close()))

			const opened = yield* emulator.call(
				slackEmulatorAliceToken,
				'conversations.open',
				{ users: slackEmulatorBotUserId },
				OpenConversationResponse,
			)
			const root = yield* emulator.call(
				slackEmulatorAliceToken,
				'chat.postMessage',
				{ channel: opened.channel.id, text: 'hello in DM' },
				PostedMessageResponse,
			)
			const response = yield* Effect.promise(() =>
				app.handle(
					emulator.signedWebhook({
						type: 'event_callback',
						team_id: emulator.teamId,
						event_id: 'Ev_dm_root',
						event_time: eventTime,
						event: {
							type: 'message',
							channel: root.channel,
							ts: root.ts,
							text: root.message.text,
							user: root.message.user ?? emulator.aliceUserId,
							channel_type: 'im',
						},
					}),
				),
			)
			assert.strictEqual(response.status, 200)
			const delivered = yield* Queue.take(deliveries)
			assert.strictEqual(delivered.isDm, true)
			assert.ok(delivered.historyCount >= 1)
			assert.ok(delivered.id.includes(':im:'))
			const proactiveThread = yield* Queue.take(proactive)
			assert.strictEqual(proactiveThread.isDM, true)
			assert.strictEqual(proactiveThread.ref.id.endsWith(`:im:${opened.channel.id}`), true)

			const mpim = yield* emulator.call(
				slackEmulatorAliceToken,
				'conversations.open',
				{ users: [slackEmulatorBotUserId, emulator.adminUserId] },
				OpenConversationResponse,
			)
			const mpimRoot = yield* emulator.call(
				slackEmulatorAliceToken,
				'chat.postMessage',
				{ channel: mpim.channel.id, text: 'hello in MPIM' },
				PostedMessageResponse,
			)
			const mpimResponse = yield* Effect.promise(() =>
				app.handle(
					emulator.signedWebhook({
						type: 'event_callback',
						team_id: emulator.teamId,
						event_id: 'Ev_mpim_root',
						event_time: eventTime,
						event: {
							type: 'message',
							channel: mpimRoot.channel,
							ts: mpimRoot.ts,
							text: mpimRoot.message.text,
							user: mpimRoot.message.user ?? emulator.aliceUserId,
							channel_type: 'mpim',
						},
					}),
				),
			)
			assert.strictEqual(mpimResponse.status, 200)
			const mpimDelivery = yield* Queue.take(deliveries)
			assert.ok(mpimDelivery.id.includes(':mpim:'))
			assert.strictEqual(mpimDelivery.isDm, true)
			yield* Queue.take(proactive)
		}),
	)
})
