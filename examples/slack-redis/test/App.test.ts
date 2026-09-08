import { assert, it } from '@effect/vitest'
import { Effect, Layer, Queue } from 'effect'
import { HttpRouter } from 'effect/unstable/http'

import {
	SlackEmulator,
	makeExampleTestTransport,
	PostedMessageResponse,
	HistoryResponse,
	slackEmulatorAliceToken,
	slackEmulatorBotToken,
} from '../../slack-thread-echo/test/support.js'
import { application } from '../src/app.js'

it.live('the actual storage example subscribes on a signed mention and echoes a subscribed reply', () =>
	Effect.gen(function* () {
		const emulator = yield* SlackEmulator
		const test = yield* makeExampleTestTransport
		const web = HttpRouter.toWebHandler(
			application.pipe(Layer.provide(test.transport), Layer.provide(test.config)),
			{ disableLogger: true },
		)
		yield* Effect.addFinalizer(() => Effect.promise(web.dispose))
		const root = yield* emulator.call(
			slackEmulatorAliceToken,
			'chat.postMessage',
			{ channel: emulator.publicChannelId, text: 'hello' },
			PostedMessageResponse,
		)
		const callback = {
			type: 'event_callback' as const,
			team_id: emulator.teamId,
			event_id: 'Ev_storage_mention',
			event_time: 1,
			event: {
				type: 'app_mention' as const,
				channel: root.channel,
				ts: root.ts,
				text: 'hello',
				user: emulator.aliceUserId,
			},
		}
		assert.strictEqual((yield* Effect.promise(() => web.handler(emulator.signedWebhook(callback)))).status, 200)
		yield* Queue.take(test.posts)
		const followUp = yield* emulator.call(
			slackEmulatorAliceToken,
			'chat.postMessage',
			{ channel: root.channel, thread_ts: root.ts, text: 'again' },
			PostedMessageResponse,
		)
		assert.strictEqual(
			(yield* Effect.promise(() =>
				web.handler(
					emulator.signedWebhook({
						...callback,
						event_id: 'Ev_storage_followup',
						event: {
							...callback.event,
							type: 'message',
							ts: followUp.ts,
							thread_ts: root.ts,
							text: 'again',
						},
					}),
				),
			)).status,
			200,
		)
		yield* Queue.take(test.posts)
		const history = yield* emulator.call(
			slackEmulatorBotToken,
			'conversations.replies',
			{ channel: root.channel, ts: root.ts },
			HistoryResponse,
		)
		const replies = history.messages.filter((message) => message.text.startsWith('Durable echo:'))
		assert.deepStrictEqual(
			replies.map((message) => message.text),
			['Durable echo: hello', 'Durable echo: again'],
		)
		assert.ok(replies.every((message) => message.thread_ts === root.ts))
	}).pipe(Effect.provide(SlackEmulator.layer)),
)
