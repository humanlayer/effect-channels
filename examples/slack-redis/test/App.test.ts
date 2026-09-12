import { assert, it } from '@effect/vitest'
import { MailboxReadiness, MailboxStore } from '@humanlayer/channels-delivery'
import { Clock, Context, Effect, Layer, Queue, Scope } from 'effect'
import { TestClock } from 'effect/testing'
import { HttpRouter } from 'effect/unstable/http'

import {
	SlackEmulator,
	makeExampleTestTransport,
	PostedMessageResponse,
	HistoryResponse,
	slackEmulatorAliceToken,
	slackEmulatorBotToken,
} from '../../slack-thread-echo/test/support.js'
import { application, bot } from '../src/app.js'

it.effect('the actual storage example debounces a signed mention, subscribes, then echoes a subscribed reply', () =>
	Effect.gen(function* () {
		yield* TestClock.setTime(yield* Clock.currentTimeMillis.pipe(TestClock.withLive))
		const emulator = yield* SlackEmulator
		const test = yield* makeExampleTestTransport
		const dependencies = Layer.merge(test.transport, test.config)
		const memoMap = yield* Layer.makeMemoMap
		yield* Layer.buildWithMemoMap(bot.worker.pipe(Layer.provide(dependencies)), memoMap, yield* Scope.Scope)
		const web = HttpRouter.toWebHandler(application.pipe(Layer.provide(dependencies)), {
			memoMap,
			disableLogger: true,
		})
		const clock = Context.make(Clock.Clock, yield* Clock.Clock)
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
		assert.strictEqual(
			(yield* Effect.promise(() => web.handler(emulator.signedWebhook(callback), clock))).status,
			200,
		)
		yield* TestClock.adjust(1500)
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
					clock,
				),
			)).status,
			200,
		)
		yield* TestClock.adjust(1500)
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

it.effect('the actual bot resets the quiet period for a burst, not duplicates, and posts only the latest', () =>
	Effect.gen(function* () {
		yield* TestClock.setTime(yield* Clock.currentTimeMillis.pipe(TestClock.withLive))
		const start = yield* Clock.currentTimeMillis
		const emulator = yield* SlackEmulator
		const test = yield* makeExampleTestTransport
		const dependencies = Layer.merge(test.transport, test.config)
		const memoMap = yield* Layer.makeMemoMap
		const scope = yield* Scope.Scope
		const context = yield* Layer.buildWithMemoMap(
			bot.services.pipe(Layer.provideMerge(dependencies)),
			memoMap,
			scope,
		)
		const web = HttpRouter.toWebHandler(bot.routes.pipe(Layer.provide(dependencies)), {
			memoMap,
			disableLogger: true,
		})
		yield* Effect.addFinalizer(() => Effect.promise(web.dispose))
		const clock = Context.make(Clock.Clock, yield* Clock.Clock)
		const root = yield* emulator.call(
			slackEmulatorAliceToken,
			'chat.postMessage',
			{ channel: emulator.publicChannelId, text: 'burst root' },
			PostedMessageResponse,
		)
		const deliver = (text: string, index: number) =>
			Effect.promise(() =>
				web.handler(
					emulator.signedWebhook({
						type: 'event_callback',
						team_id: emulator.teamId,
						event_id: `Ev_storage_burst_${index}`,
						event_time: 1,
						event: {
							type: 'app_mention',
							channel: root.channel,
							thread_ts: root.ts,
							ts: `${Math.floor(Number(root.ts)) + index}.000001`,
							text,
							user: emulator.aliceUserId,
						},
					}),
					clock,
				),
			).pipe(Effect.tap((response) => Effect.sync(() => assert.strictEqual(response.status, 200))))
		yield* deliver('earlier', 1)
		const [key] = yield* Context.get(context, MailboxReadiness).scanReady({
			prefix: '',
			now: start + 1500,
			limit: 10,
		})
		assert.ok(key !== undefined)
		const store = Context.get(context, MailboxStore)
		assert.strictEqual((yield* store.loadMailbox({ key }))?.state.readyAt, start + 1500)
		yield* TestClock.adjust(100)
		yield* deliver('latest', 2)
		assert.strictEqual((yield* store.loadMailbox({ key }))?.state.readyAt, start + 1600)
		yield* TestClock.adjust(100)
		yield* deliver('latest', 2)
		const pending = yield* store.loadMailbox({ key })
		assert.strictEqual(pending?.state.pending.length, 2)
		assert.strictEqual(pending?.state.readyAt, start + 1600)
		yield* Layer.buildWithMemoMap(bot.worker.pipe(Layer.provide(dependencies)), memoMap, scope)
		yield* TestClock.adjust(1399)
		assert.strictEqual(yield* Queue.size(test.posts), 0)
		yield* TestClock.adjust(1)
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
			['Durable echo: latest'],
		)
		assert.ok(replies.every((message) => message.thread_ts === root.ts))
	}).pipe(Effect.provide(SlackEmulator.layer)),
)
