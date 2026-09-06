import { assert, it } from '@effect/vitest'
import { MailboxReadiness, MailboxStore } from '@humanlayer/channels-delivery'
import { Clock, Context, Effect, Exit, Layer, Queue, Scope } from 'effect'
import { HttpRouter } from 'effect/unstable/http'

import { routes, services, worker } from '../src/app.ts'
import { close, handle } from '../src/fetch.ts'
import {
	SlackEmulator,
	makeExampleTestTransport,
	PostedMessageResponse,
	HistoryResponse,
	slackEmulatorAliceToken,
	slackEmulatorBotToken,
} from './support.ts'

it.live('the actual echo graph admits without a worker, then replies and handles subscribed follow-ups', () =>
	Effect.gen(function* () {
		const emulator = yield* SlackEmulator
		const test = yield* makeExampleTestTransport
		const memoMap = yield* Layer.makeMemoMap
		const dependencies = Layer.merge(test.transport, test.config)
		const web = HttpRouter.toWebHandler(routes.pipe(Layer.provide(dependencies)), { memoMap, disableLogger: true })
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
			event_id: 'Ev_example_mention',
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
		assert.strictEqual(yield* Queue.size(test.posts), 0)
		const context = yield* Layer.buildWithMemoMap(
			services.pipe(Layer.provide(dependencies)),
			memoMap,
			yield* Scope.Scope,
		)
		const keys = yield* Context.get(context, MailboxReadiness).scanReady({
			prefix: '',
			now: yield* Clock.currentTimeMillis,
			limit: 10,
		})
		assert.strictEqual(keys.length, 1)
		const key = keys.at(0)
		assert.ok(key !== undefined)
		assert.strictEqual((yield* Context.get(context, MailboxStore).loadMailbox({ key }))?.state.pending.length, 1)
		const workerScope = yield* Scope.make()
		yield* Effect.addFinalizer(() => Scope.close(workerScope, Exit.void))
		yield* Layer.buildWithMemoMap(worker.pipe(Layer.provide(dependencies)), memoMap, workerScope)
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
						event_id: 'Ev_example_followup',
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
		assert.deepStrictEqual(
			history.messages.filter((message) => message.text.startsWith('Echo')).map((message) => message.text),
			['Echo: hello', 'Echo 2: again'],
		)
		yield* Scope.close(workerScope, Exit.void)
	}).pipe(Effect.provide(SlackEmulator.layer)),
)

it.effect('the Fetch entrypoint refuses new admission after shutdown without loading credentials', () =>
	Effect.gen(function* () {
		yield* Effect.promise(close)
		const response = yield* Effect.promise(() =>
			handle(new Request('http://localhost/api/v1/integrations/slack/webhook', { method: 'POST', body: '{}' })),
		)
		assert.strictEqual(response.status, 503)
	}),
)
