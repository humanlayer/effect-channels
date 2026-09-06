import { assert, it } from '@effect/vitest'
import { SlackConnectionStore, SlackTeamId } from '@humanlayer/channels-slack'
import { Context, Effect, Layer, Queue, Scope } from 'effect'
import { HttpRouter } from 'effect/unstable/http'

import {
	SlackEmulator,
	makeExampleTestTransport,
	PostedMessageResponse,
	OpenConversationResponse,
	HistoryResponse,
	slackEmulatorAliceToken,
	slackEmulatorBotToken,
} from '../../slack-thread-echo/test/support.ts'
import { application } from '../src/app.ts'

it.live(
	'the actual multi-tenant graph replies to DMs and drops unknown installations using supplied credential Layers',
	() =>
		Effect.gen(function* () {
			const emulator = yield* SlackEmulator
			const test = yield* makeExampleTestTransport
			const memoMap = yield* Layer.makeMemoMap
			const web = HttpRouter.toWebHandler(
				application.pipe(Layer.provide(test.transport), Layer.provide(test.config)),
				{ disableLogger: true, memoMap },
			)
			yield* Effect.addFinalizer(() => Effect.promise(web.dispose))
			const dm = yield* emulator.call(
				slackEmulatorBotToken,
				'conversations.open',
				{ users: emulator.aliceUserId },
				OpenConversationResponse,
			)
			const message = yield* emulator.call(
				slackEmulatorAliceToken,
				'chat.postMessage',
				{ channel: dm.channel.id, text: 'hello dm' },
				PostedMessageResponse,
			)
			const callback = {
				type: 'event_callback' as const,
				team_id: emulator.teamId,
				event_id: 'Ev_example_dm',
				event_time: 1,
				event: {
					type: 'message' as const,
					channel_type: 'im' as const,
					channel: dm.channel.id,
					ts: message.ts,
					user: emulator.aliceUserId,
					text: 'hello dm',
				},
			}
			assert.strictEqual((yield* Effect.promise(() => web.handler(emulator.signedWebhook(callback)))).status, 200)
			yield* Queue.take(test.posts)
			const history = yield* emulator.call(
				slackEmulatorBotToken,
				'conversations.replies',
				{ channel: dm.channel.id, ts: message.ts },
				HistoryResponse,
			)
			assert.strictEqual(
				history.messages.filter((item) => item.text === 'Workspace-aware echo: hello dm').length,
				1,
			)
			assert.strictEqual(
				(yield* Effect.promise(() =>
					web.handler(emulator.signedWebhook({ ...callback, team_id: 'T_UNKNOWN' })),
				)).status,
				200,
			)
			assert.strictEqual(yield* Queue.size(test.posts), 0)
			const services = yield* Layer.buildWithMemoMap(test.transport, memoMap, yield* Scope.Scope)
			yield* Context.get(services, SlackConnectionStore).remove({
				workspaceId: SlackTeamId.make(emulator.teamId),
			})
			assert.strictEqual(
				(yield* Effect.promise(() =>
					web.handler(emulator.signedWebhook({ ...callback, event_id: 'Ev_removed_installation' })),
				)).status,
				200,
			)
			assert.strictEqual(yield* Queue.size(test.posts), 0)
		}).pipe(Effect.provide(SlackEmulator.layer)),
)
