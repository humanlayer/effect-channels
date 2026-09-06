import { assert, it } from '@effect/vitest'
import { MailboxReadiness, MailboxStore } from '@humanlayer/channels-delivery'
import { MarkdownContent } from '@humanlayer/channels-slack'
import { Clock, ConfigProvider, Context, Deferred, Effect, Exit, Layer, Scope } from 'effect'
import { HttpRouter } from 'effect/unstable/http'

import {
	HistoryResponse,
	PostedMessageResponse,
	SlackEmulator,
	slackEmulatorAliceToken,
	slackEmulatorBotToken,
	slackEmulatorBotUserId,
	slackEmulatorBotId,
	slackEmulatorSigningSecret,
} from './support/SlackEmulator.ts'
import { ChannelsStorage, makeSlackTestHost, defaultDeliveryPolicy, slack } from './support/SlackTestHost.ts'

it.live('routes commit without executing; a scoped runner replies then finalizes on shutdown', () =>
	Effect.gen(function* () {
		const emulator = yield* SlackEmulator
		const entered = yield* Deferred.make<void>()
		const finalized = yield* Deferred.make<void>()
		const app = makeSlackTestHost({
			providers: [slack()],
			storage: ChannelsStorage.memory(),
			policy: defaultDeliveryPolicy,
			onNewMention: (thread) =>
				thread
					.post(MarkdownContent.make({ markdown: 'explicit runner reply' }))
					.pipe(
						Effect.andThen(Deferred.succeed(entered, undefined)),
						Effect.andThen(Effect.never),
						Effect.ensuring(Deferred.succeed(finalized, undefined)),
					),
			advanced: {
				slackApiOrigin: new URL(`${emulator.emulator.url}/api`),
				configProvider: ConfigProvider.fromUnknown({
					SLACK_BOT_TOKEN: slackEmulatorBotToken,
					SLACK_BOT_USER_ID: slackEmulatorBotUserId,
					SLACK_BOT_ID: slackEmulatorBotId,
					SLACK_SIGNING_SECRET: slackEmulatorSigningSecret,
				}),
			},
		})
		const memoMap = yield* Layer.makeMemoMap
		const web = HttpRouter.toWebHandler(app.routes, { memoMap, disableLogger: true })
		yield* Effect.addFinalizer(() => Effect.promise(web.dispose))
		const root = yield* emulator.call(
			slackEmulatorAliceToken,
			'chat.postMessage',
			{ channel: emulator.publicChannelId, text: 'admit before processing' },
			PostedMessageResponse,
		)
		const response = yield* Effect.promise(() =>
			web.handler(
				emulator.signedWebhook({
					type: 'event_callback',
					team_id: emulator.teamId,
					event_id: 'Ev_explicit_runner',
					event_time: 1,
					event: {
						type: 'app_mention',
						channel: root.channel,
						ts: root.ts,
						text: root.message.text,
						user: emulator.aliceUserId,
					},
				}),
			),
		)
		assert.strictEqual(response.status, 200)
		assert.strictEqual(yield* Deferred.isDone(entered), false)
		const context = yield* Layer.buildWithMemoMap(app.services, memoMap, yield* Scope.Scope)
		const keys = yield* Context.get(context, MailboxReadiness).scanReady({
			prefix: '',
			now: yield* Clock.currentTimeMillis,
			limit: 10,
		})
		assert.strictEqual(keys.length, 1)
		const key = keys.at(0)
		assert.ok(key !== undefined)
		const snapshot = yield* Context.get(context, MailboxStore).loadMailbox({ key })
		assert.strictEqual(snapshot?.state.pending.length, 1)
		assert.strictEqual(snapshot?.state.active, null)
		const workerScope = yield* Scope.make()
		yield* Effect.addFinalizer(() => Scope.close(workerScope, Exit.void))
		yield* Layer.buildWithMemoMap(app.worker, memoMap, workerScope)
		yield* Deferred.await(entered)
		const history = yield* emulator.call(
			slackEmulatorBotToken,
			'conversations.replies',
			{ channel: root.channel, ts: root.ts },
			HistoryResponse,
		)
		assert.strictEqual(history.messages.filter((message) => message.text === 'explicit runner reply').length, 1)
		yield* Scope.close(workerScope, Exit.void)
		assert.strictEqual(yield* Deferred.isDone(finalized), true)
	}).pipe(Effect.provide(SlackEmulator.layer)),
)
