import { assert, layer } from '@effect/vitest'
import { MarkdownTextChunk } from '@humanlayer/channels'
import {
	SlackChannelId,
	SlackClient,
	SlackMessageTs,
	SlackProvider,
	SlackTeamId,
	SlackTenantCredentials,
	SlackThreadRef,
	slackThreadRef,
} from '@humanlayer/channels-slack'
import { ConfigProvider, Deferred, Effect, Fiber, Layer, Option, Redacted, Stream } from 'effect'
import { TestClock } from 'effect/testing'
import * as FetchHttpClient from 'effect/unstable/http/FetchHttpClient'

import {
	HistoryResponse,
	PostedMessageResponse,
	SlackEmulator,
	slackEmulatorAliceToken,
	slackEmulatorBotToken,
} from './support/SlackEmulator.ts'

layer(SlackEmulator.layer, { timeout: '30 seconds' })('Slack streaming emulator integration', (it) => {
	it.effect('falls back through production clients and finalizes one stable Slack reply', () =>
		Effect.gen(function* () {
			const emulator = yield* SlackEmulator
			const credentials = SlackTenantCredentials.make({
				load: () =>
					Effect.succeed(
						Option.some({ botToken: Redacted.make(slackEmulatorBotToken), botUserId: 'U_CHANNELS_BOT' }),
					),
				save: () => Effect.void,
			})
			const clientLayer = SlackClient.layerWith({ apiOrigin: new URL(`${emulator.emulator.url}/api`) }).pipe(
				Layer.provide(Layer.merge(FetchHttpClient.layer, credentials)),
				Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({}))),
			)
			const root = yield* emulator.call(
				slackEmulatorAliceToken,
				'chat.postMessage',
				{ channel: emulator.publicChannelId, text: 'stream root' },
				PostedMessageResponse,
			)
			const thread = slackThreadRef(
				SlackThreadRef.make({
					teamId: SlackTeamId.make(emulator.teamId),
					channelId: SlackChannelId.make(emulator.publicChannelId),
					threadTs: SlackMessageTs.make(root.ts),
				}),
				false,
			)
			const waitingForSecond = yield* Deferred.make<void>()
			const releaseSecond = yield* Deferred.make<void>()
			const chunks = Stream.make(MarkdownTextChunk.make({ text: 'one ' })).pipe(
				Stream.concat(
					Stream.fromEffect(
						Deferred.succeed(waitingForSecond, undefined).pipe(
							Effect.andThen(Deferred.await(releaseSecond)),
							Effect.as(MarkdownTextChunk.make({ text: 'complete streamed response' })),
						),
					),
				),
			)
			const streamFiber = yield* Effect.flatMap(SlackProvider, (provider) =>
				provider.stream({ threadId: thread.id }, chunks),
			).pipe(
				Effect.provide(
					SlackProvider.layerWith({ streaming: 'post_and_edit' }).pipe(Layer.provide(clientLayer)),
				),
				Effect.forkChild,
			)
			yield* Deferred.await(waitingForSecond)
			const intermediate = yield* emulator.call(
				slackEmulatorBotToken,
				'conversations.replies',
				{ channel: emulator.publicChannelId, ts: root.ts },
				HistoryResponse,
			)
			assert.strictEqual(intermediate.messages.filter((message) => message.text === 'one ').length, 1)
			yield* Deferred.succeed(releaseSecond, undefined)
			yield* TestClock.adjust('500 millis')
			const sent = yield* Fiber.join(streamFiber)
			const replies = yield* emulator.call(
				slackEmulatorBotToken,
				'conversations.replies',
				{ channel: emulator.publicChannelId, ts: root.ts },
				HistoryResponse,
			)
			const matching = replies.messages.filter((message) => message.ts === sent.ref.messageRef)
			assert.strictEqual(matching.length, 1)
			assert.strictEqual(matching[0]?.text, 'one complete streamed response')
			assert.deepStrictEqual(sent.ref.degraded, [])
		}),
	)
})
