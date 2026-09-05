import { assert, layer } from '@effect/vitest'
import { Emoji, MarkdownContent } from '@humanlayer/channels'
import {
	SlackClient,
	SlackChannelId,
	SlackMessageTs,
	SlackProvider,
	SlackTeamId,
	SlackTenantCredentials,
	SlackThreadRef,
	slackThreadRef,
} from '@humanlayer/channels-slack'
import { ConfigProvider, Effect, Layer, Option, Redacted, Schema } from 'effect'
import * as FetchHttpClient from 'effect/unstable/http/FetchHttpClient'

import {
	HistoryResponse,
	PostedMessageResponse,
	SlackEmulator,
	slackEmulatorAliceToken,
	slackEmulatorBotToken,
} from './support/SlackEmulator.ts'

const ReactionsResponse = Schema.Struct({
	ok: Schema.Literal(true),
	message: Schema.Struct({
		reactions: Schema.optionalKey(
			Schema.Array(Schema.Struct({ name: Schema.String, users: Schema.Array(Schema.String) })),
		),
	}),
})

layer(SlackEmulator.layer, { timeout: '30 seconds' })('Slack lifecycle emulator integration', (it) => {
	it.effect('edits, reacts to, and deletes a Slack reply through production clients', () =>
		Effect.gen(function* () {
			const emulator = yield* SlackEmulator
			const credentials = SlackTenantCredentials.make({
				load: () =>
					Effect.succeed(
						Option.some({
							botToken: Redacted.make(slackEmulatorBotToken),
							botUserId: 'U_CHANNELS_BOT',
							botId: 'B_CHANNELS_BOT',
						}),
					),
				save: () => Effect.void,
			})
			const clientLayer = SlackClient.layerWith({ apiOrigin: new URL(`${emulator.emulator.url}/api`) }).pipe(
				Layer.provide(Layer.merge(FetchHttpClient.layer, credentials)),
				Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({}))),
			)
			const providerLayer = SlackProvider.layer.pipe(Layer.provide(clientLayer))
			const root = yield* emulator.call(
				slackEmulatorAliceToken,
				'chat.postMessage',
				{ channel: emulator.publicChannelId, text: 'lifecycle root' },
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
			const messageRef = yield* Effect.gen(function* () {
				const provider = yield* SlackProvider
				const sent = yield* provider.post({
					threadId: thread.id,
					content: MarkdownContent.make({ markdown: 'before edit' }),
				})
				yield* provider.edit({
					threadId: thread.id,
					messageRef: sent.ref.messageRef,
					content: MarkdownContent.make({ markdown: 'after edit' }),
				})
				yield* provider.addReaction({
					threadId: thread.id,
					messageRef: sent.ref.messageRef,
					emoji: Emoji.ThumbsUp,
				})
				return sent.ref.messageRef
			}).pipe(Effect.provide(providerLayer))

			const replies = yield* emulator.call(
				slackEmulatorBotToken,
				'conversations.replies',
				{ channel: emulator.publicChannelId, ts: root.ts },
				HistoryResponse,
			)
			assert.strictEqual(replies.messages.find((message) => message.ts === messageRef)?.text, 'after edit')
			const reaction = yield* emulator.call(
				slackEmulatorBotToken,
				'reactions.get',
				{ channel: emulator.publicChannelId, timestamp: messageRef },
				ReactionsResponse,
			)
			assert.strictEqual(reaction.message.reactions?.[0]?.name, 'thumbsup')

			yield* Effect.gen(function* () {
				const provider = yield* SlackProvider
				yield* provider.removeReaction({ threadId: thread.id, messageRef, emoji: Emoji.ThumbsUp })
				yield* provider.delete({ threadId: thread.id, messageRef })
			}).pipe(Effect.provide(providerLayer))
			const afterDelete = yield* emulator.call(
				slackEmulatorBotToken,
				'conversations.replies',
				{ channel: emulator.publicChannelId, ts: root.ts },
				HistoryResponse,
			)
			assert.strictEqual(
				afterDelete.messages.some((message) => message.ts === messageRef),
				false,
			)
		}),
	)
})
