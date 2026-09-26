import { assert, layer } from '@effect/vitest'
import { FileUpload, MarkdownContent, PlainTextContent } from '@humanlayer/channels-slack'
import {
	SlackClient,
	SlackChannelId,
	SlackMessageTs,
	Slack,
	SlackTeamId,
	SlackTenantCredentials,
	SlackThreadRef,
	slackChannelRef,
	slackThreadRef,
} from '@humanlayer/channels-slack'
import { ConfigProvider, Effect, Layer, Redacted } from 'effect'
import * as FetchHttpClient from 'effect/unstable/http/FetchHttpClient'

import { testConnectionStoreLayer } from '../support'
import { SlackEmulator, slackEmulatorAliceToken, slackEmulatorBotToken } from './support/SlackEmulator'

const encoder = new TextEncoder()
const decoder = new TextDecoder()

layer(SlackEmulator.layer, { timeout: '30 seconds' })('Slack file emulator integration', (it) => {
	it.effect(
		'uploads and downloads public/private Slack files through production clients',
		() =>
			Effect.gen(function* () {
				const emulator = yield* SlackEmulator
				const credentials = SlackTenantCredentials.make({
					load: () =>
						Effect.succeedSome({
							botToken: Redacted.make(slackEmulatorBotToken),
							botUserId: 'U_CHANNELS_BOT',
							botId: 'B_CHANNELS_BOT',
						}),
					save: () => Effect.void,
				})
				const clientLayer = SlackClient.layerWith({ apiOrigin: new URL(`${emulator.emulator.url}/api`) }).pipe(
					Layer.provide(Layer.merge(FetchHttpClient.layer, credentials)),
					Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({}))),
				)
				const providerLayer = Slack.layer.pipe(
					Layer.provide(testConnectionStoreLayer),
					Layer.provide(clientLayer),
				)
				const publicRoot = yield* emulator.call(slackEmulatorAliceToken, 'chat.postMessage', {
					channel: emulator.publicChannelId,
					text: 'public file root',
				})
				const publicThread = slackThreadRef(
					SlackThreadRef.make({
						teamId: SlackTeamId.make(emulator.teamId),
						channelId: SlackChannelId.make(emulator.publicChannelId),
						threadTs: SlackMessageTs.make(publicRoot.ts),
					}),
					false,
				)
				const firstBytes = encoder.encode('public file payload')
				const secondBytes = encoder.encode('private file one')
				const thirdBytes = encoder.encode('private file two')
				const program = Effect.gen(function* () {
					const provider = yield* Slack
					const textAndFile = yield* provider.post({
						threadId: publicThread.id,
						content: MarkdownContent.make({
							markdown: 'public text and file',
							files: [
								FileUpload.make({ data: firstBytes, filename: 'public.txt', mimeType: 'text/plain' }),
							],
						}),
					})
					assert.deepStrictEqual(textAndFile.ref.degraded, [])
					assert.strictEqual(textAndFile.message.attachments.length, 1)
					const uploadedAttachment = textAndFile.message.attachments.at(0)
					if (uploadedAttachment === undefined) {
						return yield* Effect.die(new Error('Slack upload did not return an attachment'))
					}
					const downloaded = yield* provider.downloadAttachment({
						attachment: uploadedAttachment.ref,
					})
					assert.strictEqual(decoder.decode(downloaded), 'public file payload')

					const privateSent = yield* provider.postToChannel({
						channel: slackChannelRef(
							SlackTeamId.make(emulator.teamId),
							SlackChannelId.make(emulator.privateChannelId),
						),
						content: MarkdownContent.make({
							markdown: 'private multiple files',
							files: [
								FileUpload.make({ data: secondBytes, filename: 'private-one.txt' }),
								FileUpload.make({ data: thirdBytes, filename: 'private-two.txt' }),
							],
						}),
					})
					assert.deepStrictEqual(privateSent.ref.degraded, [])

					const onlyFile = yield* provider.post({
						threadId: publicThread.id,
						content: PlainTextContent.make({
							text: '',
							files: [FileUpload.make({ data: encoder.encode('file only'), filename: 'only.txt' })],
						}),
					})
					assert.strictEqual(onlyFile.message.text, '')
					return uploadedAttachment.ref.id
				})
				const uploadedFileId = yield* program.pipe(Effect.provide(providerLayer))

				const info = yield* emulator.call(slackEmulatorBotToken, 'files.info', { file: uploadedFileId })
				assert.strictEqual(info.file.name, 'public.txt')
				assert.strictEqual(info.file.size, firstBytes.byteLength)
				assert.ok(info.file.url_private.startsWith(emulator.emulator.url))

				const publicHistory = yield* emulator.call(slackEmulatorBotToken, 'conversations.replies', {
					channel: emulator.publicChannelId,
					ts: publicRoot.ts,
				})
				assert.ok(
					publicHistory.messages.some((message) => message.files?.some((file) => file.id === uploadedFileId)),
				)
				const privateHistory = yield* emulator.call(slackEmulatorBotToken, 'conversations.history', {
					channel: emulator.privateChannelId,
				})
				assert.ok(privateHistory.messages.some((message) => (message.files?.length ?? 0) === 2))
			}),
		{ timeout: 20_000 },
	)
})
