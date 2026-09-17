import { assert, it } from '@effect/vitest'
import { Effect, Layer, Queue, Schema } from 'effect'

import {
	Attachment,
	AttachmentRef,
	FileUpload,
	MarkdownContent,
	PlainTextContent,
	TenantId,
	ThreadId,
} from '../src/index'
import { SlackFileUploadInput, SlackSentMessage, SlackThreadRef } from '../src/Schema'
import { Slack } from '../src/Slack'
import { SlackClient } from '../src/SlackClient'
import { normalizeSlackHistoryMessage, slackFileAttachments } from '../src/SlackNormalize'
import { slackThreadRef } from '../src/SlackThreadId'
import {
	makeSlackClientHarness,
	slackJsonResponse,
	testChannelId,
	testRootThreadId,
	testRootTs,
	testTeamId,
} from './support'

const encoder = new TextEncoder()

const file = (filename: string, text: string, mimeType?: string) =>
	mimeType === undefined
		? FileUpload.make({ filename, data: encoder.encode(text) })
		: FileUpload.make({ filename, data: encoder.encode(text), mimeType })

const uploadResponder = () => {
	let ticket = 0
	return (request: { readonly url: URL }) => {
		if (request.url.pathname.endsWith('/files.getUploadURLExternal')) {
			ticket += 1
			return slackJsonResponse(
				JSON.stringify({
					ok: true,
					upload_url: `https://files.slack.com/upload/v1/F_TEST_${ticket}`,
					file_id: `F_TEST_${ticket}`,
				}),
			)
		}
		if (request.url.hostname === 'files.slack.com' && request.url.pathname.startsWith('/upload/v1/')) {
			return new Response('OK')
		}
		if (request.url.pathname.endsWith('/files.completeUploadExternal')) {
			const completedTicketCount = ticket
			ticket = 0
			return slackJsonResponse(
				JSON.stringify({
					ok: true,
					files: Array.from({ length: completedTicketCount }, (_, index) => ({
						id: `F_TEST_${index + 1}`,
						name: `file-${index + 1}.txt`,
						mimetype: 'text/plain',
						shares: { public: { C_TEST: [{ ts: '100.9', thread_ts: '100.1' }] } },
					})),
				}),
			)
		}
		return slackJsonResponse(JSON.stringify({ ok: false, error: 'unexpected_method' }))
	}
}

it.effect('normalizes queue-safe Slack attachment metadata and reconstructs Attachment behavior', () =>
	Effect.gen(function* () {
		const [attachment] = slackFileAttachments(testTeamId, [
			{
				id: 'F_INBOUND',
				name: 'diagram.png',
				mimetype: 'image/png',
				size: 123,
				original_w: 640,
				original_h: 480,
				url_private: new URL('https://files.slack.com/files-pri/F_INBOUND/diagram.png'),
			},
		])
		assert.ok(Schema.is(Attachment)(attachment))
		assert.deepStrictEqual(attachment.ref.providerLocator, { id: 'F_INBOUND' })
		assert.strictEqual(attachment.ref.kind, 'image')
		assert.strictEqual(attachment.ref.width, 640)
		assert.strictEqual(attachment.ref.height, 480)

		const encoded = yield* Schema.encodeEffect(Attachment)(attachment)
		const decoded = yield* Schema.decodeEffect(Attachment)(encoded)
		assert.ok(Schema.is(Attachment)(decoded))
		assert.ok(Effect.isEffect(decoded.download()))
	}),
)

it.effect('keeps inbound files in provider-backed history normalization', () =>
	Effect.sync(() => {
		const threadRef = slackThreadRef(
			SlackThreadRef.make({ teamId: testTeamId, channelId: testChannelId, threadTs: testRootTs }),
			false,
		)
		const message = normalizeSlackHistoryMessage({
			teamId: testTeamId,
			threadRef,
			identity: { botUserId: 'U_BOT', botId: 'B_OURS' },
			snapshot: {
				ts: testRootTs,
				text: 'see attachment',
				user: 'U_HUMAN',
				files: [
					{
						id: 'F_HISTORY',
						name: 'notes.txt',
						mimetype: 'text/plain',
						size: 5,
						url_private: new URL('https://files.slack.com/files-pri/F_HISTORY/notes.txt'),
					},
				],
			},
		})
		assert.strictEqual(message.attachments.length, 1)
		assert.strictEqual(message.attachments[0]?.ref.id, 'F_HISTORY')
		assert.ok(Schema.is(Schema.Json)(message.raw))
	}),
)

it.effect('performs the uploadV2 sequence for multiple files without sending the token to upload URLs', () =>
	Effect.gen(function* () {
		const harness = yield* makeSlackClientHarness(uploadResponder())
		const program = Effect.gen(function* () {
			const client = yield* SlackClient
			const sent = yield* client.uploadFiles(
				SlackFileUploadInput.make({
					teamId: testTeamId,
					channelId: testChannelId,
					threadTs: testRootTs,
					initialComment: 'two files',
					files: [file('one.txt', 'one'), file('two.txt', 'two')],
				}),
			)
			assert.deepStrictEqual(
				sent.map((item) => item.fileId),
				['F_TEST_1', 'F_TEST_2'],
			)
			const requests = yield* Queue.takeAll(harness.requests)
			assert.deepStrictEqual(
				requests.map((request) => request.url.pathname),
				[
					'/api/files.getUploadURLExternal',
					'/api/files.getUploadURLExternal',
					'/upload/v1/F_TEST_1',
					'/upload/v1/F_TEST_2',
					'/api/files.completeUploadExternal',
				],
			)
			for (const request of requests.filter((item) => item.url.hostname === 'files.slack.com')) {
				assert.strictEqual(request.authorization, null)
			}
			const completion = requests.at(-1)
			assert.ok(completion?.authorization?.startsWith('Bearer '))
			assert.match(completion?.body ?? '', /"initial_comment":"two files"/)
		})
		yield* program.pipe(Effect.provide(harness.layer))
	}),
)

it.effect('composes text-plus-file and file-only thread posts without file degradation', () =>
	Effect.gen(function* () {
		const harness = yield* makeSlackClientHarness(uploadResponder())
		const providerLayer = Slack.layer.pipe(Layer.provide(harness.layer))
		const program = Effect.gen(function* () {
			const provider = yield* Slack
			const withText = yield* provider.post({
				threadId: ThreadId.make(testRootThreadId),
				content: MarkdownContent.make({ markdown: 'report', files: [file('report.txt', 'payload')] }),
			})
			assert.deepStrictEqual(withText.ref.degraded, [])
			assert.strictEqual(withText.message.text, 'report')
			assert.strictEqual(withText.message.attachments.length, 1)
			assert.strictEqual(withText.message.attachments[0]?.ref.id, 'F_TEST_1')

			const onlyFile = yield* provider.post({
				threadId: ThreadId.make(testRootThreadId),
				content: PlainTextContent.make({ text: '', files: [file('only.txt', 'only')] }),
			})
			assert.deepStrictEqual(onlyFile.ref.degraded, [])
			assert.strictEqual(onlyFile.message.text, '')
		})
		yield* program.pipe(Effect.provide(providerLayer))
	}),
)

it.effect('constructs trusted uploads with FileUpload.make', () =>
	Effect.sync(() => {
		const upload = file('trusted.txt', 'trusted', 'text/plain')
		assert.strictEqual(upload.filename, 'trusted.txt')
		assert.deepStrictEqual([...upload.data], [...encoder.encode('trusted')])
		const attachment = Attachment.make({
			ref: AttachmentRef.make({
				provider: 'slack',
				tenant: TenantId.make(testTeamId),
				id: 'F_TRUSTED',
				kind: 'file',
				providerLocator: { id: 'F_TRUSTED' },
			}),
		})
		assert.ok(Schema.is(Attachment)(attachment))
		assert.ok(SlackSentMessage.make({ channelId: testChannelId, ts: testRootTs, fileId: 'F_TRUSTED' }))
	}),
)
