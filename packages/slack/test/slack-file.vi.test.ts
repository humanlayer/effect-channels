import { describe, it } from '@effect/vitest'
import { Effect, Exit, Layer, Queue, Schema, Stream } from 'effect'

import { SlackApi, SlackDownloadFileBytesRequest, SlackDownloadFileRequest } from '../src/SlackApi'
import { SlackChannel } from '../src/SlackChannel'
import {
	SlackFile,
	SlackFileId,
	SlackFileRef,
	slackFileFromMetadata,
	SlackMarkdownContent,
	SlackMessage,
	SlackMessageRef,
	SlackParticipant,
	SlackUploadFileInput,
	SlackUserId,
	slackMaxUploadBytes,
} from '../src/SlackModels'
import { SlackThread } from '../src/SlackThread'
import { SlackFileMetadata } from '../src/SlackWebhookEventSchemas'
import {
	channelRef,
	fileId,
	privateDownloadUrl,
	privateUrl,
	rootTs,
	slackFileMetadata,
	slackFileObject,
	teamId,
	threadRef,
} from './slack-file-fixtures'

const ref = SlackFileRef.make({ teamId, fileId: SlackFileId.make(fileId) })
const file = slackFileFromMetadata(teamId, slackFileMetadata)
const bytes = new TextEncoder().encode('hello')

describe('SlackFile values', () => {
	it.effect('normalizes provider metadata and prefers url_private_download', ({ expect }) =>
		Effect.gen(function* () {
			const decoded = yield* Schema.decodeEffect(SlackFileMetadata)(slackFileObject)
			expect(slackFileFromMetadata(teamId, decoded)).toEqual(
				SlackFile.make({
					ref,
					name: 'notes.txt',
					contentType: 'text/plain',
					size: 24,
					downloadUrl: privateDownloadUrl,
				}),
			)
			expect(
				slackFileFromMetadata(
					teamId,
					SlackFileMetadata.make({ id: fileId, url_private: privateUrl, url_private_download: '' }),
				).downloadUrl,
			).toBe(privateUrl)
		}),
	)

	it.effect('turns hidden, tombstoned, and sparse files into explicit nulls', ({ expect }) =>
		Effect.gen(function* () {
			const hidden = yield* Schema.decodeUnknownEffect(SlackFileMetadata)({ id: fileId, mode: 'hidden_by_limit' })
			expect(slackFileFromMetadata(teamId, hidden)).toEqual(
				SlackFile.make({ ref, name: null, contentType: null, size: null, downloadUrl: null }),
			)
			expect(
				slackFileFromMetadata(teamId, SlackFileMetadata.make({ id: fileId, name: '', mimetype: '' })),
			).toMatchObject({ name: null, contentType: null })
		}),
	)

	it.effect('round-trips messages with files through their JSON encoding', ({ expect }) =>
		Effect.gen(function* () {
			const message = SlackMessage.make({
				ref: SlackMessageRef.make({ teamId, channelId: threadRef.channelId, messageTs: rootTs }),
				thread: threadRef,
				author: SlackParticipant.make({
					userId: SlackUserId.make('U061F7AUR'),
					userName: 'alice',
					fullName: 'Alice Example',
					isBot: false,
					isMe: false,
				}),
				content: SlackMarkdownContent.make({ markdown: 'see attached' }),
				files: [file],
				metadata: {},
			})
			const codec = Schema.toCodecJson(SlackMessage)
			const decoded = yield* Schema.decodeEffect(codec)(yield* Schema.encodeEffect(codec)(message))
			expect(decoded).toEqual(message)
			expect(decoded.files[0]).toBeInstanceOf(SlackFile)
		}),
	)

	it('accepts only non-empty uploads within the in-memory bound', ({ expect }) => {
		const decode = Schema.decodeUnknownExit(SlackUploadFileInput)
		expect(Exit.isSuccess(decode({ filename: 'notes.txt', bytes }))).toBe(true)
		expect(Exit.isFailure(decode({ filename: 'notes.txt', bytes: new Uint8Array() }))).toBe(true)
		expect(Exit.isFailure(decode({ filename: 'big.bin', bytes: new Uint8Array(slackMaxUploadBytes + 1) }))).toBe(
			true,
		)
		expect(Exit.isFailure(decode({ filename: '', bytes }))).toBe(true)
	})
})

describe('Slack file resources', () => {
	it.effect('delegate downloads and uploads to SlackApi with their stored refs', ({ expect }) =>
		Effect.gen(function* () {
			const calls = yield* Queue.unbounded<unknown>()
			const api = Layer.mock(SlackApi, {
				downloadFile: (request) =>
					Queue.offer(calls, { operation: 'downloadFile', request }).pipe(Effect.as(Stream.make(bytes))),
				downloadFileBytes: (request) =>
					Queue.offer(calls, { operation: 'downloadFileBytes', request }).pipe(Effect.as(bytes)),
				uploadFileToChannel: (request) =>
					Queue.offer(calls, { operation: 'uploadFileToChannel', request }).pipe(Effect.as(file)),
				uploadFileToThread: (request) =>
					Queue.offer(calls, { operation: 'uploadFileToThread', request }).pipe(Effect.as(file)),
			})
			const input = SlackUploadFileInput.make({ filename: 'notes.txt', bytes })
			const download = {
				file: ref,
				downloadUrl: privateDownloadUrl,
				contentType: 'text/plain',
				size: 24,
			}

			expect(yield* file.download().pipe(Effect.flatMap(Stream.runCollect), Effect.provide(api))).toHaveLength(1)
			yield* file.downloadBytes({ maxBytes: 100 }).pipe(Effect.provide(api))
			yield* SlackChannel.make({ ref: channelRef }).uploadFile(input).pipe(Effect.provide(api))
			yield* SlackThread.make({ ref: threadRef, mailboxKey: 'mailbox:file' })
				.uploadFile(input)
				.pipe(Effect.provide(api))

			expect(Array.from(yield* Queue.clear(calls))).toEqual([
				{ operation: 'downloadFile', request: SlackDownloadFileRequest.make(download) },
				{
					operation: 'downloadFileBytes',
					request: SlackDownloadFileBytesRequest.make({ ...download, maxBytes: 100 }),
				},
				{ operation: 'uploadFileToChannel', request: { channel: channelRef, input } },
				{ operation: 'uploadFileToThread', request: { thread: threadRef, input } },
			])
		}),
	)
})
