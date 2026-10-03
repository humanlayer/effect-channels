import { describe, it } from '@effect/vitest'
import { Effect, Queue, Stream } from 'effect'

import { SlackApiError, SlackFileAuthorizationError, SlackFileSizeLimitExceeded } from '../src/SlackApi'
import { SlackFile, slackFileFromMetadata } from '../src/SlackModels'
import {
	botToken,
	makeRecordingSlackHttp,
	privateDownloadUrl,
	type RecordedSlackRequest,
	type SlackHttpResponder,
	slackApiLayer,
	slackFileMetadata,
	teamId,
} from './slack-file-fixtures'

const bytes = new TextEncoder().encode('channels-live file body\n')
const file = slackFileFromMetadata(teamId, slackFileMetadata)
const fileFields = {
	ref: file.ref,
	name: file.name,
	contentType: file.contentType,
	size: file.size,
	downloadUrl: file.downloadUrl,
}
const missingFilesRead = SlackFileAuthorizationError.make({
	operation: 'download_file',
	requiredScope: 'files:read',
	retryable: false,
})

const chunked = (chunks: ReadonlyArray<Uint8Array>, fail = false) =>
	new ReadableStream<Uint8Array>({
		start(controller) {
			for (const chunk of chunks) controller.enqueue(chunk)
			if (fail) controller.error(new Error('connection reset'))
			else controller.close()
		},
	})

const redirect = (location: string) => () => new Response(null, { status: 302, headers: { location } })

const withRequests = <A, E>(run: (requests: Queue.Queue<RecordedSlackRequest>) => Effect.Effect<A, E>) =>
	Effect.flatMap(Queue.unbounded<RecordedSlackRequest>(), run)

const collect = (target: SlackFile, respond: SlackHttpResponder) =>
	withRequests((requests) =>
		target.download().pipe(
			Effect.flatMap(Stream.runCollect),
			Effect.map((chunks) => new Uint8Array(Array.from(chunks).flatMap((chunk) => Array.from(chunk)))),
			Effect.provide(slackApiLayer(makeRecordingSlackHttp(requests, respond))),
			Effect.flatMap((downloaded) =>
				Effect.map(Queue.clear(requests), (recorded) => ({ downloaded, requests: Array.from(recorded) })),
			),
		),
	)

const downloadFails = (target: SlackFile, respond: SlackHttpResponder, maxBytes = 1024) =>
	withRequests((requests) =>
		target.downloadBytes({ maxBytes }).pipe(
			Effect.provide(slackApiLayer(makeRecordingSlackHttp(requests, respond))),
			Effect.flip,
			Effect.flatMap((error) =>
				Effect.map(Queue.clear(requests), (recorded) => ({ error, requests: Array.from(recorded) })),
			),
		),
	)

describe('Slack file download', () => {
	it.effect('streams url_private_download with the bot token', ({ expect }) =>
		Effect.gen(function* () {
			const result = yield* collect(file, () => new Response(chunked([bytes.slice(0, 5), bytes.slice(5)])))
			expect(result.downloaded).toEqual(bytes)
			expect(result.requests.map(({ url, authorization }) => ({ url, authorization }))).toEqual([
				{ url: privateDownloadUrl, authorization: `Bearer ${botToken}` },
			])
		}),
	)

	it.effect('buffers bytes and follows redirects only within Slack file origins', ({ expect }) =>
		Effect.gen(function* () {
			const hop = `${privateDownloadUrl}?origin_team=${teamId}`
			const requests = yield* Queue.unbounded<RecordedSlackRequest>()
			const buffered = yield* file
				.downloadBytes({ maxBytes: bytes.byteLength })
				.pipe(
					Effect.provide(
						slackApiLayer(
							makeRecordingSlackHttp(requests, (request) =>
								request.url === privateDownloadUrl
									? redirect(hop)()
									: new Response(bytes, { headers: { 'content-length': String(bytes.byteLength) } }),
							),
						),
					),
				)
			expect(buffered).toEqual(bytes)
			const recorded = Array.from(yield* Queue.clear(requests))
			expect(recorded.map(({ url, authorization }) => ({ url, authorization }))).toEqual([
				{ url: privateDownloadUrl, authorization: `Bearer ${botToken}` },
				{ url: hop, authorization: `Bearer ${botToken}` },
			])
		}),
	)

	it.effect('names files:read for rejected, sign-in, and HTML responses', ({ expect }) =>
		Effect.gen(function* () {
			for (const status of [401, 403]) {
				const rejected = yield* downloadFails(file, () => new Response(null, { status }))
				expect(rejected.error).toEqual(missingFilesRead)
			}

			const signIn = yield* downloadFails(file, redirect('https://example.slack.com/?redir=%2Ffiles-pri%2F'))
			expect(signIn.error).toEqual(missingFilesRead)
			expect(signIn.requests).toHaveLength(1)

			const html = yield* downloadFails(
				file,
				() => new Response('<!DOCTYPE html><title>Slack</title>', { headers: { 'content-type': 'text/html' } }),
			)
			expect(html.error).toEqual(missingFilesRead)

			const htmlFile = SlackFile.make({ ...fileFields, contentType: 'text/html' })
			const page = yield* collect(
				htmlFile,
				() => new Response(bytes, { headers: { 'content-type': 'text/html; charset=utf-8' } }),
			)
			expect(page.downloaded).toEqual(bytes)
		}),
	)

	it.effect('enforces maxBytes from declared size, Content-Length, and received chunks', ({ expect }) =>
		Effect.gen(function* () {
			const declared = yield* downloadFails(file, () => new Response(bytes), 4)
			expect(declared.error).toEqual(
				SlackFileSizeLimitExceeded.make({ maxBytes: 4, observedBytes: 24, source: 'declared_size' }),
			)
			expect(declared.requests).toHaveLength(0)

			const unsized = SlackFile.make({ ...fileFields, size: null })
			const byLength = yield* downloadFails(
				unsized,
				() => new Response(bytes, { headers: { 'content-length': String(bytes.byteLength) } }),
				4,
			)
			expect(byLength.error).toEqual(
				SlackFileSizeLimitExceeded.make({
					maxBytes: 4,
					observedBytes: bytes.byteLength,
					source: 'content_length',
				}),
			)

			const received = yield* downloadFails(
				unsized,
				() => new Response(chunked([bytes.slice(0, 5), bytes.slice(5)])),
				8,
			)
			expect(received.error).toEqual(
				SlackFileSizeLimitExceeded.make({
					maxBytes: 8,
					observedBytes: bytes.byteLength,
					source: 'received_bytes',
				}),
			)
		}),
	)

	it.effect('rejects unapproved URLs and redirects without sending the token elsewhere', ({ expect }) =>
		Effect.gen(function* () {
			const cases = [
				{ downloadUrl: 'https://files.slack.com.example.com/files-pri/T-F/notes.txt', requests: 0 },
				{ downloadUrl: 'http://files.slack.com/files-pri/T-F/notes.txt', requests: 0 },
				{ downloadUrl: null, requests: 0 },
			] as const
			for (const testCase of cases) {
				const result = yield* downloadFails(
					SlackFile.make({ ...fileFields, downloadUrl: testCase.downloadUrl }),
					() => new Response(bytes),
				)
				expect(result.error).toBeInstanceOf(SlackApiError)
				expect(result.error).toMatchObject({ operation: 'download_file' })
				expect(result.requests).toHaveLength(testCase.requests)
			}

			const offOrigin = yield* downloadFails(file, redirect('https://cdn.example.com/notes.txt'))
			expect(offOrigin.error).toEqual(
				SlackApiError.make({
					operation: 'download_file',
					message: 'Slack file redirect target is not an approved Slack file origin',
				}),
			)
			expect(offOrigin.requests.map(({ url }) => url)).toEqual([privateDownloadUrl])

			const loop = yield* downloadFails(file, redirect(privateDownloadUrl))
			expect(loop.error).toEqual(
				SlackApiError.make({ operation: 'download_file', message: 'Slack file redirect limit exceeded' }),
			)
			expect(loop.requests).toHaveLength(4)
		}),
	)

	it.effect('classifies missing files and interrupted streams as typed API errors', ({ expect }) =>
		Effect.gen(function* () {
			const missing = yield* downloadFails(file, () => new Response(null, { status: 404 }))
			expect(missing.error).toEqual(
				SlackApiError.make({ operation: 'download_file', message: 'Slack file was not found' }),
			)

			const interrupted = yield* downloadFails(file, () => new Response(chunked([bytes.slice(0, 5)], true)))
			expect(interrupted.error).toEqual(
				SlackApiError.make({ operation: 'download_file', message: 'Slack file stream failed' }),
			)
		}),
	)
})
