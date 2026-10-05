import { describe, it } from '@effect/vitest'
import { Effect, Queue, Redacted, Stream } from 'effect'
import { HttpClient, HttpClientRequest, HttpClientResponse } from 'effect/http'

import { narrowLinearProviderErrors } from '../src/api/LinearApiErrors'
import { LinearHttpClient, makeFixedCredentialLinearHttpClient } from '../src/api/LinearHttpClient'
import { LinearApi } from '../src/LinearApi'
import { LinearFile, LinearFileRef, LinearFileSizeLimitExceeded, LinearFileUrl } from '../src/LinearFiles'
import { LinearOrganizationId } from '../src/LinearIdentity'
import { apiLayer, issue, viewer } from './api-test-fixtures'

const fileUrl =
	'https://uploads.linear.app/8f1d3c4e-1111-4222-8333-944455556666/11111111-1111-4111-8111-111111111111/0b1c2d3e-aaaa-4bbb-8ccc-ddddeeeeffff'
const signedUrl = 'https://files.linear-cdn.example/object?signature=signed-secret'
const bytes = new TextEncoder().encode('hello linear file')

type RecordedRequest = {
	readonly url: string
	readonly authorization: string | null
}

type FileResponder = (url: string) => Response

const makeHttp = (requests: Queue.Queue<RecordedRequest>, respond: FileResponder) =>
	HttpClient.make((request) =>
		Effect.gen(function* () {
			const web = yield* HttpClientRequest.toWeb(request).pipe(Effect.orDie)
			yield* Queue.offer(requests, { url: web.url, authorization: web.headers.get('authorization') })
			if (web.url === 'https://api.linear.app/graphql')
				return HttpClientResponse.fromWeb(request, Response.json(viewer))
			return HttpClientResponse.fromWeb(request, respond(web.url))
		}),
	)

const chunked = (chunks: ReadonlyArray<Uint8Array>, fail = false) =>
	new ReadableStream<Uint8Array>({
		start(controller) {
			for (const chunk of chunks) controller.enqueue(chunk)
			if (fail) controller.error(new Error('connection reset'))
			else controller.close()
		},
	})

const file = (size: number | null = null) =>
	LinearFile.make({
		ref: LinearFileRef.make({ organizationId: issue.organizationId, issueId: issue.issueId }),
		url: LinearFileUrl.make(fileUrl),
		name: 'notes.txt',
		contentType: 'text/plain',
		size,
	})

const fileDownloads = (recorded: ReadonlyArray<RecordedRequest>) =>
	recorded.filter((request) => request.url !== 'https://api.linear.app/graphql')

describe('Linear file download', () => {
	it.effect('streams authenticated bytes from the approved Linear upload origin', ({ expect }) =>
		Effect.gen(function* () {
			const requests = yield* Queue.unbounded<RecordedRequest>()
			const http = makeHttp(requests, () => new Response(chunked([bytes.slice(0, 5), bytes.slice(5)])))
			const collected = yield* file()
				.download()
				.pipe(Effect.flatMap(Stream.runCollect), Effect.provide(apiLayer(http)))
			expect(new Uint8Array(collected.flatMap((chunk) => Array.from(chunk)))).toEqual(bytes)
			const downloads = fileDownloads(Array.from(yield* Queue.takeAll(requests)))
			expect(downloads).toEqual([{ url: fileUrl, authorization: 'Bearer token-never-log' }])
		}),
	)

	it.effect('follows redirects manually and drops the credential for other origins', ({ expect }) =>
		Effect.gen(function* () {
			const requests = yield* Queue.unbounded<RecordedRequest>()
			const http = makeHttp(requests, (url) => {
				if (url === fileUrl)
					return new Response(null, { status: 302, headers: { location: `${fileUrl}?hop=1` } })
				if (url.startsWith('https://uploads.linear.app/'))
					return new Response(null, { status: 307, headers: { location: signedUrl } })
				return new Response(bytes, { headers: { 'content-length': String(bytes.byteLength) } })
			})
			const buffered = yield* file(bytes.byteLength)
				.downloadBytes({ maxBytes: 1024 })
				.pipe(Effect.provide(apiLayer(http)))
			expect(buffered).toEqual(bytes)
			const downloads = fileDownloads(Array.from(yield* Queue.takeAll(requests)))
			expect(downloads.map((request) => request.authorization)).toEqual([
				'Bearer token-never-log',
				'Bearer token-never-log',
				null,
			])
			expect(downloads[2]?.url).toBe(signedUrl)
		}),
	)

	it.effect('rejects exhausted, missing, and insecure redirects without contacting the target', ({ expect }) =>
		Effect.gen(function* () {
			const cases = [
				{ location: fileUrl, reason: 'invalid_response', requests: 4 },
				{ location: null, reason: 'invalid_response', requests: 1 },
				{ location: 'http://files.linear-cdn.example/object', reason: 'invalid_response', requests: 1 },
			] as const
			for (const testCase of cases) {
				const requests = yield* Queue.unbounded<RecordedRequest>()
				const http = makeHttp(requests, () =>
					testCase.location === null
						? new Response(null, { status: 302 })
						: new Response(null, { status: 302, headers: { location: testCase.location } }),
				)
				const error = yield* file()
					.download()
					.pipe(Effect.provide(apiLayer(http)), Effect.flip)
				expect(error).toMatchObject({ operation: 'download_file', reason: testCase.reason, retryable: false })
				const downloads = fileDownloads(Array.from(yield* Queue.takeAll(requests)))
				expect(downloads).toHaveLength(testCase.requests)
				expect(downloads.every((request) => request.url.startsWith('https://'))).toBe(true)
			}
		}),
	)

	it.effect('enforces maxBytes from declared size, Content-Length, and received chunks', ({ expect }) =>
		Effect.gen(function* () {
			const declaredRequests = yield* Queue.unbounded<RecordedRequest>()
			const declared = yield* file(bytes.byteLength)
				.downloadBytes({ maxBytes: 4 })
				.pipe(Effect.provide(apiLayer(makeHttp(declaredRequests, () => new Response(bytes)))), Effect.flip)
			expect(declared).toBeInstanceOf(LinearFileSizeLimitExceeded)
			expect(declared).toMatchObject({
				maxBytes: 4,
				observedBytes: bytes.byteLength,
				source: 'declared_size',
			})
			expect(yield* Queue.size(declaredRequests)).toBe(0)

			const lengthRequests = yield* Queue.unbounded<RecordedRequest>()
			const byLength = yield* file()
				.downloadBytes({ maxBytes: 4 })
				.pipe(
					Effect.provide(
						apiLayer(
							makeHttp(
								lengthRequests,
								() => new Response(bytes, { headers: { 'content-length': String(bytes.byteLength) } }),
							),
						),
					),
					Effect.flip,
				)
			expect(byLength).toBeInstanceOf(LinearFileSizeLimitExceeded)
			expect(byLength).toMatchObject({ source: 'content_length' })

			const streamRequests = yield* Queue.unbounded<RecordedRequest>()
			const received = yield* file()
				.downloadBytes({ maxBytes: 8 })
				.pipe(
					Effect.provide(
						apiLayer(
							makeHttp(streamRequests, () => new Response(chunked([bytes.slice(0, 5), bytes.slice(5)]))),
						),
					),
					Effect.flip,
				)
			expect(received).toBeInstanceOf(LinearFileSizeLimitExceeded)
			expect(received).toMatchObject({
				maxBytes: 8,
				observedBytes: bytes.byteLength,
				source: 'received_bytes',
			})
		}),
	)

	it.effect('classifies provider statuses and interrupted streams as typed API errors', ({ expect }) =>
		Effect.gen(function* () {
			const requests = yield* Queue.unbounded<RecordedRequest>()
			const missing = yield* file()
				.download()
				.pipe(
					Effect.provide(apiLayer(makeHttp(requests, () => new Response(null, { status: 404 })))),
					Effect.flip,
				)
			expect(missing).toMatchObject({ operation: 'download_file', reason: 'not_found', retryable: false })

			const unauthorized = yield* file()
				.download()
				.pipe(
					Effect.provide(apiLayer(makeHttp(requests, () => new Response(null, { status: 401 })))),
					Effect.flip,
				)
			expect(unauthorized).toMatchObject({ operation: 'download_file', reason: 'unauthorized' })

			const interrupted = yield* file()
				.downloadBytes({ maxBytes: 1024 })
				.pipe(
					Effect.provide(apiLayer(makeHttp(requests, () => new Response(chunked([bytes], true))))),
					Effect.flip,
				)
			expect(interrupted).toMatchObject({ operation: 'download_file', reason: 'unavailable', retryable: true })
		}),
	)

	it.effect('attaches the credential only to HTTPS requests on the Linear upload origin', ({ expect }) =>
		Effect.gen(function* () {
			const requests = yield* Queue.unbounded<RecordedRequest>()
			const client = makeFixedCredentialLinearHttpClient(
				makeHttp(requests, () => new Response(null, { status: 200 })),
				issue.organizationId,
				Redacted.make('secret-token'),
			)
			const execute = (url: string) =>
				Effect.flatMap(LinearHttpClient, (transport) =>
					transport.executeFile({ operation: 'download_file', request: HttpClientRequest.get(url) }),
				).pipe(Effect.provideService(LinearHttpClient, client))
			yield* execute(fileUrl)
			yield* execute('https://uploads.linear.app.example.com/a/b/c')
			const insecure = yield* execute('http://uploads.linear.app/a/b/c').pipe(
				narrowLinearProviderErrors,
				Effect.flip,
			)
			expect(insecure).toMatchObject({ operation: 'download_file', reason: 'validation' })
			expect(Array.from(yield* Queue.takeAll(requests))).toEqual([
				{ url: fileUrl, authorization: 'Bearer secret-token' },
				{ url: 'https://uploads.linear.app.example.com/a/b/c', authorization: null },
			])
		}),
	)

	it.effect('rejects organization mismatches before any file request', ({ expect }) =>
		Effect.gen(function* () {
			const requests = yield* Queue.unbounded<RecordedRequest>()
			const foreign = LinearFile.make({
				ref: LinearFileRef.make({
					organizationId: LinearOrganizationId.make('other-org'),
					issueId: issue.issueId,
				}),
				url: LinearFileUrl.make(fileUrl),
				name: null,
				contentType: null,
				size: null,
			})
			const error = yield* Effect.flatMap(LinearApi, (api) =>
				api.downloadFile({ file: foreign.ref, url: foreign.url, size: null }),
			).pipe(Effect.provide(apiLayer(makeHttp(requests, () => new Response(bytes)))), Effect.flip)
			expect(error).toMatchObject({ reason: 'identity_mismatch' })
			expect(yield* Queue.size(requests)).toBe(0)
		}),
	)
})
