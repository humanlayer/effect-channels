import { describe, it } from '@effect/vitest'
import { Effect, Layer, Match, Queue, Redacted, Schema } from 'effect'
import { HttpClient, HttpClientRequest, HttpClientResponse } from 'effect/http'

import { narrowLinearProviderErrors } from '../src/api/LinearApiErrors'
import { LinearHttpClient, makeFixedCredentialLinearHttpClient } from '../src/api/LinearHttpClient'
import { requestLinearFileUpload } from '../src/api/RequestLinearFileUpload'
import { LinearApi } from '../src/LinearApi'
import { LinearFile } from '../src/LinearFiles'
import { LinearIssueAttachment } from '../src/LinearResources'
import { apiLayer, issue, viewer } from './api-test-fixtures'

const assetUrl =
	'https://uploads.linear.app/8f1d3c4e-1111-4222-8333-944455556666/11111111-1111-4111-8111-111111111111/0b1c2d3e-aaaa-4bbb-8ccc-ddddeeeeffff'
const uploadUrl = 'https://storage.googleapis.com/linear-uploads/object?X-Goog-Signature=signed-secret'
const bytes = new TextEncoder().encode('channels-live file body\n')
const input = { filename: 'notes.txt', contentType: 'text/plain', bytes }

type RecordedRequest = {
	readonly url: string
	readonly method: string
	readonly authorization: string | null
	readonly headers: Readonly<Record<string, string>>
	readonly operation: string | null
	readonly variables: unknown
	readonly body: Uint8Array
}

type UploadScript = {
	readonly fileUpload?: unknown
	readonly putStatus?: number
	readonly attachmentSuccess?: boolean
}

const linearReturnedStorageKey = assetUrl.replace('https://uploads.linear.app/', '')
const uploadTarget = {
	assetUrl,
	uploadUrl,
	filename: linearReturnedStorageKey,
	contentType: 'text/plain',
	size: bytes.byteLength,
	headers: [
		{ key: 'Cache-Control', value: 'public, max-age=31536000' },
		{ key: 'x-goog-meta-source', value: 'linear' },
	],
}

const GraphqlRequestBody = Schema.fromJsonString(Schema.Struct({ query: Schema.String, variables: Schema.Json }))

const makeHttp = (requests: Queue.Queue<RecordedRequest>, script: UploadScript = {}) =>
	HttpClient.make((request) =>
		Effect.gen(function* () {
			const web = yield* HttpClientRequest.toWeb(request).pipe(Effect.orDie)
			const body = new Uint8Array(yield* Effect.promise(() => web.arrayBuffer()))
			const isGraphql = web.url === 'https://api.linear.app/graphql'
			const parsed = isGraphql
				? yield* Schema.decodeEffect(GraphqlRequestBody)(new TextDecoder().decode(body)).pipe(Effect.orDie)
				: null
			const operation =
				parsed === null ? null : (/(?:query|mutation) (Linear\w+)/.exec(parsed.query)?.[1] ?? null)
			yield* Queue.offer(requests, {
				url: web.url,
				method: web.method,
				authorization: web.headers.get('authorization'),
				headers: Object.fromEntries(web.headers.entries()),
				operation,
				variables: parsed?.variables ?? null,
				body,
			})
			if (web.url.startsWith('https://storage.googleapis.com/'))
				return HttpClientResponse.fromWeb(request, new Response(null, { status: script.putStatus ?? 200 }))
			const response = yield* Match.value(operation).pipe(
				Match.when('LinearViewerIdentity', () => Effect.succeed(Response.json(viewer))),
				Match.when('LinearFileUpload', () =>
					Effect.succeed(
						Response.json(
							script.fileUpload ?? { data: { fileUpload: { success: true, uploadFile: uploadTarget } } },
						),
					),
				),
				Match.when('LinearAttachmentCreate', () =>
					Effect.succeed(
						Response.json({
							data: {
								attachmentCreate: {
									success: script.attachmentSuccess ?? true,
									attachment: {
										id: 'attachment-1',
										title: 'Notes',
										subtitle: '0.0 KB',
										url: assetUrl,
										metadata: { contentType: 'text/plain', size: bytes.byteLength },
									},
								},
							},
						}),
					),
				),
				Match.orElse(() => Effect.die(`unexpected request ${web.method} ${web.url}`)),
			)
			return HttpClientResponse.fromWeb(request, response)
		}),
	)

const operationsOf = (recorded: ReadonlyArray<RecordedRequest>) =>
	recorded.map((request) => request.operation ?? `${request.method} ${new URL(request.url).host}`)

describe('Linear file upload', () => {
	it.effect('owns the fileUpload document, variables, and signed-target decoding', ({ expect }) =>
		Effect.gen(function* () {
			const requests = yield* Queue.unbounded<RecordedRequest>()
			const layer = Layer.succeed(
				LinearHttpClient,
				makeFixedCredentialLinearHttpClient(makeHttp(requests), issue.organizationId, Redacted.make('t')),
			)
			const target = yield* requestLinearFileUpload({ issue, input }).pipe(Effect.provide(layer))
			const [request] = Array.from(yield* Queue.takeAll(requests))
			expect(request?.operation).toBe('LinearFileUpload')
			expect(request?.variables).toEqual({
				contentType: 'text/plain',
				filename: 'notes.txt',
				size: bytes.byteLength,
			})
			expect(target.assetUrl).toBe(assetUrl)
			expect(Redacted.value(target.uploadUrl)).toBe(uploadUrl)
			expect(Redacted.isRedacted(target.uploadUrl)).toBe(true)
		}),
	)

	it.effect('uploads exact bytes to the signed target without the Linear bearer credential', ({ expect }) =>
		Effect.gen(function* () {
			const requests = yield* Queue.unbounded<RecordedRequest>()
			const file = yield* Effect.flatMap(LinearApi, (api) => api.uploadFile({ issue, input })).pipe(
				Effect.provide(apiLayer(makeHttp(requests))),
			)
			const recorded = Array.from(yield* Queue.takeAll(requests))
			expect(operationsOf(recorded)).toEqual([
				'LinearViewerIdentity',
				'LinearFileUpload',
				'PUT storage.googleapis.com',
			])
			const put = recorded[2]
			expect(put?.url).toBe(uploadUrl)
			expect(put?.authorization).toBeNull()
			expect(put?.body).toEqual(bytes)
			expect(put?.headers).toMatchObject({
				'content-type': 'text/plain',
				'cache-control': 'public, max-age=31536000',
				'x-goog-meta-source': 'linear',
			})
			expect(recorded.slice(0, 2).every((request) => request.authorization === 'Bearer token-never-log')).toBe(
				true,
			)
			expect(file).toBeInstanceOf(LinearFile)
			expect(file).toMatchObject({
				ref: { organizationId: issue.organizationId, issueId: issue.issueId },
				url: assetUrl,
				name: 'notes.txt',
				contentType: 'text/plain',
				size: bytes.byteLength,
			})
			expect(Object.values(file)).not.toContain(uploadUrl)
		}),
	)

	it.effect('creates a first-class issue attachment for the uploaded asset', ({ expect }) =>
		Effect.gen(function* () {
			const requests = yield* Queue.unbounded<RecordedRequest>()
			const attachment = yield* Effect.flatMap(LinearApi, (api) =>
				api.uploadAttachment({ issue, input: { ...input, title: 'Notes', metadata: { source: 'test' } } }),
			).pipe(Effect.provide(apiLayer(makeHttp(requests))))
			const recorded = Array.from(yield* Queue.takeAll(requests))
			expect(operationsOf(recorded)).toEqual([
				'LinearViewerIdentity',
				'LinearFileUpload',
				'PUT storage.googleapis.com',
				'LinearAttachmentCreate',
			])
			expect(recorded[3]?.variables).toEqual({
				input: {
					issueId: issue.issueId,
					url: assetUrl,
					title: 'Notes',
					subtitle: '0.0 KB',
					metadata: { contentType: 'text/plain', size: bytes.byteLength, source: 'test' },
				},
			})
			expect(attachment).toBeInstanceOf(LinearIssueAttachment)
			expect(attachment.url).toBe(assetUrl)
		}),
	)

	it.effect('reports request-target, byte-transfer, and attachment failures as separate stages', ({ expect }) =>
		Effect.gen(function* () {
			const run = (script: UploadScript, attach: boolean) =>
				Effect.gen(function* () {
					const requests = yield* Queue.unbounded<RecordedRequest>()
					const upload = Effect.flatMap(LinearApi, (api) =>
						attach
							? Effect.asVoid(api.uploadAttachment({ issue, input }))
							: Effect.asVoid(api.uploadFile({ issue, input })),
					)
					const error = yield* upload.pipe(Effect.provide(apiLayer(makeHttp(requests, script))), Effect.flip)
					return { error, operations: operationsOf(Array.from(yield* Queue.takeAll(requests))) }
				})

			const rejectedTarget = yield* run(
				{ fileUpload: { data: { fileUpload: { success: false, uploadFile: null } } } },
				false,
			)
			expect(rejectedTarget.error).toMatchObject({ operation: 'request_file_upload', reason: 'rejected' })
			expect(rejectedTarget.operations).toEqual(['LinearViewerIdentity', 'LinearFileUpload'])

			const lookalikeAsset = yield* run(
				{
					fileUpload: {
						data: {
							fileUpload: {
								success: true,
								uploadFile: {
									...uploadTarget,
									assetUrl: 'https://uploads.linear.app.example.com/a/b/c',
								},
							},
						},
					},
				},
				false,
			)
			expect(lookalikeAsset.error).toMatchObject({ operation: 'request_file_upload', reason: 'invalid_response' })
			expect(lookalikeAsset.operations).toEqual(['LinearViewerIdentity', 'LinearFileUpload'])

			const unavailable = yield* run({ putStatus: 503 }, true)
			expect(unavailable.error).toMatchObject({
				operation: 'upload_file_bytes',
				reason: 'unavailable',
				retryable: true,
				status: 503,
			})
			expect(unavailable.operations).toEqual([
				'LinearViewerIdentity',
				'LinearFileUpload',
				'PUT storage.googleapis.com',
			])

			const expiredTarget = yield* run({ putStatus: 403 }, false)
			expect(expiredTarget.error).toMatchObject({
				operation: 'upload_file_bytes',
				reason: 'rejected',
				retryable: false,
			})
			expect(expiredTarget.operations).toHaveLength(3)

			const attachmentFailed = yield* run({ attachmentSuccess: false }, true)
			expect(attachmentFailed.error).toMatchObject({ operation: 'create_attachment', reason: 'rejected' })
			expect(attachmentFailed.operations).toHaveLength(4)
		}),
	)

	it.effect('narrows a failed fileUpload GraphQL response through the shared provider errors', ({ expect }) =>
		Effect.gen(function* () {
			const requests = yield* Queue.unbounded<RecordedRequest>()
			const layer = Layer.succeed(
				LinearHttpClient,
				makeFixedCredentialLinearHttpClient(
					makeHttp(requests, {
						fileUpload: { errors: [{ message: 'too large', extensions: { code: 'BAD_USER_INPUT' } }] },
					}),
					issue.organizationId,
					Redacted.make('t'),
				),
			)
			const error = yield* requestLinearFileUpload({ issue, input }).pipe(
				Effect.provide(layer),
				narrowLinearProviderErrors,
				Effect.flip,
			)
			expect(error).toMatchObject({ operation: 'request_file_upload', reason: 'validation', retryable: false })
		}),
	)
})
