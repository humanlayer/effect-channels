import { describe, it } from '@effect/vitest'
import { Effect, Fiber, Ref, Schema } from 'effect'
import { TestClock } from 'effect/testing'
import { HttpClient, HttpClientResponse } from 'effect/unstable/http'

import { GetIssueVariables } from '../src/api/GetIssue'
import { GetViewerIdentityVariables } from '../src/api/GetViewerIdentity'
import { UpdateIssueVariables } from '../src/api/UpdateIssue'
import { LinearApi } from '../src/LinearApi'
import { apiLayer, decodeGraphqlRequest, issue, issueJson, viewer } from './api-test-fixtures'

const decodeRequest = decodeGraphqlRequest(
	Schema.Union([UpdateIssueVariables, GetIssueVariables, GetViewerIdentityVariables]),
)

describe('Linear API retry policy', () => {
	it.effect('retries reads but never blanket-retries ambiguous mutations', ({ expect }) =>
		Effect.gen(function* () {
			const reads = yield* Ref.make(0)
			const mutations = yield* Ref.make(0)
			const http = HttpClient.make((request) =>
				Effect.gen(function* () {
					const body = yield* decodeRequest(request)
					if (body.query.includes('viewer')) return HttpClientResponse.fromWeb(request, Response.json(viewer))
					if (body.query.includes('mutation')) {
						yield* Ref.update(mutations, (n) => n + 1)
						return HttpClientResponse.fromWeb(request, Response.json({}, { status: 503 }))
					}
					const count = yield* Ref.updateAndGet(reads, (n) => n + 1)
					return HttpClientResponse.fromWeb(
						request,
						count === 1
							? Response.json({}, { status: 503, headers: { 'retry-after': '0' } })
							: Response.json({ data: { issue: issueJson } }),
					)
				}),
			)
			const layer = apiLayer(http)
			const operation = Effect.flatMap(LinearApi, (api) => api.getIssue({ issue })).pipe(Effect.provide(layer))
			const read = yield* Effect.forkChild(operation)
			yield* TestClock.adjust('1 second')
			yield* Fiber.join(read)
			yield* Effect.flatMap(LinearApi, (api) => api.updateIssue({ issue, update: { priority: 1 } })).pipe(
				Effect.provide(layer),
				Effect.flip,
			)
			expect(yield* Ref.get(reads)).toBe(2)
			expect(yield* Ref.get(mutations)).toBe(1)
		}),
	)

	it.effect('decodes an HTTP 400 RATELIMITED envelope and retries the read with extension metadata', ({ expect }) =>
		Effect.gen(function* () {
			const reads = yield* Ref.make(0)
			const http = HttpClient.make((request) =>
				Effect.gen(function* () {
					const body = yield* decodeRequest(request)
					if (body.query.includes('viewer')) return HttpClientResponse.fromWeb(request, Response.json(viewer))
					const count = yield* Ref.updateAndGet(reads, (n) => n + 1)
					return HttpClientResponse.fromWeb(
						request,
						count === 1
							? Response.json(
									{
										errors: [
											{
												message: 'slow down',
												extensions: { code: 'RATELIMITED', statusCode: 429, retryAfterMs: 0 },
											},
										],
									},
									{ status: 400 },
								)
							: Response.json({ data: { issue: issueJson } }),
					)
				}),
			)
			const operation = Effect.flatMap(LinearApi, (api) => api.getIssue({ issue })).pipe(
				Effect.provide(apiLayer(http)),
			)
			const read = yield* Effect.forkChild(operation)
			yield* TestClock.adjust('1 second')
			const result = yield* Fiber.join(read)
			expect(result.identifier).toBe('CORE-1')
			expect(yield* Ref.get(reads)).toBe(2)
		}),
	)
})
