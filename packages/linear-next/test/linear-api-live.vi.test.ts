import { describe, it } from '@effect/vitest'
import { Config, Effect, Layer, Redacted, Ref } from 'effect'
import { HttpClient, HttpClientRequest, HttpClientResponse } from 'effect/unstable/http'

import { LinearAuth } from '../src/index'
import { LinearApi } from '../src/LinearApi'
import { LinearApiLiveOptions, makeLinearApiLiveBase } from '../src/LinearApiLive'
import { LinearOrganizationId } from '../src/LinearIdentity'
import { apiLayer, appUserId, issue, issueJson, organizationId, viewer } from './api-test-fixtures'

describe('LinearApiLive', () => {
	it.effect('verifies identity then executes named reads and mutations', ({ expect }) =>
		Effect.gen(function* () {
			const calls = yield* Ref.make<string[]>([])
			const http = HttpClient.make((request) =>
				Effect.gen(function* () {
					const web = yield* HttpClientRequest.toWeb(request).pipe(Effect.orDie)
					expect(web.headers.get('authorization')).toBe('Bearer token-never-log')
					const body = (yield* Effect.promise(() => web.json())) as { query: string }
					yield* Ref.update(calls, (values) => [...values, body.query])
					if (body.query.includes('viewer')) return HttpClientResponse.fromWeb(request, Response.json(viewer))
					return HttpClientResponse.fromWeb(request, Response.json({ data: { issue: issueJson } }))
				}),
			)
			const result = yield* Effect.flatMap(LinearApi, (api) => api.getIssue({ issue })).pipe(
				Effect.provide(apiLayer(http)),
			)
			expect(result.ref.teamId).toBe(issue.teamId)
			expect(yield* Ref.get(calls)).toHaveLength(2)
		}),
	)

	it.effect('invalidates and refreshes client credentials exactly once after 401', ({ expect }) =>
		Effect.gen(function* () {
			const tokenCalls = yield* Ref.make(0)
			const issueCalls = yield* Ref.make(0)
			const http = HttpClient.make((request) =>
				Effect.gen(function* () {
					const web = yield* HttpClientRequest.toWeb(request).pipe(Effect.orDie)
					if (new URL(web.url).pathname === '/oauth/token') {
						const count = yield* Ref.updateAndGet(tokenCalls, (n) => n + 1)
						return HttpClientResponse.fromWeb(
							request,
							Response.json({ access_token: `token-${count}`, expires_in: 3600 }),
						)
					}
					const body = (yield* Effect.promise(() => web.json())) as { query: string }
					if (body.query.includes('viewer')) return HttpClientResponse.fromWeb(request, Response.json(viewer))
					const count = yield* Ref.updateAndGet(issueCalls, (n) => n + 1)
					return HttpClientResponse.fromWeb(
						request,
						count === 1
							? Response.json({}, { status: 401 })
							: Response.json({ data: { issue: issueJson } }),
					)
				}),
			)
			const options = LinearApiLiveOptions.make({
				auth: LinearAuth.clientCredentials({
					clientId: Config.succeed('client'),
					clientSecret: Config.succeed(Redacted.make('secret')),
				}),
				organizationId: Config.succeed(organizationId),
				appUserId: Config.succeed(appUserId),
			})
			const layer = makeLinearApiLiveBase(options).pipe(Layer.provide(Layer.succeed(HttpClient.HttpClient, http)))
			yield* Effect.flatMap(LinearApi, (api) => api.getIssue({ issue })).pipe(Effect.provide(layer))
			expect(yield* Ref.get(tokenCalls)).toBe(2)
			expect(yield* Ref.get(issueCalls)).toBe(2)
		}),
	)

	it.effect('rejects refs outside the one configured organization before HTTP', ({ expect }) =>
		Effect.gen(function* () {
			const calls = yield* Ref.make(0)
			const http = HttpClient.make((request) =>
				Ref.update(calls, (count) => count + 1).pipe(
					Effect.as(HttpClientResponse.fromWeb(request, Response.json(viewer))),
				),
			)
			const mismatchedIssue = {
				...issue,
				organizationId: LinearOrganizationId.make('other-organization'),
			}
			const error = yield* Effect.flatMap(LinearApi, (api) => api.getIssue({ issue: mismatchedIssue })).pipe(
				Effect.provide(apiLayer(http)),
				Effect.flip,
			)
			if (error._tag !== 'LinearApiError') return yield* Effect.die('expected LinearApiError')
			expect(error.reason).toBe('identity_mismatch')
			expect(yield* Ref.get(calls)).toBe(0)
		}),
	)
})
