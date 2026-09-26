import { describe, it } from '@effect/vitest'
import { Effect, Redacted, Ref } from 'effect'
import { HttpClient, HttpClientRequest, HttpClientResponse } from 'effect/unstable/http'

import { makeLinearCredentialResolver } from '../src/LinearCredentialResolver'
import { appUserId, organizationId, viewer } from './api-test-fixtures'

describe('Linear client-credentials resolver', () => {
	it.effect('coalesces concurrent acquisition and verifies viewer before caching', ({ expect }) =>
		Effect.gen(function* () {
			const tokenCalls = yield* Ref.make(0)
			const http = HttpClient.make((request) =>
				Effect.gen(function* () {
					const path = new URL((yield* HttpClientRequest.toWeb(request).pipe(Effect.orDie)).url).pathname
					if (path === '/oauth/token') {
						yield* Ref.update(tokenCalls, (n) => n + 1)
						return HttpClientResponse.fromWeb(
							request,
							Response.json({ access_token: 'fresh', expires_in: 3600 }),
						)
					}
					return HttpClientResponse.fromWeb(request, Response.json(viewer))
				}),
			)
			const resolver = yield* makeLinearCredentialResolver({
				clientId: 'client',
				clientSecret: Redacted.make('secret'),
				organizationId,
				appUserId,
			}).pipe(Effect.provideService(HttpClient.HttpClient, http))
			yield* Effect.all([resolver.resolve, resolver.resolve], { concurrency: 2 })
			expect(yield* Ref.get(tokenCalls)).toBe(1)
		}),
	)
})
