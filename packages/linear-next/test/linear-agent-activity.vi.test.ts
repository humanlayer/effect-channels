import { describe, it } from '@effect/vitest'
import { Config, Effect, Layer, Redacted, Ref, Schema } from 'effect'
import { HttpClient, HttpClientRequest, HttpClientResponse } from 'effect/unstable/http'

import { LinearAuth } from '../src'
import { LinearApi, LinearApiError } from '../src/LinearApi'
import { LinearApiLiveOptions, makeLinearApiLiveBase } from '../src/LinearApiLive'
import { LinearAgentActivityId, LinearAgentSessionId, LinearWebhookDeliveryId } from '../src/LinearIdentity'
import { LinearActivityContent, LinearCreateAgentActivityRequest } from '../src/LinearModels'
import { linearAppUserId, linearOrganizationId } from './fixtures'

const sessionId = LinearAgentSessionId.make('71000000-0000-4000-8000-000000000001')
const activityId = LinearAgentActivityId.make('74000000-0000-4000-8000-000000000001')

const options = LinearApiLiveOptions.make({
	auth: LinearAuth.clientCredentials({
		clientId: Config.succeed('linear-test-client'),
		clientSecret: Config.succeed(Redacted.make('client-secret-never-log')),
	}),
	organizationId: Config.succeed(linearOrganizationId),
	appUserId: Config.succeed(linearAppUserId),
})

const developerTokenOptions = LinearApiLiveOptions.make({
	auth: LinearAuth.developerToken({
		token: Config.succeed(Redacted.make('developer-token-never-log')),
	}),
	organizationId: Config.succeed(linearOrganizationId),
	appUserId: Config.succeed(linearAppUserId),
})

const request = LinearCreateAgentActivityRequest.make({
	organizationId: linearOrganizationId,
	sessionId,
	content: LinearActivityContent.cases.Response.make({ body: 'Completed.' }),
	ephemeral: false,
	deliveryId: LinearWebhookDeliveryId.make('70000000-0000-4000-8000-000000000001'),
})

describe('Linear Agent Activity API', () => {
	it.effect('verifies and reuses a provided developer token without requesting an OAuth token', ({ expect }) =>
		Effect.gen(function* () {
			const calls = yield* Ref.make<ReadonlyArray<string>>([])
			const http = HttpClient.make((httpRequest) =>
				Effect.gen(function* () {
					const web = yield* HttpClientRequest.toWeb(httpRequest).pipe(Effect.orDie)
					yield* Ref.update(calls, (values) => [...values, new URL(web.url).pathname])
					expect(web.headers.get('authorization')).toBe('Bearer developer-token-never-log')
					const body = yield* Effect.promise(() => web.json())
					if (
						typeof body === 'object' &&
						body !== null &&
						'query' in body &&
						String(body.query).includes('viewer')
					)
						return HttpClientResponse.fromWeb(
							httpRequest,
							Response.json({
								data: { viewer: { id: linearAppUserId, organization: { id: linearOrganizationId } } },
							}),
						)
					return HttpClientResponse.fromWeb(
						httpRequest,
						Response.json({
							data: {
								agentActivityCreate: {
									success: true,
									agentActivity: { id: activityId, agentSession: { id: sessionId } },
								},
							},
						}),
					)
				}),
			)
			const layer = makeLinearApiLiveBase(developerTokenOptions).pipe(
				Layer.provide(Layer.succeed(HttpClient.HttpClient, http)),
			)
			yield* Effect.gen(function* () {
				const api = yield* LinearApi
				yield* api.createAgentActivity(request)
				yield* api.createAgentActivity(request)
			}).pipe(Effect.provide(layer))
			expect(yield* Ref.get(calls)).toEqual(['/graphql', '/graphql', '/graphql'])
		}),
	)

	it.effect('acquires and verifies lazily, caches the token, and lets Linear assign activity IDs', ({ expect }) =>
		Effect.gen(function* () {
			const calls = yield* Ref.make<ReadonlyArray<string>>([])
			const http = HttpClient.make((httpRequest) =>
				Effect.gen(function* () {
					const web = yield* HttpClientRequest.toWeb(httpRequest).pipe(Effect.orDie)
					const url = new URL(web.url)
					yield* Ref.update(calls, (values) => [...values, url.pathname])
					if (url.pathname === '/oauth/token') {
						const body = new URLSearchParams(yield* Effect.promise(() => web.text()))
						expect(body.get('grant_type')).toBe('client_credentials')
						expect(body.get('client_id')).toBe('linear-test-client')
						expect(body.get('client_secret')).toBe('client-secret-never-log')
						return HttpClientResponse.fromWeb(
							httpRequest,
							Response.json({ access_token: 'access-token-never-log', expires_in: 2_592_000 }),
						)
					}
					expect(web.headers.get('authorization')).toBe('Bearer access-token-never-log')
					const body = yield* Effect.promise(() => web.json())
					if (
						typeof body === 'object' &&
						body !== null &&
						'query' in body &&
						String(body.query).includes('viewer')
					)
						return HttpClientResponse.fromWeb(
							httpRequest,
							Response.json({
								data: { viewer: { id: linearAppUserId, organization: { id: linearOrganizationId } } },
							}),
						)
					expect(body).toMatchObject({ variables: { input: { agentSessionId: sessionId } } })
					if (
						typeof body === 'object' &&
						body !== null &&
						'variables' in body &&
						typeof body.variables === 'object' &&
						body.variables !== null &&
						'input' in body.variables
					)
						expect(body.variables.input).not.toHaveProperty('id')
					return HttpClientResponse.fromWeb(
						httpRequest,
						Response.json({
							data: {
								agentActivityCreate: {
									success: true,
									agentActivity: { id: activityId, agentSession: { id: sessionId } },
								},
							},
						}),
					)
				}),
			)
			const layer = makeLinearApiLiveBase(options).pipe(Layer.provide(Layer.succeed(HttpClient.HttpClient, http)))
			const receipts = yield* Effect.gen(function* () {
				const api = yield* LinearApi
				return [yield* api.createAgentActivity(request), yield* api.createAgentActivity(request)] as const
			}).pipe(Effect.provide(layer))
			expect(receipts[0].activityId).toBe(activityId)
			expect(yield* Ref.get(calls)).toEqual(['/oauth/token', '/graphql', '/graphql', '/graphql'])
		}),
	)

	it.effect('fails viewer mismatch before the requested mutation', ({ expect }) =>
		Effect.gen(function* () {
			const calls = yield* Ref.make<ReadonlyArray<string>>([])
			const http = HttpClient.make((httpRequest) =>
				Effect.gen(function* () {
					const web = yield* HttpClientRequest.toWeb(httpRequest).pipe(Effect.orDie)
					const url = new URL(web.url)
					yield* Ref.update(calls, (values) => [...values, url.pathname])
					return HttpClientResponse.fromWeb(
						httpRequest,
						url.pathname === '/oauth/token'
							? Response.json({ access_token: 'access-token-never-log', expires_in: 2_592_000 })
							: Response.json({
									data: {
										viewer: { id: 'wrong-app-user', organization: { id: linearOrganizationId } },
									},
								}),
					)
				}),
			)
			const layer = makeLinearApiLiveBase(options).pipe(Layer.provide(Layer.succeed(HttpClient.HttpClient, http)))
			const error = yield* Effect.flatMap(LinearApi, (api) => api.createAgentActivity(request)).pipe(
				Effect.provide(layer),
				Effect.flip,
			)
			expect(Schema.is(LinearApiError)(error)).toBe(true)
			if (Schema.is(LinearApiError)(error)) expect(error.reason).toBe('identity_mismatch')
			expect(yield* Ref.get(calls)).toEqual(['/oauth/token', '/graphql'])
		}),
	)
})
