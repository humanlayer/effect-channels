import { describe, it } from '@effect/vitest'
import { Config, Effect, Layer, Redacted, Ref, Schema } from 'effect'
import { HttpClient, HttpClientRequest, HttpClientResponse } from 'effect/unstable/http'

import { LinearAuth } from '../src'
import { CreateAgentActivityVariables } from '../src/api/CreateAgentActivity'
import { CreateReactionVariables } from '../src/api/CreateReaction'
import { DeleteReactionVariables } from '../src/api/DeleteReaction'
import { GetViewerIdentityVariables } from '../src/api/GetViewerIdentity'
import { UpdateAgentSessionVariables } from '../src/api/UpdateAgentSession'
import { LinearApi, LinearApiError, LinearReactionTarget } from '../src/LinearApi'
import { LinearApiLiveOptions, makeLinearApiLiveBase } from '../src/LinearApiLive'
import {
	LinearAgentActivityId,
	LinearAgentSessionId,
	LinearIssueId,
	LinearReactionId,
	LinearWebhookDeliveryId,
} from '../src/LinearIdentity'
import {
	LinearActivityContent,
	LinearCreateAgentActivityRequest,
	LinearUpdateAgentSessionRequest,
} from '../src/LinearModels'
import { decodeGraphqlRequest } from './api-test-fixtures'
import { linearAppUserId, linearOrganizationId } from './fixtures'

const decodeAgentActivityRequest = decodeGraphqlRequest(
	Schema.Union([
		GetViewerIdentityVariables,
		CreateAgentActivityVariables,
		UpdateAgentSessionVariables,
		CreateReactionVariables,
		DeleteReactionVariables,
	]),
)

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
					const body = yield* decodeAgentActivityRequest(httpRequest)
					if (body.query.includes('viewer'))
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
					const body = yield* decodeAgentActivityRequest(httpRequest)
					if (body.query.includes('viewer'))
						return HttpClientResponse.fromWeb(
							httpRequest,
							Response.json({
								data: { viewer: { id: linearAppUserId, organization: { id: linearOrganizationId } } },
							}),
						)
					expect(body.variables).toEqual({
						input: {
							agentSessionId: sessionId,
							content: { type: 'response', body: 'Completed.' },
							ephemeral: false,
						},
					})
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

	/**
	 * A fake Linear that verifies the developer token's viewer, records each mutation's variables, and
	 * answers each with `answer`.
	 */
	const fakeLinear = (answer: Schema.Json) =>
		Effect.gen(function* () {
			const sent = yield* Ref.make<ReadonlyArray<unknown>>([])
			const http = HttpClient.make((httpRequest) =>
				Effect.gen(function* () {
					const body = yield* decodeAgentActivityRequest(httpRequest)
					if (body.query.includes('viewer'))
						return HttpClientResponse.fromWeb(
							httpRequest,
							Response.json({
								data: { viewer: { id: linearAppUserId, organization: { id: linearOrganizationId } } },
							}),
						)
					yield* Ref.update(sent, (all) => [...all, body.variables])
					return HttpClientResponse.fromWeb(httpRequest, Response.json(answer))
				}),
			)
			const layer = makeLinearApiLiveBase(developerTokenOptions).pipe(
				Layer.provide(Layer.succeed(HttpClient.HttpClient, http)),
			)
			return { sent, layer }
		})

	const created = {
		data: {
			agentActivityCreate: {
				success: true,
				agentActivity: { id: activityId, agentSession: { id: sessionId } },
			},
		},
	}

	it.effect('sends a caller-chosen ID, error and elicitation content, and choices as the select signal', ({ expect }) =>
		Effect.gen(function* () {
			const { sent, layer } = yield* fakeLinear(created)
			yield* Effect.gen(function* () {
				const api = yield* LinearApi
				yield* api.createAgentActivity(
					LinearCreateAgentActivityRequest.make({
						organizationId: linearOrganizationId,
						sessionId,
						content: LinearActivityContent.cases.Error.make({ body: 'Stopped.' }),
						ephemeral: false,
						activityId,
					}),
				)
				yield* api.createAgentActivity(
					LinearCreateAgentActivityRequest.make({
						organizationId: linearOrganizationId,
						sessionId,
						content: LinearActivityContent.cases.Elicitation.make({ body: 'Where?', options: ['staging', 'production'] }),
						ephemeral: false,
					}),
				)
				yield* api.createAgentActivity(
					LinearCreateAgentActivityRequest.make({
						organizationId: linearOrganizationId,
						sessionId,
						content: LinearActivityContent.cases.Elicitation.make({ body: 'Anything else?' }),
						ephemeral: false,
					}),
				)
			}).pipe(Effect.provide(layer))
			expect(yield* Ref.get(sent)).toEqual([
				{ input: { id: activityId, agentSessionId: sessionId, content: { type: 'error', body: 'Stopped.' }, ephemeral: false } },
				{
					input: {
						agentSessionId: sessionId,
						content: { type: 'elicitation', body: 'Where?' },
						ephemeral: false,
						signal: 'select',
						signalMetadata: {
							options: [
								{ label: 'staging', value: 'staging' },
								{ label: 'production', value: 'production' },
							],
						},
					},
				},
				{ input: { agentSessionId: sessionId, content: { type: 'elicitation', body: 'Anything else?' }, ephemeral: false } },
			])
		}),
	)

	it.effect('reports a repeated activity ID as already_exists, and other input errors as final rejections', ({ expect }) =>
		Effect.gen(function* () {
			const refusal = (message: string, userPresentableMessage: string) => ({
				errors: [{ message, extensions: { code: 'INPUT_ERROR', userPresentableMessage } }],
			})
			const repeated = yield* fakeLinear(
				refusal('conflict on insert of AgentActivity', `Entity AgentActivity with id ${activityId} already exists.`),
			)
			const withId = LinearCreateAgentActivityRequest.make({ ...request, activityId })
			const exists = yield* Effect.flatMap(LinearApi, (api) => api.createAgentActivity(withId)).pipe(
				Effect.provide(repeated.layer),
				Effect.flip,
			)
			expect(exists).toMatchObject({ reason: 'already_exists', retryable: false })

			const notUuid = yield* fakeLinear(refusal('id must be a UUID', 'id must be a UUID'))
			const invalid = yield* Effect.flatMap(LinearApi, (api) => api.createAgentActivity(withId)).pipe(
				Effect.provide(notUuid.layer),
				Effect.flip,
			)
			expect(invalid).toMatchObject({ reason: 'rejected', retryable: false })
		}),
	)

	it.effect('sends a caller-chosen reaction ID, and reports a repeated one as already_exists', ({ expect }) =>
		Effect.gen(function* () {
			const issueId = LinearIssueId.make('b33fb278-fbe0-45e4-b4eb-94b0839f51b9')
			const reactionId = LinearReactionId.make('75000000-0000-4000-8000-000000000001')
			const reaction = {
				target: LinearReactionTarget.cases.Issue.make({
					issue: { organizationId: linearOrganizationId, teamId: null, issueId },
				}),
				emoji: 'eyes',
				reactionId,
			}
			const { sent, layer } = yield* fakeLinear({
				data: { reactionCreate: { success: true, reaction: { id: reactionId, emoji: 'eyes', user: null } } },
			})
			const made = yield* Effect.flatMap(LinearApi, (api) => api.createReaction(reaction)).pipe(Effect.provide(layer))
			expect(made.ref.reactionId).toBe(reactionId)
			expect(yield* Ref.get(sent)).toEqual([{ input: { id: reactionId, emoji: 'eyes', issueId } }])

			const repeated = yield* fakeLinear({
				errors: [
					{
						message: 'conflict on insert of Reaction',
						extensions: {
							code: 'INPUT_ERROR',
							userPresentableMessage: `Entity Reaction with id ${reactionId} already exists.`,
						},
					},
				],
			})
			const exists = yield* Effect.flatMap(LinearApi, (api) => api.createReaction(reaction)).pipe(
				Effect.provide(repeated.layer),
				Effect.flip,
			)
			expect(exists).toMatchObject({ reason: 'already_exists', retryable: false })
		}),
	)

	it.effect("reports Linear's answer for a reaction already deleted as not_found", ({ expect }) =>
		Effect.gen(function* () {
			const gone = yield* fakeLinear({
				errors: [{ message: 'Entity not found: Reaction', extensions: { code: 'INPUT_ERROR' } }],
			})
			const issue = {
				organizationId: linearOrganizationId,
				teamId: null,
				issueId: LinearIssueId.make('b33fb278-fbe0-45e4-b4eb-94b0839f51b9'),
			}
			const deleted = yield* Effect.flatMap(LinearApi, (api) =>
				api.deleteReaction({ issue, reactionId: LinearReactionId.make('75000000-0000-4000-8000-000000000001') }),
			).pipe(Effect.provide(gone.layer), Effect.flip)
			expect(deleted).toMatchObject({ reason: 'not_found', retryable: false })
		}),
	)

	it.effect('adds labeled links to a session', ({ expect }) =>
		Effect.gen(function* () {
			const { sent, layer } = yield* fakeLinear({
				data: { agentSessionUpdate: { success: true, agentSession: { id: sessionId } } },
			})
			yield* Effect.flatMap(LinearApi, (api) =>
				api.updateAgentSession(
					LinearUpdateAgentSessionRequest.make({
						organizationId: linearOrganizationId,
						sessionId,
						addedExternalUrls: [{ label: 'Run log', url: 'https://example.com/run/1' }],
					}),
				),
			).pipe(Effect.provide(layer))
			expect(yield* Ref.get(sent)).toEqual([
				{ id: sessionId, input: { addedExternalUrls: [{ label: 'Run log', url: 'https://example.com/run/1' }] } },
			])
		}),
	)
})
