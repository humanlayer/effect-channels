import { Config, Effect, Layer, Match, Schema } from 'effect'
import * as FetchHttpClient from 'effect/unstable/http/FetchHttpClient'
import * as HttpClient from 'effect/unstable/http/HttpClient'

import { createAgentActivity } from './api/CreateAgentActivity'
import { LinearApi } from './LinearApi'
import { LinearAuthenticationInput } from './LinearAuth'
import * as LinearAuth from './LinearAuth'
import { makeLinearCredentialResolver, makeLinearDeveloperTokenResolver } from './LinearCredentialResolver'
import { LinearOrganizationId, LinearUserId } from './LinearIdentity'

const LinearOrganizationIdConfig = Schema.declare<Config.Config<LinearOrganizationId>>(
	(value): value is Config.Config<LinearOrganizationId> => Config.isConfig(value),
)
const LinearUserIdConfig = Schema.declare<Config.Config<LinearUserId>>((value): value is Config.Config<LinearUserId> =>
	Config.isConfig(value),
)

export const LinearApiLiveOptions = Schema.TaggedStruct('LinearApiLiveOptions', {
	auth: LinearAuthenticationInput,
	organizationId: LinearOrganizationIdConfig,
	appUserId: LinearUserIdConfig,
})
export type LinearApiLiveOptions = typeof LinearApiLiveOptions.Type

/** Injectable transport seam. Construction reads configuration and initializes local state but performs no I/O. */
export const makeLinearApiLiveBase = (options: LinearApiLiveOptions) =>
	Layer.effect(
		LinearApi,
		Effect.gen(function* () {
			const client = yield* HttpClient.HttpClient
			const organizationId = yield* options.organizationId
			const appUserId = yield* options.appUserId
			const auth = yield* LinearAuth.resolve(options.auth)
			const credentials = yield* Match.value(auth).pipe(
				Match.tagsExhaustive({
					LinearClientCredentials: (credentials) =>
						Effect.gen(function* () {
							const clientId = yield* credentials.clientId
							const clientSecret = yield* credentials.clientSecret
							return yield* makeLinearCredentialResolver({
								clientId,
								clientSecret,
								organizationId,
								appUserId,
							})
						}),
					LinearDeveloperToken: (developer) =>
						Effect.gen(function* () {
							const token = yield* developer.token
							return yield* makeLinearDeveloperTokenResolver({ token, organizationId, appUserId })
						}),
				}),
				Effect.provideService(HttpClient.HttpClient, client),
			)
			return LinearApi.of({
				createAgentActivity: (request) =>
					Effect.gen(function* () {
						const credential = yield* credentials.resolve(request.organizationId)
						return yield* createAgentActivity(request, credential.accessToken).pipe(
							Effect.provideService(HttpClient.HttpClient, client),
							Effect.catchTag('LinearApiError', (error) =>
								error.reason === 'unauthorized' && credentials.canRefresh
									? credentials.invalidate(request.organizationId).pipe(
											Effect.andThen(credentials.resolve(request.organizationId)),
											Effect.flatMap((fresh) =>
												createAgentActivity(request, fresh.accessToken).pipe(
													Effect.provideService(HttpClient.HttpClient, client),
												),
											),
										)
									: Effect.fail(error),
							),
						)
					}).pipe(
						Effect.withSpan('linear.api.create_agent_activity', {
							attributes: {
								'linear.delivery_id': request.deliveryId,
								'linear.agent_session_id': request.sessionId,
							},
						}),
					),
			})
		}),
	)

const defaultOptions = LinearApiLiveOptions.make({
	auth: LinearAuth.fromEnvironment,
	organizationId: Config.schema(LinearOrganizationId, 'LINEAR_ORGANIZATION_ID'),
	appUserId: Config.schema(LinearUserId, 'LINEAR_APP_USER_ID'),
})

export const LinearApiLiveBase = makeLinearApiLiveBase(defaultOptions)
export const LinearApiLive = LinearApiLiveBase.pipe(Layer.provide(FetchHttpClient.layer))

export const makeLinearApiLive = (options: LinearApiLiveOptions) =>
	makeLinearApiLiveBase(options).pipe(Layer.provide(FetchHttpClient.layer))
