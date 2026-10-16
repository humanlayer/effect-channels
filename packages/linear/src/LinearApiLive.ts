import { Config, Effect, Layer, Match, Schema } from 'effect'
import * as FetchHttpClient from 'effect/http/FetchHttpClient'

import { LinearHttpClient } from './api/LinearHttpClient'
import { LinearApi } from './LinearApi'
import { LinearAuthenticationInput } from './LinearAuth'
import * as LinearAuth from './LinearAuth'
import {
	LinearCredentialResolverClientCredentials,
	LinearCredentialResolverDeveloperToken,
} from './LinearCredentialResolver'
import { LinearFileTransferPolicy } from './LinearFileModels'
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
	filePolicy: Schema.optionalKey(LinearFileTransferPolicy),
})
export type LinearApiLiveOptions = typeof LinearApiLiveOptions.Type

/** Builds LinearApi from an injected HTTP transport and credential resolver. */
export const LinearApiLiveFromResolver = LinearApi.layer().pipe(Layer.provide(LinearHttpClient.layer))

/** Injectable transport seam. Construction reads configuration and initializes local state but performs no I/O. */
export const makeLinearApiLiveBase = (options: LinearApiLiveOptions) => {
	const resolverLayer = Layer.unwrap(
		Effect.gen(function* () {
			const organizationId = yield* options.organizationId
			const appUserId = yield* options.appUserId
			const auth = yield* LinearAuth.resolve(options.auth)
			return yield* Match.value(auth).pipe(
				Match.tagsExhaustive({
					LinearClientCredentials: (credentials) =>
						Effect.gen(function* () {
							const clientId = yield* credentials.clientId
							const clientSecret = yield* credentials.clientSecret
							return LinearCredentialResolverClientCredentials({
								clientId,
								clientSecret,
								organizationId,
								appUserId,
							})
						}),
					LinearDeveloperToken: (developer) =>
						Effect.gen(function* () {
							const token = yield* developer.token
							return LinearCredentialResolverDeveloperToken({ token, organizationId, appUserId })
						}),
				}),
			)
		}),
	)
	const linearHttpLayer = LinearHttpClient.layer.pipe(Layer.provide(resolverLayer))
	return LinearApi.layer(options.filePolicy).pipe(Layer.provide(linearHttpLayer))
}

const defaultOptions = LinearApiLiveOptions.make({
	auth: LinearAuth.fromEnvironment,
	organizationId: Config.schema(LinearOrganizationId, 'LINEAR_ORGANIZATION_ID'),
	appUserId: Config.schema(LinearUserId, 'LINEAR_APP_USER_ID'),
})

export const LinearApiLiveBase = makeLinearApiLiveBase(defaultOptions)
export const LinearApiLive = LinearApiLiveBase.pipe(Layer.provide(FetchHttpClient.layer))

export const makeLinearApiLive = (options: LinearApiLiveOptions) =>
	makeLinearApiLiveBase(options).pipe(Layer.provide(FetchHttpClient.layer))
