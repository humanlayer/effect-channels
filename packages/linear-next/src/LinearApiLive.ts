import { Config, Effect, Layer, Schema } from 'effect'
import * as FetchHttpClient from 'effect/unstable/http/FetchHttpClient'
import * as HttpClient from 'effect/unstable/http/HttpClient'

import { LinearApi } from './LinearApi'
import { LinearClientCredentials } from './LinearAuth'
import { LinearOrganizationId, LinearUserId } from './LinearIdentity'

const LinearOrganizationIdConfig = Schema.declare<Config.Config<LinearOrganizationId>>(
	(value): value is Config.Config<LinearOrganizationId> => Config.isConfig(value),
)
const LinearUserIdConfig = Schema.declare<Config.Config<LinearUserId>>(
	(value): value is Config.Config<LinearUserId> => Config.isConfig(value),
)

export const LinearApiLiveOptions = Schema.TaggedStruct('LinearApiLiveOptions', {
	auth: LinearClientCredentials,
	organizationId: LinearOrganizationIdConfig,
	appUserId: LinearUserIdConfig,
})
export type LinearApiLiveOptions = typeof LinearApiLiveOptions.Type

/** Injectable transport seam. Construction is network-free but eagerly discovers all configuration. */
export const makeLinearApiLiveBase = (options: LinearApiLiveOptions) =>
	Layer.effect(
		LinearApi,
		Effect.gen(function* () {
			yield* HttpClient.HttpClient
			yield* options.auth.clientId
			yield* options.auth.clientSecret
			yield* options.organizationId
			yield* options.appUserId
			return LinearApi.of({})
		}),
	)

const defaultOptions = LinearApiLiveOptions.make({
	auth: LinearClientCredentials.make({
		clientId: Config.string('LINEAR_CLIENT_ID'),
		clientSecret: Config.redacted('LINEAR_CLIENT_SECRET'),
	}),
	organizationId: Config.schema(LinearOrganizationId, 'LINEAR_ORGANIZATION_ID'),
	appUserId: Config.schema(LinearUserId, 'LINEAR_APP_USER_ID'),
})

export const LinearApiLiveBase = makeLinearApiLiveBase(defaultOptions)
export const LinearApiLive = LinearApiLiveBase.pipe(Layer.provide(FetchHttpClient.layer))

export const makeLinearApiLive = (options: LinearApiLiveOptions) =>
	makeLinearApiLiveBase(options).pipe(Layer.provide(FetchHttpClient.layer))
