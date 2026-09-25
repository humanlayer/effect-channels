import { Context, Effect, Layer, Redacted } from 'effect'
import * as HttpClient from 'effect/unstable/http/HttpClient'
import type { HttpClientRequest } from 'effect/unstable/http/HttpClientRequest'
import * as HttpClientRequestModule from 'effect/unstable/http/HttpClientRequest'
import type { HttpClientResponse } from 'effect/unstable/http/HttpClientResponse'

import type { LinearApiError, LinearApiOperation } from '../LinearApi'
import { LinearCredentialResolver } from '../LinearCredentialResolver'
import type { LinearOrganizationId } from '../LinearIdentity'
import { LinearTransportError, linearProviderErrorDetails } from './LinearApiErrors'

export type LinearHttpRequest = {
	readonly operation: LinearApiOperation
	readonly request: HttpClientRequest
}

export type LinearHttpClientShape = {
	readonly organizationId: LinearOrganizationId
	readonly canRefresh: boolean
	readonly invalidateCredential: Effect.Effect<void>
	readonly execute: (
		input: LinearHttpRequest,
	) => Effect.Effect<HttpClientResponse, LinearTransportError | LinearApiError>
}

/** One organization-scoped, bearer-authenticated transport for Linear API operations. */
export class LinearHttpClient extends Context.Service<LinearHttpClient, LinearHttpClientShape>()(
	'@humanlayer/channels-linear-next/LinearHttpClient',
) {}

const executeAuthenticated = (
	client: HttpClient.HttpClient,
	accessToken: Redacted.Redacted<string>,
	input: LinearHttpRequest,
) => {
	const configuredClient = client.pipe(
		HttpClient.mapRequest(HttpClientRequestModule.prependUrl('https://api.linear.app')),
		HttpClient.mapRequest(HttpClientRequestModule.bearerToken(accessToken)),
		HttpClient.mapRequest(HttpClientRequestModule.acceptJson),
	)
	return configuredClient.execute(input.request).pipe(
		Effect.mapError(
			() => new LinearTransportError(linearProviderErrorDetails(input.operation, { retryable: true })),
		),
		Effect.withSpan('linear.http_request', {
			attributes: { 'linear.operation': input.operation, method: input.request.method },
		}),
	)
}

export const makeAuthenticatedLinearHttpClient = (
	client: HttpClient.HttpClient,
	credentials: LinearCredentialResolver['Service'],
): LinearHttpClientShape => ({
	organizationId: credentials.organizationId,
	canRefresh: credentials.canRefresh,
	invalidateCredential: credentials.invalidate,
	execute: (input) =>
		credentials.resolve.pipe(
			Effect.flatMap((credential) => executeAuthenticated(client, credential.accessToken, input)),
		),
})

export const LinearHttpClientLive = Layer.effect(
	LinearHttpClient,
	Effect.gen(function* () {
		return makeAuthenticatedLinearHttpClient(yield* HttpClient.HttpClient, yield* LinearCredentialResolver)
	}),
)

export const makeFixedCredentialLinearHttpClient = (
	client: HttpClient.HttpClient,
	organizationId: LinearOrganizationId,
	accessToken: Redacted.Redacted<string>,
): LinearHttpClientShape => ({
	organizationId,
	canRefresh: false,
	invalidateCredential: Effect.void,
	execute: (input) => executeAuthenticated(client, accessToken, input),
})
