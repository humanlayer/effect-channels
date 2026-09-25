import { Context, Effect, Layer, Redacted } from 'effect'
import * as FetchHttpClient from 'effect/unstable/http/FetchHttpClient'
import * as HttpClient from 'effect/unstable/http/HttpClient'
import type { HttpClientRequest } from 'effect/unstable/http/HttpClientRequest'
import * as HttpClientRequestModule from 'effect/unstable/http/HttpClientRequest'
import type { HttpClientResponse } from 'effect/unstable/http/HttpClientResponse'

import type { LinearApiError, LinearApiOperation } from '../LinearApi'
import { LinearCredentialResolver } from '../LinearCredentialResolver'
import { isLinearFileOrigin } from '../LinearFiles'
import type { LinearOrganizationId } from '../LinearIdentity'
import { LinearFileOriginRejectedError, LinearTransportError, linearProviderErrorDetails } from './LinearApiErrors'

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
	/**
	 * Sends one absolute HTTPS file request without following redirects or recording its URL in client spans. The
	 * bearer credential is attached only when the request targets the approved Linear upload origin.
	 */
	readonly executeFile: (
		input: LinearHttpRequest,
	) => Effect.Effect<HttpClientResponse, LinearTransportError | LinearFileOriginRejectedError | LinearApiError>
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

const sendFileRequest = (client: HttpClient.HttpClient, input: LinearHttpRequest) =>
	client.execute(input.request).pipe(
		Effect.mapError(
			() => new LinearTransportError(linearProviderErrorDetails(input.operation, { retryable: true })),
		),
		Effect.provideService(FetchHttpClient.RequestInit, { redirect: 'manual' }),
		Effect.provideService(HttpClient.TracerDisabledWhen, () => true),
		Effect.withSpan('linear.file_request', {
			attributes: { 'linear.operation': input.operation, method: input.request.method },
		}),
	)

const executeFileRequest = (
	client: HttpClient.HttpClient,
	accessToken: Effect.Effect<Redacted.Redacted<string>, LinearApiError>,
	input: LinearHttpRequest,
) => {
	const url = input.request.url
	if (!URL.canParse(url) || new URL(url).protocol !== 'https:')
		return Effect.fail(new LinearFileOriginRejectedError(linearProviderErrorDetails(input.operation)))
	if (!isLinearFileOrigin(url)) return sendFileRequest(client, input)
	return accessToken.pipe(
		Effect.flatMap((token) =>
			sendFileRequest(client, {
				operation: input.operation,
				request: HttpClientRequestModule.bearerToken(input.request, token),
			}),
		),
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
	executeFile: (input) =>
		executeFileRequest(client, credentials.resolve.pipe(Effect.map((credential) => credential.accessToken)), input),
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
	executeFile: (input) => executeFileRequest(client, Effect.succeed(accessToken), input),
})
