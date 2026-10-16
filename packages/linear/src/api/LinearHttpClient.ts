import { Context, Effect, Layer, Redacted } from 'effect'
import * as FetchHttpClient from 'effect/http/FetchHttpClient'
import * as HttpClient from 'effect/http/HttpClient'
import type { HttpClientRequest } from 'effect/http/HttpClientRequest'
import * as HttpClientRequestModule from 'effect/http/HttpClientRequest'
import type { HttpClientResponse } from 'effect/http/HttpClientResponse'

import type { LinearApiError, LinearApiOperation } from '../LinearApiContract'
import { LinearCredentialResolver } from '../LinearCredential'
import { isLinearFileOrigin } from '../LinearFileModels'
import type { LinearOrganizationId } from '../LinearIdentity'
import { LinearFileOriginRejectedError, LinearTransportError, linearProviderErrorDetails } from './LinearApiErrors'

export type LinearHttpRequest = {
	readonly operation: LinearApiOperation
	readonly request: HttpClientRequest
}

type LinearHttpClientOperations = {
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

export type LinearFixedCredentialOptions = {
	readonly organizationId: LinearOrganizationId
	readonly accessToken: Redacted.Redacted<string>
}

const executeAuthenticated = (accessToken: Redacted.Redacted<string>, input: LinearHttpRequest) =>
	Effect.flatMap(HttpClient.HttpClient, (client) =>
		client
			.pipe(
				HttpClient.mapRequest(HttpClientRequestModule.prependUrl('https://api.linear.app')),
				HttpClient.mapRequest(HttpClientRequestModule.bearerToken(accessToken)),
				HttpClient.mapRequest(HttpClientRequestModule.acceptJson),
			)
			.execute(input.request),
	).pipe(
		Effect.mapError(
			() => new LinearTransportError(linearProviderErrorDetails(input.operation, { retryable: true })),
		),
		Effect.withSpan('linear.http_request', {
			attributes: { 'linear.operation': input.operation, method: input.request.method },
		}),
	)

const sendFileRequest = (input: LinearHttpRequest) =>
	Effect.flatMap(HttpClient.HttpClient, (client) => client.execute(input.request)).pipe(
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
	accessToken: Effect.Effect<Redacted.Redacted<string>, LinearApiError>,
	input: LinearHttpRequest,
): Effect.Effect<
	HttpClientResponse,
	LinearTransportError | LinearFileOriginRejectedError | LinearApiError,
	HttpClient.HttpClient
> => {
	const url = input.request.url
	if (!URL.canParse(url) || new URL(url).protocol !== 'https:')
		return Effect.fail(new LinearFileOriginRejectedError(linearProviderErrorDetails(input.operation)))
	if (!isLinearFileOrigin(url)) return sendFileRequest(input)
	return accessToken.pipe(
		Effect.flatMap((token) =>
			sendFileRequest({
				operation: input.operation,
				request: HttpClientRequestModule.bearerToken(input.request, token),
			}),
		),
	)
}

/** Sends one Linear API request with whatever token `accessToken` yields. */
const executeApiRequest = (
	accessToken: Effect.Effect<Redacted.Redacted<string>, LinearApiError>,
	input: LinearHttpRequest,
): Effect.Effect<HttpClientResponse, LinearTransportError | LinearApiError, HttpClient.HttpClient> =>
	accessToken.pipe(Effect.flatMap((token) => executeAuthenticated(token, input)))

/**
 * One organization-scoped, bearer-authenticated transport for Linear API operations, sending through
 * `HttpClient`. The layer takes its credentials from `LinearCredentialResolver`; `layerFixedCredential` always
 * sends one known token and cannot refresh.
 */
export class LinearHttpClient extends Context.Service<LinearHttpClient, LinearHttpClientOperations>()(
	'@humanlayer/channels-linear/LinearHttpClient',
	{
		make: Effect.gen(function* () {
			const credentials = yield* LinearCredentialResolver
			const client = yield* HttpClient.HttpClient
			const accessToken = credentials.resolve.pipe(Effect.map((credential) => credential.accessToken))
			const operations: LinearHttpClientOperations = {
				organizationId: credentials.organizationId,
				canRefresh: credentials.canRefresh,
				invalidateCredential: credentials.invalidate,
				execute: (input) =>
					executeApiRequest(accessToken, input).pipe(Effect.provideService(HttpClient.HttpClient, client)),
				executeFile: (input) =>
					executeFileRequest(accessToken, input).pipe(Effect.provideService(HttpClient.HttpClient, client)),
			}
			return operations
		}),
	},
) {
	static readonly layer = Layer.effect(this, this.make)

	/** A transport that always sends one known token, through the `HttpClient` in context. It cannot refresh. */
	static readonly makeFixedCredential = (options: LinearFixedCredentialOptions) =>
		Effect.gen(function* () {
			const client = yield* HttpClient.HttpClient
			const accessToken = Effect.succeed(options.accessToken)
			const operations: LinearHttpClientOperations = {
				organizationId: options.organizationId,
				canRefresh: false,
				invalidateCredential: Effect.void,
				execute: (input) =>
					executeApiRequest(accessToken, input).pipe(Effect.provideService(HttpClient.HttpClient, client)),
				executeFile: (input) =>
					executeFileRequest(accessToken, input).pipe(Effect.provideService(HttpClient.HttpClient, client)),
			}
			return operations
		})

	static readonly layerFixedCredential = (options: LinearFixedCredentialOptions) =>
		Layer.effect(this, this.makeFixedCredential(options))
}
