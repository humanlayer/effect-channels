import { Clock, Effect, Predicate, Redacted, Schema } from 'effect'
import * as HttpClient from 'effect/unstable/http/HttpClient'
import * as HttpClientRequest from 'effect/unstable/http/HttpClientRequest'

import { LinearApiError } from '../LinearApi'

const TokenResponse = Schema.Struct({
	access_token: Schema.NonEmptyString,
	token_type: Schema.optionalKey(Schema.String),
	expires_in: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
	scope: Schema.optionalKey(Schema.Union([Schema.String, Schema.Array(Schema.String)])),
})

export const LinearUnverifiedCredential = Schema.Struct({
	accessToken: Schema.Redacted(Schema.NonEmptyString, { disallowJsonEncode: true }),
	expiresAt: Schema.Finite,
})
export type LinearUnverifiedCredential = typeof LinearUnverifiedCredential.Type

export type AcquireClientCredentialsTokenInput = {
	readonly clientId: string
	readonly clientSecret: Redacted.Redacted<string>
	readonly scopes: ReadonlyArray<string>
}

const tokenError = (reason: LinearApiError['reason'], retryable: boolean, status?: number) =>
	Predicate.isUndefined(status)
		? LinearApiError.make({ operation: 'acquire_client_credentials_token', reason, retryable })
		: LinearApiError.make({ operation: 'acquire_client_credentials_token', reason, retryable, status })

/** Exchanges static client credentials for short-lived token material without exposing the token. */
const acquireClientCredentialsTokenRequest = Effect.fn('linear.auth.acquire_client_credentials_token.request')(
	function* (input: AcquireClientCredentialsTokenInput) {
		const client = yield* HttpClient.HttpClient
		const request = HttpClientRequest.post('https://api.linear.app/oauth/token').pipe(
			HttpClientRequest.bodyUrlParams({
				grant_type: 'client_credentials',
				client_id: input.clientId,
				client_secret: Redacted.value(input.clientSecret),
				scope: input.scopes.join(','),
			}),
		)
		const response = yield* client.execute(request).pipe(Effect.mapError(() => tokenError('unavailable', true)))
		if (response.status === 401) return yield* tokenError('unauthorized', false, response.status)
		if (response.status === 403) return yield* tokenError('forbidden', false, response.status)
		if (response.status < 200 || response.status >= 300) {
			const retryable = response.status >= 500 || response.status === 429
			return yield* tokenError(retryable ? 'unavailable' : 'rejected', retryable, response.status)
		}
		const decoded = yield* response.json.pipe(
			Effect.flatMap(Schema.decodeUnknownEffect(TokenResponse)),
			Effect.mapError(() => tokenError('invalid_response', false, response.status)),
		)
		const now = yield* Clock.currentTimeMillis
		const lifetimeMs = Math.max(0, decoded.expires_in * 1_000)
		const safetyMarginMs = Math.min(60 * 60 * 1_000, lifetimeMs / 10)
		return LinearUnverifiedCredential.make({
			accessToken: Redacted.make(decoded.access_token),
			expiresAt: now + Math.max(0, lifetimeMs - safetyMarginMs),
		})
	},
)

export const acquireClientCredentialsToken = (input: AcquireClientCredentialsTokenInput) =>
	acquireClientCredentialsTokenRequest(input).pipe(
		Effect.tapError((error) =>
			Effect.logError('Linear client-credentials token acquisition failed').pipe(
				Effect.annotateLogs({
					operation: error.operation,
					reason: error.reason,
					retryable: error.retryable,
					status: Predicate.isUndefined(error.status) ? 'none' : error.status,
				}),
			),
		),
		Effect.withSpan('linear.auth.acquire_client_credentials_token', {
			attributes: { 'linear.operation': 'acquire_client_credentials_token' },
		}),
	)
