import { Effect, Match, Schema } from 'effect'
import * as HttpClientRequest from 'effect/unstable/http/HttpClientRequest'

import type { LinearApiError, LinearApiOperation } from '../LinearApi'
import {
	LinearAuthenticationError,
	LinearForbiddenError,
	LinearGraphqlRequestError,
	LinearRateLimitedError,
	LinearResourceNotFoundError,
	LinearResponseDecodeError,
	LinearUnavailableError,
	LinearValidationError,
	type LinearProviderError,
	linearProviderErrorDetails,
} from './LinearApiErrors'
import { LinearHttpClient } from './LinearHttpClient'

export const LinearRateLimitedGraphqlCode = Schema.Literals(['RATELIMITED', 'RATE_LIMITED'])
export type LinearRateLimitedGraphqlCode = typeof LinearRateLimitedGraphqlCode.Type

export const LinearAuthenticationGraphqlCode = Schema.Literals(['UNAUTHENTICATED', 'AUTHENTICATION_ERROR'])
export type LinearAuthenticationGraphqlCode = typeof LinearAuthenticationGraphqlCode.Type

export const LinearNotFoundGraphqlCode = Schema.Literals(['NOT_FOUND', 'ENTITY_NOT_FOUND'])
export type LinearNotFoundGraphqlCode = typeof LinearNotFoundGraphqlCode.Type

export const LinearValidationGraphqlCode = Schema.Literals([
	'BAD_USER_INPUT',
	'GRAPHQL_VALIDATION_FAILED',
	'VALIDATION_ERROR',
])
export type LinearValidationGraphqlCode = typeof LinearValidationGraphqlCode.Type

export const LinearKnownGraphqlCode = Schema.Literals([
	...LinearRateLimitedGraphqlCode.literals,
	'FORBIDDEN',
	...LinearAuthenticationGraphqlCode.literals,
	...LinearNotFoundGraphqlCode.literals,
	...LinearValidationGraphqlCode.literals,
])
export type LinearKnownGraphqlCode = typeof LinearKnownGraphqlCode.Type

const LinearGraphqlErrorCategory = Schema.Literals([
	'rate_limited',
	'forbidden',
	'authentication',
	'not_found',
	'validation',
])
type LinearGraphqlErrorCategory = typeof LinearGraphqlErrorCategory.Type

const graphqlErrorCategoryByCode = {
	RATELIMITED: 'rate_limited',
	RATE_LIMITED: 'rate_limited',
	FORBIDDEN: 'forbidden',
	UNAUTHENTICATED: 'authentication',
	AUTHENTICATION_ERROR: 'authentication',
	NOT_FOUND: 'not_found',
	ENTITY_NOT_FOUND: 'not_found',
	BAD_USER_INPUT: 'validation',
	GRAPHQL_VALIDATION_FAILED: 'validation',
	VALIDATION_ERROR: 'validation',
} satisfies Record<LinearKnownGraphqlCode, LinearGraphqlErrorCategory>

const LinearGraphqlErrorPayload = Schema.Struct({
	message: Schema.optionalKey(Schema.String),
	extensions: Schema.optionalKey(
		Schema.Struct({
			code: Schema.optionalKey(Schema.String),
			statusCode: Schema.optionalKey(Schema.Int),
			retryAfter: Schema.optionalKey(Schema.Finite),
			retryAfterMs: Schema.optionalKey(Schema.Finite),
		}),
	),
})

const LinearGraphqlErrorResponse = Schema.Struct({ errors: Schema.Array(LinearGraphqlErrorPayload) })

/** The JSON body of a Linear GraphQL request whose variables follow the operation's own Schema. */
export const linearGraphqlRequest = <Variables extends Schema.Constraint>(variables: Variables) =>
	Schema.Struct({ query: Schema.String, variables })

export type LinearGraphqlInput<V, A> = {
	readonly operation: LinearApiOperation
	readonly query: string
	readonly variables: Schema.Codec<V, unknown, never, never>
	readonly input: NoInfer<V>
	readonly response: Schema.Codec<A, unknown, never, never>
}

const parseRetryAfterHeader = (value: string | undefined): number | null => {
	if (value === undefined || !/^\d+$/.test(value)) return null
	const milliseconds = Number(value) * 1_000
	if (!Number.isSafeInteger(milliseconds)) return null
	return milliseconds
}

const parseExtensionRetryAfter = (error: typeof LinearGraphqlErrorPayload.Type): number | null => {
	const milliseconds = error.extensions?.retryAfterMs
	if (milliseconds !== undefined && Number.isSafeInteger(milliseconds) && milliseconds >= 0) return milliseconds
	const seconds = error.extensions?.retryAfter
	if (seconds === undefined) return null
	const fromSeconds = seconds * 1_000
	if (!Number.isSafeInteger(fromSeconds) || fromSeconds < 0) return null
	return fromSeconds
}

const failGraphqlResponseError = (
	operation: LinearApiOperation,
	status: number,
	payload: typeof LinearGraphqlErrorPayload.Type,
	headerRetryAfterMs: number | null,
): Effect.Effect<never, LinearProviderError> => {
	const extensionRetryAfterMs = parseExtensionRetryAfter(payload)
	const details = linearProviderErrorDetails(operation, {
		status: payload.extensions?.statusCode ?? status,
		message: payload.message?.slice(0, 1024) ?? null,
		retryAfterMs: headerRetryAfterMs ?? extensionRetryAfterMs,
	})
	const code = payload.extensions?.code?.toUpperCase()
	if (code === undefined || !Schema.is(LinearKnownGraphqlCode)(code)) {
		return Effect.fail(new LinearGraphqlRequestError(details))
	}
	return Match.value(graphqlErrorCategoryByCode[code]).pipe(
		Match.when('rate_limited', () => Effect.fail(new LinearRateLimitedError({ ...details, retryable: true }))),
		Match.when('forbidden', () => Effect.fail(new LinearForbiddenError(details))),
		Match.when('authentication', () => Effect.fail(new LinearAuthenticationError(details))),
		Match.when('not_found', () => Effect.fail(new LinearResourceNotFoundError(details))),
		Match.when('validation', () => Effect.fail(new LinearValidationError(details))),
		Match.exhaustive,
	)
}

/** Executes one schema-decoded Linear GraphQL request without retrying or resolving public API policy. */
export const linearGraphql = <V, A>(
	input: LinearGraphqlInput<V, A>,
): Effect.Effect<A, LinearProviderError | LinearApiError, LinearHttpClient> =>
	Effect.gen(function* () {
		const client = yield* LinearHttpClient
		const request = yield* HttpClientRequest.post('/graphql').pipe(
			HttpClientRequest.schemaBodyJson(linearGraphqlRequest(input.variables))({
				query: input.query,
				variables: input.input,
			}),
			Effect.mapError(
				() =>
					new LinearResponseDecodeError(
						linearProviderErrorDetails(input.operation, {
							message: 'Could not encode Linear GraphQL request',
						}),
					),
			),
		)
		const response = yield* client.execute({ operation: input.operation, request })
		const headerRetryAfterMs = parseRetryAfterHeader(response.headers['retry-after'])
		if (response.status === 401) {
			return yield* new LinearAuthenticationError(
				linearProviderErrorDetails(input.operation, { status: response.status }),
			)
		}
		if (response.status === 403) {
			return yield* new LinearForbiddenError(
				linearProviderErrorDetails(input.operation, { status: response.status }),
			)
		}
		if (response.status === 404) {
			return yield* new LinearResourceNotFoundError(
				linearProviderErrorDetails(input.operation, { status: response.status }),
			)
		}
		if (response.status === 429) {
			return yield* new LinearRateLimitedError(
				linearProviderErrorDetails(input.operation, {
					status: response.status,
					retryable: true,
					retryAfterMs: headerRetryAfterMs,
				}),
			)
		}
		if (response.status >= 500) {
			return yield* new LinearUnavailableError(
				linearProviderErrorDetails(input.operation, { status: response.status, retryable: true }),
			)
		}
		const body = yield* response.json.pipe(
			Effect.mapError(
				() =>
					new LinearResponseDecodeError(
						linearProviderErrorDetails(input.operation, {
							status: response.status,
							message: 'Linear returned invalid JSON',
						}),
					),
			),
		)
		const LinearGraphqlOperationResponse = Schema.Struct({
			data: Schema.optionalKey(Schema.NullOr(input.response)),
			errors: Schema.optionalKey(LinearGraphqlErrorResponse.fields.errors),
		})
		const envelope = yield* Schema.decodeUnknownEffect(LinearGraphqlOperationResponse)(body).pipe(
			Effect.mapError(
				() =>
					new LinearResponseDecodeError(
						linearProviderErrorDetails(input.operation, { status: response.status }),
					),
			),
		)
		const firstError = envelope.errors?.[0]
		if (firstError !== undefined) {
			return yield* failGraphqlResponseError(input.operation, response.status, firstError, headerRetryAfterMs)
		}
		if (response.status < 200 || response.status >= 300) {
			return yield* new LinearGraphqlRequestError(
				linearProviderErrorDetails(input.operation, { status: response.status }),
			)
		}
		if (envelope.data === null || envelope.data === undefined) {
			return yield* new LinearResponseDecodeError(
				linearProviderErrorDetails(input.operation, { status: response.status }),
			)
		}
		return envelope.data
	}).pipe(
		Effect.tapError((error) =>
			Effect.logError('Linear GraphQL operation failed').pipe(
				Effect.annotateLogs({
					operation: error.operation,
					retryable: error.retryable,
					status: error.status ?? 'none',
				}),
			),
		),
		Effect.withSpan('linear.api.graphql', { attributes: { 'linear.operation': input.operation } }),
	)
