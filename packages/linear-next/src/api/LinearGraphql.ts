import { Effect, Option, Predicate, Schema } from 'effect'
import * as HttpClient from 'effect/unstable/http/HttpClient'
import * as HttpClientRequest from 'effect/unstable/http/HttpClientRequest'

import type { LinearApiOperation } from '../LinearApi'
import { LinearProviderError } from './LinearApiErrors'

const GraphqlError = Schema.Struct({
	message: Schema.optionalKey(Schema.String),
	extensions: Schema.optionalKey(
		Schema.Struct({
			code: Schema.optionalKey(Schema.String),
			statusCode: Schema.optionalKey(Schema.Int),
			retryAfter: Schema.optionalKey(Schema.Number),
			retryAfterMs: Schema.optionalKey(Schema.Number),
		}),
	),
})
const GraphqlErrorEnvelope = Schema.Struct({ errors: Schema.Array(GraphqlError) })

export type LinearGraphqlInput<A> = {
	readonly operation: LinearApiOperation
	readonly query: string
	readonly variables: Schema.Json
	readonly data: Schema.Codec<A, unknown, never, never>
}

const apiError = (
	operation: LinearApiOperation,
	reason: LinearProviderError['reason'],
	retryable: boolean,
	status?: number,
) =>
	Predicate.isUndefined(status)
		? LinearProviderError.make({ operation, reason, retryable })
		: LinearProviderError.make({ operation, reason, retryable, status })

const retryAfter = (value: string | undefined) => {
	if (Predicate.isUndefined(value) || !/^\d+$/.test(value)) return undefined
	const milliseconds = Number(value) * 1_000
	return Number.isSafeInteger(milliseconds) ? milliseconds : undefined
}
const knownGraphqlCodes = new Set([
	'RATELIMITED',
	'RATE_LIMITED',
	'FORBIDDEN',
	'UNAUTHENTICATED',
	'AUTHENTICATION_ERROR',
	'NOT_FOUND',
	'ENTITY_NOT_FOUND',
	'BAD_USER_INPUT',
	'GRAPHQL_VALIDATION_FAILED',
	'VALIDATION_ERROR',
])

const graphqlFailure = (
	operation: LinearApiOperation,
	status: number,
	error: typeof GraphqlError.Type,
	headerRetryAfter: number | undefined,
) => {
	const code = error.extensions?.code?.toUpperCase()
	const reason =
		code === 'RATELIMITED' || code === 'RATE_LIMITED'
			? 'rate_limited'
			: code === 'FORBIDDEN'
				? 'forbidden'
				: code === 'UNAUTHENTICATED' || code === 'AUTHENTICATION_ERROR'
					? 'authentication'
					: code === 'NOT_FOUND' || code === 'ENTITY_NOT_FOUND'
						? 'not_found'
						: code === 'BAD_USER_INPUT' ||
							  code === 'GRAPHQL_VALIDATION_FAILED' ||
							  code === 'VALIDATION_ERROR'
							? 'validation'
							: 'graphql'
	const retryable = reason === 'rate_limited'
	const extensionDelay =
		error.extensions?.retryAfterMs ??
		(Predicate.isUndefined(error.extensions?.retryAfter) ? undefined : error.extensions.retryAfter * 1_000)
	const delay = headerRetryAfter ?? extensionDelay
	return LinearProviderError.make({
		operation,
		reason,
		retryable,
		status: error.extensions?.statusCode ?? status,
		...(Predicate.isUndefined(delay) || !Number.isSafeInteger(delay) || delay < 0 ? {} : { retryAfterMs: delay }),
		...(Predicate.isUndefined(error.message) ? {} : { message: error.message.slice(0, 1024) }),
	})
}

/** Executes one schema-decoded Linear GraphQL request. It does not retry or resolve credentials. */
export const linearGraphql = <A>(
	input: LinearGraphqlInput<A>,
): Effect.Effect<A, LinearProviderError, HttpClient.HttpClient> =>
	Effect.gen(function* () {
		const client = yield* HttpClient.HttpClient
		const request = yield* HttpClientRequest.post('https://api.linear.app/graphql').pipe(
			HttpClientRequest.schemaBodyJson(Schema.Json)({ query: input.query, variables: input.variables }),
			Effect.mapError(() => apiError(input.operation, 'decode', false)),
		)
		const response = yield* client
			.execute(request)
			.pipe(Effect.mapError(() => apiError(input.operation, 'transport', true)))
		const rawBody = yield* response.json.pipe(
			Effect.match({
				onFailure: () => ({ _tag: 'InvalidJson' as const }),
				onSuccess: (value) => ({ _tag: 'Json' as const, value }),
			}),
		)
		const decodedErrors =
			rawBody._tag === 'Json'
				? yield* Schema.decodeUnknownEffect(GraphqlErrorEnvelope)(rawBody.value).pipe(Effect.option)
				: Option.none()
		const delay = retryAfter(response.headers['retry-after'])
		if (Option.isSome(decodedErrors) && decodedErrors.value.errors.length > 0) {
			const first = decodedErrors.value.errors[0]!
			const code = first.extensions?.code?.toUpperCase()
			if (
				(code !== undefined && knownGraphqlCodes.has(code)) ||
				(response.status >= 200 && response.status < 300)
			)
				return yield* graphqlFailure(input.operation, response.status, first, delay)
		}
		if (response.status === 401) return yield* apiError(input.operation, 'authentication', false, response.status)
		if (response.status === 403) return yield* apiError(input.operation, 'forbidden', false, response.status)
		if (response.status < 200 || response.status >= 300) {
			const rateLimited = response.status === 429
			const retryable = response.status >= 500 || rateLimited
			return yield* LinearProviderError.make({
				operation: input.operation,
				reason: rateLimited
					? 'rate_limited'
					: response.status === 404
						? 'not_found'
						: retryable
							? 'unavailable'
							: 'graphql',
				retryable,
				status: response.status,
				...(Predicate.isUndefined(delay) ? {} : { retryAfterMs: delay }),
			})
		}
		if (rawBody._tag === 'InvalidJson') return yield* apiError(input.operation, 'decode', false, response.status)
		const envelopeSchema = Schema.Struct({
			data: Schema.optionalKey(Schema.NullOr(input.data)),
			errors: Schema.optionalKey(Schema.Array(GraphqlError)),
		})
		const envelope = yield* Schema.decodeUnknownEffect(envelopeSchema)(rawBody.value).pipe(
			Effect.mapError(() => apiError(input.operation, 'decode', false, response.status)),
		)
		if (Predicate.isNullish(envelope.data))
			return yield* apiError(input.operation, 'decode', false, response.status)
		return envelope.data
	}).pipe(
		Effect.tapError((error) =>
			Effect.logError('Linear GraphQL operation failed').pipe(
				Effect.annotateLogs({
					operation: error.operation,
					reason: error.reason,
					retryable: error.retryable,
					status: Predicate.isUndefined(error.status) ? 'none' : error.status,
				}),
			),
		),
		Effect.withSpan('linear.api.graphql', { attributes: { 'linear.operation': input.operation } }),
	)
