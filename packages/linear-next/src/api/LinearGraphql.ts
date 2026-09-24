import { Effect, Predicate, Redacted, Schema } from 'effect'
import * as HttpClient from 'effect/unstable/http/HttpClient'
import * as HttpClientRequest from 'effect/unstable/http/HttpClientRequest'

import { LinearApiError, type LinearApiOperation } from '../LinearApi'

const GraphqlError = Schema.Struct({ message: Schema.optionalKey(Schema.String) })

export type LinearGraphqlInput<A> = {
	readonly operation: LinearApiOperation
	readonly query: string
	readonly variables: Schema.Json
	readonly credential: Redacted.Redacted<string>
	readonly data: Schema.Codec<A, unknown, never, never>
}

const apiError = (
	operation: LinearApiOperation,
	reason: LinearApiError['reason'],
	retryable: boolean,
	status?: number,
) =>
	Predicate.isUndefined(status)
		? LinearApiError.make({ operation, reason, retryable })
		: LinearApiError.make({ operation, reason, retryable, status })

/** Executes one schema-decoded Linear GraphQL request. It does not retry or resolve credentials. */
export const linearGraphql = <A>(
	input: LinearGraphqlInput<A>,
): Effect.Effect<A, LinearApiError, HttpClient.HttpClient> =>
	Effect.gen(function* () {
		const client = yield* HttpClient.HttpClient
		const request = yield* HttpClientRequest.post('https://api.linear.app/graphql').pipe(
			HttpClientRequest.bearerToken(Redacted.value(input.credential)),
			HttpClientRequest.schemaBodyJson(Schema.Json)({ query: input.query, variables: input.variables }),
			Effect.mapError(() => apiError(input.operation, 'invalid_response', false)),
		)
		const response = yield* client
			.execute(request)
			.pipe(Effect.mapError(() => apiError(input.operation, 'unavailable', true)))
		if (response.status === 401) return yield* apiError(input.operation, 'unauthorized', false, response.status)
		if (response.status === 403) return yield* apiError(input.operation, 'forbidden', false, response.status)
		if (response.status < 200 || response.status >= 300)
			return yield* apiError(
				input.operation,
				response.status >= 500 || response.status === 429 ? 'unavailable' : 'rejected',
				response.status >= 500 || response.status === 429,
				response.status,
			)
		const envelopeSchema = Schema.Struct({
			data: Schema.optionalKey(Schema.NullOr(input.data)),
			errors: Schema.optionalKey(Schema.Array(GraphqlError)),
		})
		const envelope = yield* response.json.pipe(
			Effect.flatMap(Schema.decodeUnknownEffect(envelopeSchema)),
			Effect.mapError(() => apiError(input.operation, 'invalid_response', false, response.status)),
		)
		if (Predicate.isNotUndefined(envelope.errors) && envelope.errors.length > 0)
			return yield* apiError(input.operation, 'rejected', false, response.status)
		if (Predicate.isNullish(envelope.data))
			return yield* apiError(input.operation, 'invalid_response', false, response.status)
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
