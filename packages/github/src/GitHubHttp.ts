import { Clock, Effect, Schema, Stream } from 'effect'
import { HttpClient, HttpClientRequest, HttpClientResponse } from 'effect/unstable/http'

import { GitHubError } from './GitHubErrors.js'

class GitHubHttpFailure extends Schema.TaggedError<GitHubHttpFailure>()('GitHubHttpFailure', {
	stage: Schema.Literals(['transport', 'status', 'decode']),
	status: Schema.optionalKey(Schema.Finite),
	rateLimited: Schema.optionalKey(Schema.Boolean),
	retryAfterMs: Schema.optionalKey(Schema.Finite),
}) {}

const secondsPattern = /^\d+$/
const secondsToMillis = (value: string | undefined) => {
	if (value === undefined || !secondsPattern.test(value)) return undefined
	const millis = Number(value) * 1_000
	return Number.isSafeInteger(millis) ? millis : undefined
}

const hasRateLimitMessage = (response: HttpClientResponse.HttpClientResponse) =>
	response.stream.pipe(
		Stream.runFoldEffect(
			() => ({ bytes: new Uint8Array(8_192), size: 0 }),
			(body, chunk) => {
				if (body.size + chunk.byteLength > body.bytes.byteLength)
					return Effect.fail(GitHubHttpFailure.make({ stage: 'decode' }))
				body.bytes.set(chunk, body.size)
				body.size += chunk.byteLength
				return Effect.succeed(body)
			},
		),
		Effect.flatMap(({ bytes, size }) =>
			Schema.decodeEffect(Schema.fromJsonString(Schema.Struct({ message: Schema.String })))(
				new TextDecoder().decode(bytes.subarray(0, size)),
			),
		),
		Effect.map(
			({ message }) =>
				message.startsWith('API rate limit exceeded') ||
				message.startsWith('You have exceeded a secondary rate limit.') ||
				message.startsWith('You have triggered an abuse detection mechanism.'),
		),
		Effect.catchTags({
			GitHubHttpFailure: () => Effect.succeed(false),
			HttpClientError: () => Effect.succeed(false),
			SchemaError: () => Effect.succeed(false),
		}),
	)

export const requestJson = <S extends Schema.Top>(
	request: HttpClientRequest.HttpClientRequest,
	schema: S,
	statuses?: ReadonlyArray<number>,
) =>
	Effect.gen(function* () {
		const client = yield* HttpClient.HttpClient
		const response = yield* client
			.execute(request)
			.pipe(Effect.mapError(() => GitHubHttpFailure.make({ stage: 'transport' })))
		if (response.status === 403 || response.status === 429) {
			const primaryLimit = response.headers['x-ratelimit-remaining'] === '0'
			const retryAfter = secondsToMillis(response.headers['retry-after'])
			const rateLimited =
				response.status === 429 ||
				primaryLimit ||
				retryAfter !== undefined ||
				(yield* hasRateLimitMessage(response))
			if (rateLimited) {
				const now = yield* Clock.currentTimeMillis
				const reset = primaryLimit ? secondsToMillis(response.headers['x-ratelimit-reset']) : undefined
				const resetDelay = reset === undefined ? undefined : Math.max(0, reset - now)
				return yield* GitHubHttpFailure.make({
					stage: 'status',
					status: response.status,
					rateLimited: true,
					retryAfterMs:
						retryAfter === undefined && resetDelay === undefined
							? 60_000
							: Math.max(retryAfter ?? 0, resetDelay ?? 0),
				})
			}
		}
		if (response.status < 200 || response.status >= 300)
			return yield* GitHubHttpFailure.make({ stage: 'status', status: response.status })
		if (statuses !== undefined && !statuses.includes(response.status))
			return yield* GitHubHttpFailure.make({ stage: 'decode', status: response.status })
		if (response.status === 204)
			return yield* Schema.decodeUnknownEffect(schema)(undefined).pipe(
				Effect.mapError(() => GitHubHttpFailure.make({ stage: 'decode', status: response.status })),
			)
		return yield* HttpClientResponse.schemaBodyJson(schema)(response).pipe(
			Effect.mapError(() => GitHubHttpFailure.make({ stage: 'decode', status: response.status })),
		)
	}).pipe(
		Effect.tapError((error) => Effect.logError('GitHub request failed', error)),
		Effect.catchTag('GitHubHttpFailure', (error) => {
			if (error.rateLimited === true)
				return Effect.fail(
					GitHubError.make({ reason: 'unavailable', retryAfterMs: error.retryAfterMs ?? 60_000 }),
				)
			return Effect.fail(
				GitHubError.make({
					reason:
						error.status === 401
							? 'authentication'
							: error.status === 403
								? 'forbidden'
								: error.status === 404 || error.status === 410
									? 'not_found'
									: error.status === 422
										? 'invalid_input'
										: error.stage === 'decode'
											? 'response'
											: 'unavailable',
				}),
			)
		}),
		Effect.withSpan('github.http.request'),
	)

export const apiRequest = (input: {
	readonly baseUrl: string
	readonly path: string
	readonly method: 'GET' | 'POST' | 'PATCH' | 'DELETE'
}) =>
	HttpClientRequest.make(input.method)(`${input.baseUrl}${input.path}`).pipe(
		HttpClientRequest.setHeader('accept', 'application/vnd.github+json'),
		HttpClientRequest.setHeader('x-github-api-version', '2022-11-28'),
		HttpClientRequest.setHeader('user-agent', 'humanlayer-channels-github'),
	)
