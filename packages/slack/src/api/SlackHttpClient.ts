import { Config, Context, Duration, Effect, Layer, Predicate, type Redacted, Schedule, Schema } from 'effect'
import * as FetchHttpClient from 'effect/http/FetchHttpClient'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientRequest from 'effect/http/HttpClientRequest'
import type { HttpClientResponse } from 'effect/http/HttpClientResponse'
import type * as UrlParams from 'effect/http/UrlParams'

import { SlackApiError, SlackApiOperation } from '../SlackApi'

/** The only origins that receive the bot token for file downloads. */
export const slackFileOrigins: ReadonlySet<string> = new Set(['https://files.slack.com', 'https://files.slack-gov.com'])

/** True only for HTTPS URLs on an approved Slack file origin. */
export const isSlackFileUrl = (value: string) => URL.canParse(value) && slackFileOrigins.has(new URL(value).origin)

/** Slack rejected a Web API call with `missing_scope`. Callers narrow it to an operation-specific scope error. */
export class SlackMissingScopeError extends Schema.TaggedError<SlackMissingScopeError>()('SlackMissingScopeError', {
	operation: SlackApiOperation,
	needed: Schema.NullOr(Schema.String),
}) {}

class SlackRateLimitedError extends Schema.TaggedError<SlackRateLimitedError>()('SlackRateLimitedError', {
	operation: SlackApiOperation,
	retryAfterMs: Schema.optionalKey(Schema.Finite),
}) {}

const SlackResponseStatus = Schema.Struct({
	ok: Schema.Boolean,
	error: Schema.optionalKey(Schema.String),
	needed: Schema.optionalKey(Schema.String),
})

export type SlackWebApiCall<A, P> = {
	readonly operation: SlackApiOperation
	readonly method: string
	readonly params: P
	readonly response: Schema.Codec<A, unknown>
}

export type SlackFileHttpRequest = {
	readonly operation: SlackApiOperation
	readonly url: string
}

export type SlackWebApiError = SlackApiError | SlackMissingScopeError

/** One bot-token-authenticated transport for Slack Web API methods and private file downloads. */
export class SlackHttpClient extends Context.Service<
	SlackHttpClient,
	{
		readonly postJson: <A>(call: SlackWebApiCall<A, Schema.Json>) => Effect.Effect<A, SlackWebApiError>
		readonly postForm: <A>(
			call: SlackWebApiCall<A, UrlParams.CoercibleRecord>,
		) => Effect.Effect<A, SlackWebApiError>
		readonly get: <A>(call: SlackWebApiCall<A, UrlParams.CoercibleRecord>) => Effect.Effect<A, SlackWebApiError>
		/**
		 * Sends one HTTPS GET to an approved Slack file origin with the bot token, without following redirects or
		 * recording its URL in client spans. Any other URL is rejected before a request is made.
		 */
		readonly executeFile: (input: SlackFileHttpRequest) => Effect.Effect<HttpClientResponse, SlackApiError>
	}
>()('@humanlayer/channels-slack/SlackHttpClient') {}

const rateLimitRetryPolicy = Schedule.exponential('200 millis').pipe(
	Schedule.setInputType<SlackRateLimitedError | SlackWebApiError>(),
	Schedule.jittered,
	Schedule.upTo({ times: 3 }),
	Schedule.passthrough,
	Schedule.while(({ input }) => Schema.is(SlackRateLimitedError)(input)),
	Schedule.modifyDelay(({ input, duration }) =>
		Effect.succeed(
			Schema.is(SlackRateLimitedError)(input) && Predicate.isNotUndefined(input.retryAfterMs)
				? Duration.max(duration, Duration.millis(input.retryAfterMs))
				: duration,
		),
	),
	Schedule.tap(({ input, attempt }) =>
		Schema.is(SlackRateLimitedError)(input)
			? Effect.logWarning('Slack rate limit reached; retrying request').pipe(
					Effect.annotateLogs({ operation: input.operation, retry_attempt: attempt }),
				)
			: Effect.void,
	),
)

const rejected = (operation: SlackApiOperation, status: typeof SlackResponseStatus.Type) => {
	if (status.error === 'missing_scope')
		return Effect.fail(SlackMissingScopeError.make({ operation, needed: status.needed ?? null }))
	return Effect.fail(SlackApiError.make({ operation, message: status.error ?? 'Slack rejected the request' }))
}

const makeSlackHttpClient = (
	client: HttpClient.HttpClient,
	botToken: Redacted.Redacted<string>,
	apiOrigin: URL,
): SlackHttpClient['Service'] => {
	const execute = <A>(
		operation: SlackApiOperation,
		method: string,
		request: Effect.Effect<HttpClientRequest.HttpClientRequest, SlackApiError>,
		schema: Schema.Codec<A, unknown>,
	): Effect.Effect<A, SlackWebApiError> =>
		Effect.gen(function* () {
			const slackRequest = yield* request
			const response = yield* client
				.execute(slackRequest)
				.pipe(Effect.mapError(() => SlackApiError.make({ operation, message: 'Could not reach Slack' })))
			if (response.status === 429) {
				const retryAfterSeconds = Number(response.headers['retry-after'])
				return yield* Number.isFinite(retryAfterSeconds) && retryAfterSeconds >= 0
					? SlackRateLimitedError.make({ operation, retryAfterMs: retryAfterSeconds * 1_000 })
					: SlackRateLimitedError.make({ operation })
			}
			if (response.status < 200 || response.status >= 300) {
				return yield* SlackApiError.make({ operation, message: `Slack returned HTTP ${response.status}` })
			}
			const invalidResponse = () =>
				SlackApiError.make({ operation, message: 'Slack returned an invalid response' })
			const json = yield* response.json.pipe(Effect.mapError(invalidResponse))
			const status = yield* Schema.decodeUnknownEffect(SlackResponseStatus)(json).pipe(
				Effect.mapError(invalidResponse),
			)
			if (!status.ok) return yield* rejected(operation, status)
			return yield* Schema.decodeEffect(schema)(json).pipe(Effect.mapError(invalidResponse))
		}).pipe(
			Effect.retry(rateLimitRetryPolicy),
			Effect.catchTag('SlackRateLimitedError', () =>
				Effect.fail(SlackApiError.make({ operation, message: 'Slack rate limit persisted after retries' })),
			),
			Effect.withSpan('slack.api.request', {
				attributes: { 'slack.operation': operation, 'slack.method': method },
			}),
		)

	const methodUrl = (method: string) => new URL(method, apiOrigin).toString()

	const sendFile = (input: SlackFileHttpRequest) =>
		client.execute(HttpClientRequest.get(input.url).pipe(HttpClientRequest.bearerToken(botToken))).pipe(
			Effect.mapError(() => SlackApiError.make({ operation: input.operation, message: 'Could not reach Slack' })),
			Effect.provideService(FetchHttpClient.RequestInit, { redirect: 'manual' }),
			Effect.provideService(HttpClient.TracerDisabledWhen, () => true),
			Effect.withSpan('slack.file_request', { attributes: { 'slack.operation': input.operation } }),
		)

	return {
		postJson: (call) =>
			execute(
				call.operation,
				call.method,
				HttpClientRequest.post(methodUrl(call.method)).pipe(
					HttpClientRequest.bearerToken(botToken),
					HttpClientRequest.schemaBodyJson(Schema.Json)(call.params),
					Effect.mapError(() =>
						SlackApiError.make({ operation: call.operation, message: 'Could not encode Slack request' }),
					),
				),
				call.response,
			),
		postForm: (call) =>
			execute(
				call.operation,
				call.method,
				Effect.succeed(
					HttpClientRequest.post(methodUrl(call.method)).pipe(
						HttpClientRequest.bearerToken(botToken),
						HttpClientRequest.bodyUrlParams(call.params),
					),
				),
				call.response,
			),
		get: (call) =>
			execute(
				call.operation,
				call.method,
				Effect.succeed(
					HttpClientRequest.get(methodUrl(call.method), { urlParams: call.params }).pipe(
						HttpClientRequest.bearerToken(botToken),
					),
				),
				call.response,
			),
		executeFile: (input) =>
			isSlackFileUrl(input.url)
				? sendFile(input)
				: Effect.fail(
						SlackApiError.make({
							operation: input.operation,
							message: 'Slack file URL is not on an approved Slack file origin',
						}),
					),
	}
}

/** Reads `SLACK_BOT_TOKEN` and the optional `SLACK_API_ORIGIN`. Construction performs no I/O. */
export const SlackHttpClientLive = Layer.effect(
	SlackHttpClient,
	Effect.gen(function* () {
		const client = yield* HttpClient.HttpClient
		const botToken = yield* Config.Redacted('SLACK_BOT_TOKEN')
		const apiOrigin = yield* Config.URL('SLACK_API_ORIGIN').pipe(
			Config.withDefault(new URL('https://slack.com/api/')),
		)
		return makeSlackHttpClient(client, botToken, apiOrigin)
	}),
)
