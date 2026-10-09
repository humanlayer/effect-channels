/**
 * What every request to GitHub's REST API shares: its standard headers, and how a response's status
 * becomes a `GitHubTransportError`.
 */
import { Clock, Effect, Predicate, Schema, type Types } from 'effect'
import * as HttpClientRequest from 'effect/http/HttpClientRequest'
import type * as HttpClientResponse from 'effect/http/HttpClientResponse'

import { GitHubTransportError, GitHubTransportErrorFields } from './GitHubApiErrors'

export type ApiMethod = 'GET' | 'POST' | 'PATCH' | 'PUT' | 'DELETE'

const ErrorBody = Schema.Struct({ message: Schema.String })

/** A request to GitHub's REST API, with its standard headers and no credentials. */
export const gitHubRequest = (method: ApiMethod, url: string) =>
	HttpClientRequest.make(method)(url).pipe(
		HttpClientRequest.setHeader('accept', 'application/vnd.github+json'),
		HttpClientRequest.setHeader('x-github-api-version', '2022-11-28'),
		HttpClientRequest.setHeader('user-agent', 'humanlayer-channels-github'),
	)

const secondsPattern = /^\d+$/
const secondsToMillis = (value: string | undefined) => {
	if (Predicate.isUndefined(value) || !secondsPattern.test(value)) return undefined
	const milliseconds = Number(value) * 1_000
	if (!Number.isSafeInteger(milliseconds)) return undefined
	return milliseconds
}
const epochSecondsToDelayMillis = (value: string | undefined, now: number) => {
	if (Predicate.isUndefined(value) || !secondsPattern.test(value)) return undefined
	const resetAt = Number(value) * 1_000
	if (!Number.isSafeInteger(resetAt)) return undefined
	const delay = resetAt - now
	if (!Number.isSafeInteger(delay)) return undefined
	return Math.max(0, delay)
}

/** Pass a 2xx response through; fail any other with what GitHub said and when to retry. */
export const inspectGitHubStatus = Effect.fn('github.api.inspect_status')(function* (
	response: HttpClientResponse.HttpClientResponse,
) {
	if (response.status >= 200 && response.status < 300) return response
	if (response.status >= 300 && response.status < 400) {
		return yield* GitHubTransportError.make({ stage: 'redirect', status: response.status })
	}
	const retryAfterHeaderMs = secondsToMillis(response.headers['retry-after'])
	const body = yield* response.text.pipe(Effect.orElseSucceed(() => ''))
	const decoded = yield* Schema.decodeEffect(Schema.fromJsonString(ErrorBody))(body).pipe(
		Effect.map((value) => value.message.trim().slice(0, 1_024)),
		Effect.orElseSucceed(() => undefined),
	)
	const rateLimited =
		response.status === 429 ||
		(response.status === 403 &&
			(response.headers['x-ratelimit-remaining'] === '0' ||
				Predicate.isNotUndefined(retryAfterHeaderMs) ||
				(decoded?.toLowerCase().includes('rate limit') ?? false)))
	const now = yield* Clock.currentTimeMillis
	let retryAfterMs = retryAfterHeaderMs
	if (Predicate.isUndefined(retryAfterMs) && rateLimited)
		retryAfterMs = epochSecondsToDelayMillis(response.headers['x-ratelimit-reset'], now)
	const error: Types.Mutable<typeof GitHubTransportErrorFields.Type> = {
		stage: 'status',
		status: response.status,
	}
	if (Predicate.isNotUndefined(decoded) && decoded.length > 0) error.responseMessage = decoded
	if (rateLimited) error.rateLimited = true
	if (Predicate.isNotUndefined(retryAfterMs)) error.retryAfterMs = retryAfterMs
	return yield* GitHubTransportError.make(error)
})
