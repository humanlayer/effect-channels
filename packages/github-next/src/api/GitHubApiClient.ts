import {
	Cache,
	Clock,
	Config,
	Context,
	Duration,
	Effect,
	Encoding,
	Exit,
	Layer,
	Option,
	Predicate,
	Redacted,
	Schema,
	Stream,
} from 'effect'
import * as HttpClient from 'effect/unstable/http/HttpClient'
import * as HttpClientRequest from 'effect/unstable/http/HttpClientRequest'
import type * as HttpClientResponse from 'effect/unstable/http/HttpClientResponse'

import { type GitHubApiOperation, GitHubApiError } from '../GitHubApi'
import { GitHubId } from '../GitHubIdentity'
import type { GitHubRepositoryRef } from '../GitHubModels'
import { GitHubTransportError, narrowGitHubTransportError } from './GitHubApiErrors'
import { Participant } from './GitHubApiSchemas'
import { GitHubAppSigner } from './GitHubAppSigner'

const GitHubApiConfig = Config.all({
	appId: Config.schema(GitHubId, 'GITHUB_APP_ID'),
	privateKey: Config.redacted('GITHUB_PRIVATE_KEY'),
	apiOrigin: Config.url('GITHUB_API_ORIGIN').pipe(Config.withDefault(new URL('https://api.github.com/'))),
	botUserId: Config.option(Config.schema(GitHubId, 'GITHUB_BOT_USER_ID')),
})

const InstallationTokenResponse = Schema.Struct({ token: Schema.NonEmptyString, expires_at: Schema.String })
const AppResponse = Schema.Struct({ slug: Schema.NonEmptyString })
const ErrorBody = Schema.Struct({ message: Schema.String })

type ApiMethod = 'GET' | 'POST' | 'PATCH' | 'PUT' | 'DELETE'
type CallInput<A> = {
	readonly operation: GitHubApiOperation
	readonly ref: GitHubRepositoryRef
	readonly method: ApiMethod
	readonly path: string
	readonly schema: Schema.Codec<A, unknown, never, never>
	readonly body?: Schema.Json
}
type PageInput<Page, A> = {
	readonly operation: GitHubApiOperation
	readonly ref: GitHubRepositoryRef
	readonly path: string
	readonly query?: ReadonlyArray<readonly [string, string]>
	readonly schema: Schema.Codec<Page, unknown, never, never>
	readonly items: (page: Page) => ReadonlyArray<A>
}

export class GitHubApiClient extends Context.Service<
	GitHubApiClient,
	{
		readonly call: <A>(input: CallInput<A>) => Effect.Effect<A, GitHubApiError>
		readonly callVoid: (input: Omit<CallInput<never>, 'schema' | 'body'>) => Effect.Effect<void, GitHubApiError>
		readonly list: <A>(
			input: Omit<PageInput<ReadonlyArray<A>, A>, 'items' | 'schema'> & {
				readonly schema: Schema.Codec<A, unknown, never, never>
			},
		) => Effect.Effect<ReadonlyArray<A>, GitHubApiError>
		readonly paginate: <Page, A>(input: PageInput<Page, A>) => Effect.Effect<ReadonlyArray<A>, GitHubApiError>
		readonly text: (
			input: Omit<CallInput<never>, 'schema' | 'body'> & {
				readonly followRedirects?: boolean
				readonly accept?: string
			},
		) => Effect.Effect<string, GitHubApiError>
		readonly botUserId: Effect.Effect<GitHubId, GitHubApiError>
	}
>()('@humanlayer/channels-github-next/internal/GitHubApiClient') {}

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
const cacheKey = (ref: GitHubRepositoryRef) => JSON.stringify([ref.installationId, ref.repositoryId])

export const GitHubApiClientLive = Layer.effect(
	GitHubApiClient,
	Effect.gen(function* () {
		const client = yield* HttpClient.HttpClient
		const signer = yield* GitHubAppSigner
		const config = yield* GitHubApiConfig
		const apiOrigin = new URL(config.apiOrigin.toString().replace(/\/?$/, '/'))

		const baseRequest = (method: ApiMethod, url: string) =>
			HttpClientRequest.make(method)(url).pipe(
				HttpClientRequest.setHeader('accept', 'application/vnd.github+json'),
				HttpClientRequest.setHeader('x-github-api-version', '2022-11-28'),
				HttpClientRequest.setHeader('user-agent', 'humanlayer-channels-github-next'),
			)

		const appJwt = Effect.fn('github.api.app_jwt')(function* () {
			const now = yield* Clock.currentTimeMillis
			const header = Encoding.encodeBase64Url(JSON.stringify({ alg: 'RS256', typ: 'JWT' }))
			const payload = Encoding.encodeBase64Url(
				JSON.stringify({
					iss: String(config.appId),
					iat: Math.floor(now / 1_000) - 60,
					exp: Math.floor(now / 1_000) + 540,
				}),
			)
			const unsigned = `${header}.${payload}`
			const signature = yield* signer.sign({ privateKey: config.privateKey, data: unsigned }).pipe(
				Effect.tapError((error) => Effect.logError('GitHub App JWT signing failed', error)),
				Effect.mapError(() => GitHubTransportError.make({ stage: 'signing' })),
			)
			return `${unsigned}.${signature}`
		})

		const inspectStatus = Effect.fn('github.api.inspect_status')(function* (
			response: HttpClientResponse.HttpClientResponse,
		) {
			if (response.status >= 200 && response.status < 300) return response
			if (response.status >= 300 && response.status < 400) {
				return yield* GitHubTransportError.make({ stage: 'redirect', status: response.status })
			}
			const retryAfterHeaderMs = secondsToMillis(response.headers['retry-after'])
			const body = yield* response.text.pipe(Effect.catch(() => Effect.succeed('')))
			const decoded = yield* Schema.decodeEffect(Schema.fromJsonString(ErrorBody))(body).pipe(
				Effect.map((value) => value.message.trim().slice(0, 1_024)),
				Effect.catch(() => Effect.succeed(undefined)),
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
			const error: {
				stage: 'status'
				status: number
				message?: string
				rateLimited?: boolean
				retryAfterMs?: number
			} = { stage: 'status', status: response.status }
			if (Predicate.isNotUndefined(decoded) && decoded.length > 0) error.message = decoded
			if (rateLimited) error.rateLimited = true
			if (Predicate.isNotUndefined(retryAfterMs)) error.retryAfterMs = retryAfterMs
			return yield* GitHubTransportError.make(error)
		})

		const observeAndNarrow = <A>(operation: GitHubApiOperation, effect: Effect.Effect<A, GitHubTransportError>) =>
			effect.pipe(
				Effect.tapError((error) =>
					Effect.logError('GitHub API request failed', error).pipe(Effect.annotateLogs({ operation })),
				),
				Effect.catchTag('GitHubTransportError', (error) =>
					Effect.fail(narrowGitHubTransportError(operation, error)),
				),
			)

		const execute = <A>(
			operation: GitHubApiOperation,
			request: HttpClientRequest.HttpClientRequest,
			schema: Schema.Codec<A, unknown, never, never>,
		) =>
			observeAndNarrow(
				operation,
				client.execute(request).pipe(
					Effect.mapError(() => GitHubTransportError.make({ stage: 'transport' })),
					Effect.flatMap(inspectStatus),
					Effect.flatMap((response) =>
						response.json.pipe(
							Effect.flatMap(Schema.decodeUnknownEffect(schema)),
							Effect.mapError(() =>
								GitHubTransportError.make({ stage: 'decode', status: response.status }),
							),
						),
					),
				),
			).pipe(
				Effect.withSpan('github.api.request', {
					attributes: { 'github.operation': operation, 'http.request.method': request.method },
				}),
			)

		const tokenCache = yield* Cache.makeWith(
			(key: string) =>
				Effect.gen(function* () {
					const [installationId, repositoryId] = yield* Schema.decodeEffect(
						Schema.fromJsonString(Schema.Tuple([GitHubId, GitHubId])),
					)(key).pipe(Effect.mapError(() => GitHubTransportError.make({ stage: 'decode' })))
					const jwt = yield* appJwt()
					const request = yield* HttpClientRequest.post(
						new URL(`app/installations/${installationId}/access_tokens`, apiOrigin).toString(),
					).pipe(
						HttpClientRequest.setHeader('accept', 'application/vnd.github+json'),
						HttpClientRequest.setHeader('x-github-api-version', '2022-11-28'),
						HttpClientRequest.setHeader('user-agent', 'humanlayer-channels-github-next'),
						HttpClientRequest.bearerToken(jwt),
						HttpClientRequest.schemaBodyJson(Schema.Json)({ repository_ids: [repositoryId] }),
						Effect.mapError(() => GitHubTransportError.make({ stage: 'decode' })),
					)
					const response = yield* client.execute(request).pipe(
						Effect.mapError(() => GitHubTransportError.make({ stage: 'transport' })),
						Effect.flatMap(inspectStatus),
						Effect.flatMap((value) =>
							value.json.pipe(
								Effect.flatMap(Schema.decodeUnknownEffect(InstallationTokenResponse)),
								Effect.mapError(() => GitHubTransportError.make({ stage: 'decode' })),
							),
						),
					)
					const expiresAt = Date.parse(response.expires_at)
					const receivedAt = yield* Clock.currentTimeMillis
					if (!Number.isFinite(expiresAt) || expiresAt <= receivedAt + 60_000)
						return yield* GitHubTransportError.make({ stage: 'token_expiry' })
					return {
						token: Redacted.make(response.token),
						ttl: Math.min(expiresAt - receivedAt - 60_000, 3_540_000),
					}
				}).pipe(Effect.tapError((error) => Effect.logError('GitHub installation token request failed', error))),
			{
				capacity: 1_000,
				timeToLive: (exit) => {
					if (Exit.isSuccess(exit)) return Duration.millis(exit.value.ttl)
					return Duration.zero
				},
			},
		)

		const authenticatedRequest = Effect.fn('github.api.authenticated_request')(function* (
			input: Omit<CallInput<never>, 'schema'>,
		) {
			const token = yield* Cache.get(tokenCache, cacheKey(input.ref)).pipe(
				Effect.catchTag('GitHubTransportError', (error) =>
					Effect.fail(narrowGitHubTransportError(input.operation, error)),
				),
			)
			const request = baseRequest(input.method, input.path).pipe(HttpClientRequest.bearerToken(token.token))
			if (Predicate.isUndefined(input.body)) return request
			return yield* HttpClientRequest.schemaBodyJson(Schema.Json)(request, input.body).pipe(
				Effect.mapError(() =>
					GitHubApiError.make({ operation: input.operation, reason: 'invalid_response', retryable: false }),
				),
			)
		})

		const retryWithFreshToken = <A>(ref: GitHubRepositoryRef, operation: Effect.Effect<A, GitHubApiError>) =>
			operation.pipe(
				Effect.catchTag('GitHubApiError', (error) => {
					if (error.reason !== 'authentication') return Effect.fail(error)
					return Cache.invalidate(tokenCache, cacheKey(ref)).pipe(Effect.andThen(operation))
				}),
			)
		const urlWithQuery = (path: string, query: ReadonlyArray<readonly [string, string]> = []) => {
			const url = new URL(path.replace(/^\//, ''), apiOrigin)
			for (const [key, value] of query) url.searchParams.set(key, value)
			return url.toString()
		}

		const call = <A>(input: CallInput<A>) => {
			let requestInput: Omit<CallInput<never>, 'schema'> = {
				operation: input.operation,
				ref: input.ref,
				method: input.method,
				path: urlWithQuery(input.path),
			}
			if (Predicate.isNotUndefined(input.body)) requestInput = { ...requestInput, body: input.body }
			return retryWithFreshToken(
				input.ref,
				authenticatedRequest(requestInput).pipe(
					Effect.flatMap((request) => execute(input.operation, request, input.schema)),
				),
			)
		}
		const callVoid = (input: Omit<CallInput<never>, 'schema' | 'body'>) =>
			retryWithFreshToken(
				input.ref,
				authenticatedRequest({ ...input, path: urlWithQuery(input.path) }).pipe(
					Effect.flatMap((request) =>
						observeAndNarrow(
							input.operation,
							client.execute(request).pipe(
								Effect.mapError(() => GitHubTransportError.make({ stage: 'transport' })),
								Effect.flatMap(inspectStatus),
								Effect.asVoid,
							),
						).pipe(
							Effect.withSpan('github.api.request', {
								attributes: {
									'github.operation': input.operation,
									'http.request.method': request.method,
								},
							}),
						),
					),
				),
			)
		const paginate = <Page, A>(input: PageInput<Page, A>): Effect.Effect<ReadonlyArray<A>, GitHubApiError> => {
			const query: Array<readonly [string, string]> = [['per_page', '100']]
			if (Predicate.isNotUndefined(input.query)) query.push(...input.query)
			return Stream.paginate<string, A, GitHubApiError>(urlWithQuery(input.path, query), (url) =>
				retryWithFreshToken(
					input.ref,
					authenticatedRequest({ operation: input.operation, ref: input.ref, method: 'GET', path: url }).pipe(
						Effect.flatMap((request) =>
							observeAndNarrow(
								input.operation,
								client.execute(request).pipe(
									Effect.mapError(() => GitHubTransportError.make({ stage: 'transport' })),
									Effect.flatMap(inspectStatus),
									Effect.flatMap((response) =>
										response.json.pipe(
											Effect.flatMap(Schema.decodeUnknownEffect(input.schema)),
											Effect.mapError(() =>
												GitHubTransportError.make({ stage: 'decode', status: response.status }),
											),
											Effect.flatMap((page) =>
												parseNextLink(response, apiOrigin).pipe(
													Effect.map((next) => [input.items(page), next] as const),
												),
											),
										),
									),
								),
							),
						),
					),
				),
			).pipe(
				Stream.runCollect,
				Effect.map((items): ReadonlyArray<A> => Array.from(items)),
			)
		}
		const list = <A>(
			input: Omit<PageInput<ReadonlyArray<A>, A>, 'items' | 'schema'> & {
				readonly schema: Schema.Codec<A, unknown, never, never>
			},
		) => paginate({ ...input, schema: Schema.Array(input.schema), items: (page) => page })
		const text = (
			input: Omit<CallInput<never>, 'schema' | 'body'> & {
				readonly followRedirects?: boolean
				readonly accept?: string
			},
		) =>
			retryWithFreshToken(
				input.ref,
				authenticatedRequest({
					operation: input.operation,
					ref: input.ref,
					method: input.method,
					path: urlWithQuery(input.path),
				}).pipe(
					Effect.map((request) => {
						if (Predicate.isUndefined(input.accept)) return request
						return HttpClientRequest.setHeader(request, 'accept', input.accept)
					}),
					Effect.flatMap((request) => {
						let transport = client
						if (input.followRedirects === true) transport = HttpClient.followRedirects(client, 3)
						return observeAndNarrow(
							input.operation,
							transport.execute(request).pipe(
								Effect.mapError(() => GitHubTransportError.make({ stage: 'transport' })),
								Effect.flatMap(inspectStatus),
								Effect.flatMap((response) =>
									response.text.pipe(
										Effect.mapError(() =>
											GitHubTransportError.make({ stage: 'decode', status: response.status }),
										),
									),
								),
							),
						).pipe(
							Effect.withSpan('github.api.request', {
								attributes: {
									'github.operation': input.operation,
									'http.request.method': request.method,
								},
							}),
						)
					}),
				),
			)

		const botUserId = yield* Effect.cached(
			Option.match(config.botUserId, {
				onSome: Effect.succeed,
				onNone: () =>
					Effect.gen(function* () {
						const jwt = yield* appJwt().pipe(
							Effect.catchTag('GitHubTransportError', (error) =>
								Effect.fail(narrowGitHubTransportError('remove_reaction', error)),
							),
						)
						const app = yield* execute(
							'remove_reaction',
							baseRequest('GET', new URL('app', apiOrigin).toString()).pipe(
								HttpClientRequest.bearerToken(jwt),
							),
							AppResponse,
						)
						const bot = yield* execute(
							'remove_reaction',
							baseRequest(
								'GET',
								new URL(`users/${encodeURIComponent(`${app.slug}[bot]`)}`, apiOrigin).toString(),
							),
							Participant,
						)
						return bot.id
					}),
			}),
		)
		return GitHubApiClient.of({ call, callVoid, list, paginate, text, botUserId })
	}),
)

const parseNextLink = (response: HttpClientResponse.HttpClientResponse, apiOrigin: URL) => {
	const link = response.headers.link
	if (Predicate.isUndefined(link)) return Effect.succeedNone
	const entry = link.split(',').find((candidate) => candidate.includes('rel="next"'))
	if (Predicate.isUndefined(entry)) return Effect.succeedNone
	const target = /<([^>]+)>/.exec(entry)?.[1]
	if (Predicate.isUndefined(target)) return Effect.fail(GitHubTransportError.make({ stage: 'pagination' }))
	return Effect.try({
		try: () => new URL(target, apiOrigin),
		catch: () => GitHubTransportError.make({ stage: 'pagination' }),
	}).pipe(
		Effect.flatMap((next) => {
			if (next.origin !== apiOrigin.origin) return Effect.fail(GitHubTransportError.make({ stage: 'pagination' }))
			return Effect.succeedSome(next.toString())
		}),
	)
}
