import { Config, Context, Effect, Layer, Option, Predicate, Schema, Stream } from 'effect'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientRequest from 'effect/http/HttpClientRequest'
import type * as HttpClientResponse from 'effect/http/HttpClientResponse'

import { type GitHubApiOperation, GitHubApiError } from '../GitHubApi'
import { GitHubId } from '../GitHubIdentity'
import type { GitHubRepositoryRef } from '../GitHubModels'
import { GitHubTransportError, narrowGitHubTransportError } from './GitHubApiErrors'
import { Participant } from './GitHubApiSchemas'
import { GitHubAppCredentials } from './GitHubAppCredentials'
import { type ApiMethod, gitHubRequest, inspectGitHubStatus } from './GitHubHttp'

const GitHubApiConfig = Config.all({
	apiOrigin: Config.URL('GITHUB_API_ORIGIN').pipe(Config.withDefault(new URL('https://api.github.com/'))),
	botUserId: Config.option(Config.schema(GitHubId, 'GITHUB_BOT_USER_ID')),
})

const AppResponse = Schema.Struct({ slug: Schema.NonEmptyString })
type RequestInput = {
	readonly operation: GitHubApiOperation
	readonly ref: GitHubRepositoryRef
	readonly method: ApiMethod
	readonly path: string
}
/** A JSON request body and the operation's Schema that encodes it. */
type RequestBody<B> = {
	readonly schema: Schema.Codec<B, unknown, never, never>
	readonly value: B
}
type CallInput<A, B> = RequestInput & {
	readonly schema: Schema.Codec<A, unknown, never, never>
	readonly body?: RequestBody<B>
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
		readonly call: <A, B>(input: CallInput<A, B>) => Effect.Effect<A, GitHubApiError>
		readonly callVoid: (input: RequestInput) => Effect.Effect<void, GitHubApiError>
		readonly list: <A>(
			input: Omit<PageInput<ReadonlyArray<A>, A>, 'items' | 'schema'> & {
				readonly schema: Schema.Codec<A, unknown, never, never>
			},
		) => Effect.Effect<ReadonlyArray<A>, GitHubApiError>
		readonly paginate: <Page, A>(input: PageInput<Page, A>) => Effect.Effect<ReadonlyArray<A>, GitHubApiError>
		readonly text: (
			input: RequestInput & {
				readonly followRedirects?: boolean
				readonly accept?: string
			},
		) => Effect.Effect<string, GitHubApiError>
		readonly botUserId: Effect.Effect<GitHubId, GitHubApiError>
	}
>()('@humanlayer/channels-github/internal/GitHubApiClient') {}

export const GitHubApiClientLive = Layer.effect(
	GitHubApiClient,
	Effect.gen(function* () {
		const client = yield* HttpClient.HttpClient
		const credentials = yield* GitHubAppCredentials
		const config = yield* GitHubApiConfig
		const apiOrigin = new URL(config.apiOrigin.toString().replace(/\/?$/, '/'))

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
					Effect.flatMap(inspectGitHubStatus),
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

		const authenticatedRequest = Effect.fn('github.api.authenticated_request')(function* (input: RequestInput) {
			const token = yield* credentials
				.installationToken(input.ref)
				.pipe(
					Effect.catchTag('GitHubTransportError', (error) =>
						Effect.fail(narrowGitHubTransportError(input.operation, error)),
					),
				)
			return gitHubRequest(input.method, input.path).pipe(HttpClientRequest.bearerToken(token))
		})
		const withBody = <B>(
			operation: GitHubApiOperation,
			request: HttpClientRequest.HttpClientRequest,
			body: RequestBody<B> | undefined,
		) => {
			if (Predicate.isUndefined(body)) return Effect.succeed(request)
			return HttpClientRequest.schemaBodyJson(body.schema)(request, body.value).pipe(
				Effect.mapError(() => GitHubApiError.make({ operation, reason: 'invalid_response', retryable: false })),
			)
		}

		/**
		 * Try once more with a new installation token when GitHub refuses the cached one. A refusal of the new
		 * token too means the app's own credentials are wrong, and is logged as a configuration error.
		 */
		const retryWithFreshToken = <A>(ref: GitHubRepositoryRef, operation: Effect.Effect<A, GitHubApiError>) =>
			operation.pipe(
				Effect.catchTag('GitHubApiError', (error) => {
					if (error.reason !== 'authentication') return Effect.fail(error)
					return credentials.invalidateInstallationToken(ref).pipe(
						Effect.andThen(operation),
						Effect.tapError((retried) =>
							retried.reason === 'authentication'
								? Effect.logError(
										'GitHub rejected the app credentials; check GITHUB_APP_ID and GITHUB_PRIVATE_KEY',
									).pipe(
										Effect.annotateLogs({
											provider: 'github',
											credential: 'app_private_key',
											operation: retried.operation,
											status: retried.status ?? 'none',
										}),
									)
								: Effect.void,
						),
					)
				}),
			)
		const urlWithQuery = (path: string, query: ReadonlyArray<readonly [string, string]> = []) => {
			const url = new URL(path.replace(/^\//, ''), apiOrigin)
			for (const [key, value] of query) url.searchParams.set(key, value)
			return url.toString()
		}

		const call = <A, B>(input: CallInput<A, B>) =>
			retryWithFreshToken(
				input.ref,
				authenticatedRequest({
					operation: input.operation,
					ref: input.ref,
					method: input.method,
					path: urlWithQuery(input.path),
				}).pipe(
					Effect.flatMap((request) => withBody(input.operation, request, input.body)),
					Effect.flatMap((request) => execute(input.operation, request, input.schema)),
				),
			)
		const callVoid = (input: RequestInput) =>
			retryWithFreshToken(
				input.ref,
				authenticatedRequest({ ...input, path: urlWithQuery(input.path) }).pipe(
					Effect.flatMap((request) =>
						observeAndNarrow(
							input.operation,
							client.execute(request).pipe(
								Effect.mapError(() => GitHubTransportError.make({ stage: 'transport' })),
								Effect.flatMap(inspectGitHubStatus),
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
									Effect.flatMap(inspectGitHubStatus),
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
			input: RequestInput & {
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
								Effect.flatMap(inspectGitHubStatus),
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
						const jwt = yield* credentials.appJwt.pipe(
							Effect.catchTag('GitHubTransportError', (error) =>
								Effect.fail(narrowGitHubTransportError('remove_reaction', error)),
							),
						)
						const app = yield* execute(
							'remove_reaction',
							gitHubRequest('GET', new URL('app', apiOrigin).toString()).pipe(
								HttpClientRequest.bearerToken(jwt),
							),
							AppResponse,
						)
						const bot = yield* execute(
							'remove_reaction',
							gitHubRequest(
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
