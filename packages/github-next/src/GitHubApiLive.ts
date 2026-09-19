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
	Match,
	Option,
	Predicate,
	Redacted,
	Schema,
	Stream,
} from 'effect'
import * as FetchHttpClient from 'effect/unstable/http/FetchHttpClient'
import * as HttpClient from 'effect/unstable/http/HttpClient'
import * as HttpClientRequest from 'effect/unstable/http/HttpClientRequest'
import type * as HttpClientResponse from 'effect/unstable/http/HttpClientResponse'

import {
	GitHubApi,
	GitHubApiError,
	type GitHubApiOperation,
	type GitHubDeleteComment,
	type GitHubReactionRequest,
	type GitHubUpdateComment,
} from './GitHubApi'
import { GitHubId } from './GitHubIdentity'
import {
	type GitHubCommentRef,
	type GitHubIssueCommentRef,
	GitHubIssueInfo,
	type GitHubIssueRef,
	GitHubParticipant,
	GitHubPullRequestInfo,
	type GitHubPullRequestRef,
	GitHubReview,
	GitHubReviewCommentRef,
	type GitHubReviewState,
} from './GitHubModels'
import {
	GitHubIssueComment,
	type GitHubIssueComments,
	GitHubReviewComment,
	type GitHubReviewComments,
	type GitHubReviews,
} from './GitHubResources'

const GitHubApiConfig = Config.all({
	appId: Config.schema(GitHubId, 'GITHUB_APP_ID'),
	privateKey: Config.redacted('GITHUB_PRIVATE_KEY'),
	apiOrigin: Config.url('GITHUB_API_ORIGIN').pipe(Config.withDefault(new URL('https://api.github.com/'))),
	botUserId: Config.option(Config.schema(GitHubId, 'GITHUB_BOT_USER_ID')),
})

const GitHubInstallationTokenResponse = Schema.Struct({
	token: Schema.NonEmptyString,
	expires_at: Schema.String,
})

const GitHubAppResponse = Schema.Struct({
	slug: Schema.NonEmptyString,
})

const GitHubApiParticipant = Schema.Struct({
	id: GitHubId,
	login: Schema.NonEmptyString,
	type: Schema.String,
})

const GitHubApiIssue = Schema.Struct({
	number: GitHubId,
	title: Schema.String,
	body: Schema.NullOr(Schema.String),
	state: Schema.Literals(['open', 'closed']),
	html_url: Schema.String,
	user: GitHubApiParticipant,
})

const GitHubApiPullRequest = Schema.Struct({
	number: GitHubId,
	title: Schema.String,
	body: Schema.NullOr(Schema.String),
	state: Schema.Literals(['open', 'closed']),
	html_url: Schema.String,
	user: Schema.NullOr(GitHubApiParticipant),
	draft: Schema.Boolean,
	merged: Schema.Boolean,
	head: Schema.Struct({ ref: Schema.String, sha: Schema.NonEmptyString }),
	base: Schema.Struct({ ref: Schema.String, sha: Schema.NonEmptyString }),
})

const GitHubApiIssueComment = Schema.Struct({
	id: GitHubId,
	body: Schema.String,
	html_url: Schema.String,
	user: Schema.NullOr(GitHubApiParticipant),
})

const GitHubApiReview = Schema.Struct({
	id: GitHubId,
	node_id: Schema.NonEmptyString,
	body: Schema.NullOr(Schema.String),
	user: Schema.NullOr(GitHubApiParticipant),
	state: Schema.String,
	commit_id: Schema.String,
	html_url: Schema.String,
})

const GitHubApiReviewComment = Schema.Struct({
	id: GitHubId,
	node_id: Schema.NonEmptyString,
	body: Schema.String,
	html_url: Schema.String,
	user: Schema.NullOr(GitHubApiParticipant),
	pull_request_review_id: Schema.NullOr(GitHubId),
	path: Schema.String,
	commit_id: Schema.String,
	original_commit_id: Schema.String,
	diff_hunk: Schema.String,
	in_reply_to_id: Schema.optionalKey(Schema.NullOr(GitHubId)),
	line: Schema.optionalKey(Schema.NullOr(Schema.Number)),
	start_line: Schema.optionalKey(Schema.NullOr(Schema.Number)),
	side: Schema.optionalKey(Schema.Literals(['LEFT', 'RIGHT'])),
})

const GitHubApiReaction = Schema.Struct({
	id: GitHubId,
	content: Schema.String,
	user: Schema.NullOr(GitHubApiParticipant),
})

class GitHubTransportError extends Schema.TaggedError<GitHubTransportError>()('GitHubTransportError', {
	stage: Schema.Literals(['transport', 'status', 'decode', 'signing', 'token_expiry', 'pagination']),
	status: Schema.optionalKey(Schema.Int),
	rateLimited: Schema.optionalKey(Schema.Boolean),
	retryAfterMs: Schema.optionalKey(Schema.Int),
}) {}

class GitHubSigningError extends Schema.TaggedError<GitHubSigningError>()('GitHubSigningError', {}) {}

/** Package-private signing seam. Production uses Web Crypto; tests replace it with a Layer. */
export class GitHubAppSigner extends Context.Service<
	GitHubAppSigner,
	{
		readonly sign: (input: {
			readonly privateKey: Redacted.Redacted<string>
			readonly data: string
		}) => Effect.Effect<string, GitHubSigningError>
	}
>()('@humanlayer/channels-github-next/internal/GitHubAppSigner') {
	static readonly layerWebCrypto = Layer.sync(GitHubAppSigner, () => {
		const encoder = new TextEncoder()
		return GitHubAppSigner.of({
			sign: ({ privateKey, data }) =>
				Effect.tryPromise({
					try: async () => {
						const bytes = privateKeyBytes(Redacted.value(privateKey))
						const key = await globalThis.crypto.subtle.importKey(
							'pkcs8',
							bytes,
							{ name: 'RSASSA-PKCS1-v1_5', hash: 'SHA-256' },
							false,
							['sign'],
						)
						const signature = await globalThis.crypto.subtle.sign(
							'RSASSA-PKCS1-v1_5',
							key,
							encoder.encode(data),
						)
						return Encoding.encodeBase64Url(new Uint8Array(signature))
					},
					catch: () => GitHubSigningError.make({}),
				}),
		})
	})
}

const pemWhitespace = /\s/g
const der = (tag: number, bytes: Uint8Array): Uint8Array<ArrayBuffer> => {
	const length =
		bytes.length < 128
			? [bytes.length]
			: bytes.length < 256
				? [0x81, bytes.length]
				: [0x82, bytes.length >> 8, bytes.length & 255]
	return new Uint8Array([tag, ...length, ...bytes])
}

const privateKeyBytes = (pem: string) => {
	const pkcs1 = pem.startsWith('-----BEGIN RSA PRIVATE KEY-----')
	const label = pkcs1 ? 'RSA PRIVATE KEY' : 'PRIVATE KEY'
	if (!pem.startsWith(`-----BEGIN ${label}-----`) || !pem.trimEnd().endsWith(`-----END ${label}-----`)) {
		throw new Error('Invalid PEM')
	}
	const bytes = Uint8Array.from(
		atob(
			pem
				.replace(`-----BEGIN ${label}-----`, '')
				.replace(`-----END ${label}-----`, '')
				.replace(pemWhitespace, ''),
		),
		(character) => character.charCodeAt(0),
	)
	if (!pkcs1) return bytes
	return der(
		0x30,
		new Uint8Array([
			0x02,
			0x01,
			0x00,
			0x30,
			0x0d,
			0x06,
			0x09,
			0x2a,
			0x86,
			0x48,
			0x86,
			0xf7,
			0x0d,
			0x01,
			0x01,
			0x01,
			0x05,
			0x00,
			...der(0x04, bytes),
		]),
	)
}

const repositoryPath = (ref: GitHubIssueRef | GitHubPullRequestRef) =>
	`/repos/${encodeURIComponent(ref.owner)}/${encodeURIComponent(ref.repository)}`

const issueCommentRef = (discussion: GitHubIssueCommentRef['discussion'], id: GitHubId): GitHubIssueCommentRef => ({
	discussion,
	id,
})

const participant = (value: typeof GitHubApiParticipant.Type) => GitHubParticipant.make(value)

const reviewState = (state: string): GitHubReviewState =>
	Match.value(state.toLowerCase()).pipe(
		Match.when('approved', () => 'approved' as const),
		Match.when('changes_requested', () => 'changes_requested' as const),
		Match.when('commented', () => 'commented' as const),
		Match.when('dismissed', () => 'dismissed' as const),
		Match.orElse(() => 'pending' as const),
	)

const issueComment = (discussion: GitHubIssueCommentRef['discussion'], value: typeof GitHubApiIssueComment.Type) =>
	GitHubIssueComment.make({
		ref: issueCommentRef(discussion, value.id),
		body: value.body,
		url: value.html_url,
		author: value.user === null ? null : participant(value.user),
	})

const reviewComment = (pullRequest: GitHubPullRequestRef, value: typeof GitHubApiReviewComment.Type) =>
	GitHubReviewComment.make({
		ref: { pullRequest, id: value.id },
		nodeId: value.node_id,
		body: value.body,
		url: value.html_url,
		author: value.user === null ? null : participant(value.user),
		reviewId: value.pull_request_review_id,
		path: value.path,
		commitId: value.commit_id,
		originalCommitId: value.original_commit_id,
		diffHunk: value.diff_hunk,
		...(Predicate.isUndefined(value.in_reply_to_id) ? {} : { inReplyToId: value.in_reply_to_id }),
		...(Predicate.isUndefined(value.line) ? {} : { line: value.line }),
		...(Predicate.isUndefined(value.start_line) ? {} : { startLine: value.start_line }),
		...(Predicate.isUndefined(value.side) ? {} : { side: value.side }),
	})

const commentRepository = (comment: GitHubCommentRef) =>
	Schema.is(GitHubReviewCommentRef)(comment)
		? comment.pullRequest
		: Match.value(comment.discussion).pipe(
				Match.tagsExhaustive({ Issue: ({ ref }) => ref, PullRequest: ({ ref }) => ref }),
			)

const commentPath = (comment: GitHubCommentRef) =>
	Schema.is(GitHubReviewCommentRef)(comment)
		? `${repositoryPath(comment.pullRequest)}/pulls/comments/${comment.id}`
		: `${repositoryPath(commentRepository(comment))}/issues/comments/${comment.id}`

const secondsPattern = /^\d+$/
const secondsToMillis = (value: string | undefined) => {
	if (Predicate.isUndefined(value) || !secondsPattern.test(value)) return undefined
	const milliseconds = Number(value) * 1_000
	return Number.isSafeInteger(milliseconds) ? milliseconds : undefined
}

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
		Effect.flatMap((next) =>
			next.origin === apiOrigin.origin
				? Effect.succeedSome(next.toString())
				: Effect.fail(GitHubTransportError.make({ stage: 'pagination' })),
		),
	)
}

type TokenEntry = {
	readonly token: Redacted.Redacted<string>
	readonly ttl: number
}

type TokenKey = {
	readonly installationId: GitHubId
	readonly repositoryId: GitHubId
}

const tokenKey = (ref: GitHubIssueRef | GitHubPullRequestRef): TokenKey => ({
	installationId: ref.installationId,
	repositoryId: ref.repositoryId,
})

const cacheKey = (key: TokenKey) => JSON.stringify([key.installationId, key.repositoryId])

/** Live GitHub API implementation with an injectable Effect HTTP transport and signer. */
export const GitHubApiLiveBase = Layer.effect(
	GitHubApi,
	Effect.gen(function* () {
		const client = yield* HttpClient.HttpClient
		const signer = yield* GitHubAppSigner
		const config = yield* GitHubApiConfig
		const apiOrigin = new URL(config.apiOrigin.toString().replace(/\/?$/, '/'))

		const baseRequest = (method: 'GET' | 'POST' | 'PATCH' | 'DELETE', url: string) =>
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

		const classifyTransport = (operation: GitHubApiOperation, error: GitHubTransportError): GitHubApiError => {
			if (error.rateLimited === true) {
				return GitHubApiError.make({
					operation,
					reason: 'rate_limited',
					retryable: true,
					...(Predicate.isUndefined(error.retryAfterMs) ? {} : { retryAfterMs: error.retryAfterMs }),
				})
			}
			if (error.status === 401)
				return GitHubApiError.make({ operation, reason: 'authentication', retryable: false })
			if (error.status === 403) return GitHubApiError.make({ operation, reason: 'forbidden', retryable: false })
			if (error.status === 404 || error.status === 410) {
				return GitHubApiError.make({ operation, reason: 'not_found', retryable: false })
			}
			if (error.stage === 'signing') {
				return GitHubApiError.make({ operation, reason: 'authentication', retryable: false })
			}
			if (error.stage === 'decode' || error.stage === 'token_expiry' || error.stage === 'pagination') {
				return GitHubApiError.make({ operation, reason: 'invalid_response', retryable: false })
			}
			if (Predicate.isNotUndefined(error.status) && error.status >= 400 && error.status < 500) {
				return GitHubApiError.make({ operation, reason: 'unavailable', retryable: false })
			}
			return GitHubApiError.make({ operation, reason: 'unavailable', retryable: true })
		}

		const inspectStatus = Effect.fn('github.api.inspect_status')(function* (
			response: HttpClientResponse.HttpClientResponse,
		) {
			if (response.status >= 200 && response.status < 300) return response
			const retryAfterMs = secondsToMillis(response.headers['retry-after'])
			const rateLimited =
				response.status === 429 ||
				(response.status === 403 &&
					(response.headers['x-ratelimit-remaining'] === '0' || Predicate.isNotUndefined(retryAfterMs)))
			return yield* GitHubTransportError.make({
				stage: 'status',
				status: response.status,
				...(rateLimited ? { rateLimited: true } : {}),
				...(Predicate.isUndefined(retryAfterMs) ? {} : { retryAfterMs }),
			})
		})

		const execute = <S extends Schema.Top>(input: {
			readonly operation: GitHubApiOperation
			readonly request: HttpClientRequest.HttpClientRequest
			readonly schema: S
		}) =>
			client.execute(input.request).pipe(
				Effect.mapError(() => GitHubTransportError.make({ stage: 'transport' })),
				Effect.flatMap(inspectStatus),
				Effect.flatMap((response) =>
					response.json.pipe(
						Effect.flatMap(Schema.decodeUnknownEffect(input.schema)),
						Effect.mapError(() => GitHubTransportError.make({ stage: 'decode', status: response.status })),
					),
				),
				Effect.tapError((error) =>
					Effect.logError('GitHub API request failed', error).pipe(
						Effect.annotateLogs({ operation: input.operation }),
					),
				),
				Effect.catchTag('GitHubTransportError', (error) =>
					Effect.fail(classifyTransport(input.operation, error)),
				),
				Effect.withSpan('github.api.request', {
					attributes: { 'github.operation': input.operation, 'http.request.method': input.request.method },
				}),
			)

		const executeVoid = (input: {
			readonly operation: GitHubApiOperation
			readonly request: HttpClientRequest.HttpClientRequest
		}) =>
			client.execute(input.request).pipe(
				Effect.mapError(() => GitHubTransportError.make({ stage: 'transport' })),
				Effect.flatMap(inspectStatus),
				Effect.asVoid,
				Effect.tapError((error) =>
					Effect.logError('GitHub API request failed', error).pipe(
						Effect.annotateLogs({ operation: input.operation }),
					),
				),
				Effect.catchTag('GitHubTransportError', (error) =>
					Effect.fail(classifyTransport(input.operation, error)),
				),
				Effect.withSpan('github.api.request', {
					attributes: { 'github.operation': input.operation, 'http.request.method': input.request.method },
				}),
			)

		const botUserId = yield* Effect.cached(
			Option.match(config.botUserId, {
				onSome: Effect.succeed,
				onNone: () =>
					Effect.gen(function* () {
						const jwt = yield* appJwt().pipe(
							Effect.catchTag('GitHubTransportError', (error) =>
								Effect.fail(classifyTransport('remove_reaction', error)),
							),
						)
						const app = yield* execute({
							operation: 'remove_reaction',
							request: baseRequest('GET', new URL('app', apiOrigin).toString()).pipe(
								HttpClientRequest.bearerToken(jwt),
							),
							schema: GitHubAppResponse,
						})
						const bot = yield* execute({
							operation: 'remove_reaction',
							request: baseRequest(
								'GET',
								new URL(`users/${encodeURIComponent(`${app.slug}[bot]`)}`, apiOrigin).toString(),
							),
							schema: GitHubApiParticipant,
						})
						return bot.id
					}),
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
								Effect.flatMap(Schema.decodeUnknownEffect(GitHubInstallationTokenResponse)),
								Effect.mapError(() => GitHubTransportError.make({ stage: 'decode' })),
							),
						),
					)
					const expiresAt = Date.parse(response.expires_at)
					const receivedAt = yield* Clock.currentTimeMillis
					if (!Number.isFinite(expiresAt) || expiresAt <= receivedAt + 60_000) {
						return yield* GitHubTransportError.make({ stage: 'token_expiry' })
					}
					return {
						token: Redacted.make(response.token),
						ttl: Math.min(expiresAt - receivedAt - 60_000, 3_540_000),
					} satisfies TokenEntry
				}).pipe(Effect.tapError((error) => Effect.logError('GitHub installation token request failed', error))),
			{
				capacity: 1_000,
				timeToLive: (exit) => (Exit.isSuccess(exit) ? Duration.millis(exit.value.ttl) : Duration.zero),
			},
		)

		const authenticatedRequest = Effect.fn('github.api.authenticated_request')(function* (input: {
			operation: GitHubApiOperation
			ref: GitHubIssueRef | GitHubPullRequestRef
			method: 'GET' | 'POST' | 'PATCH' | 'DELETE'
			url: string
			body?: Schema.Json
		}) {
			const token = yield* Cache.get(tokenCache, cacheKey(tokenKey(input.ref))).pipe(
				Effect.catchTag('GitHubTransportError', (error) =>
					Effect.fail(classifyTransport(input.operation, error)),
				),
			)
			const request = baseRequest(input.method, input.url).pipe(HttpClientRequest.bearerToken(token.token))
			if (Predicate.isUndefined(input.body)) return request
			return yield* HttpClientRequest.schemaBodyJson(Schema.Json)(request, input.body).pipe(
				Effect.mapError(() =>
					GitHubApiError.make({ operation: input.operation, reason: 'invalid_response', retryable: false }),
				),
			)
		})

		const retryWithFreshToken = <A, R>(
			ref: GitHubIssueRef | GitHubPullRequestRef,
			operation: Effect.Effect<A, GitHubApiError, R>,
		): Effect.Effect<A, GitHubApiError, R> =>
			operation.pipe(
				Effect.catchTag('GitHubApiError', (error) =>
					error.reason === 'authentication'
						? Cache.invalidate(tokenCache, cacheKey(tokenKey(ref))).pipe(Effect.andThen(operation))
						: Effect.fail(error),
				),
			)

		const call = <S extends Schema.Top>(input: {
			readonly operation: GitHubApiOperation
			readonly ref: GitHubIssueRef | GitHubPullRequestRef
			readonly method: 'GET' | 'POST' | 'PATCH' | 'DELETE'
			readonly path: string
			readonly schema: S
			readonly body?: Schema.Json
		}) => {
			const operation = authenticatedRequest({
				operation: input.operation,
				ref: input.ref,
				method: input.method,
				url: new URL(input.path.replace(/^\//, ''), apiOrigin).toString(),
				...(Predicate.isUndefined(input.body) ? {} : { body: input.body }),
			}).pipe(Effect.flatMap((request) => execute({ operation: input.operation, request, schema: input.schema })))
			return retryWithFreshToken(input.ref, operation)
		}

		const callVoid = (input: {
			readonly operation: GitHubApiOperation
			readonly ref: GitHubIssueRef | GitHubPullRequestRef
			readonly method: 'DELETE'
			readonly path: string
		}) => {
			const operation = authenticatedRequest({
				operation: input.operation,
				ref: input.ref,
				method: input.method,
				url: new URL(input.path.replace(/^\//, ''), apiOrigin).toString(),
			}).pipe(Effect.flatMap((request) => executeVoid({ operation: input.operation, request })))
			return retryWithFreshToken(input.ref, operation)
		}

		const list = <A>(input: {
			readonly operation: GitHubApiOperation
			readonly ref: GitHubIssueRef | GitHubPullRequestRef
			readonly path: string
			readonly schema: Schema.Codec<A, unknown, never, never>
		}): Effect.Effect<ReadonlyArray<A>, GitHubApiError> =>
			Stream.paginate<string, A, GitHubApiError>(
				new URL(`${input.path.replace(/^\//, '')}?per_page=100`, apiOrigin).toString(),
				(url) => {
					const operation = authenticatedRequest({
						operation: input.operation,
						ref: input.ref,
						method: 'GET',
						url,
					}).pipe(
						Effect.flatMap((request) =>
							client.execute(request).pipe(
								Effect.mapError(() => GitHubTransportError.make({ stage: 'transport' })),
								Effect.flatMap(inspectStatus),
								Effect.flatMap((response) =>
									response.json.pipe(
										Effect.flatMap(Schema.decodeUnknownEffect(Schema.Array(input.schema))),
										Effect.mapError(() =>
											GitHubTransportError.make({ stage: 'decode', status: response.status }),
										),
										Effect.flatMap((items) =>
											parseNextLink(response, apiOrigin).pipe(
												Effect.map((next) => [items, next] as const),
											),
										),
									),
								),
							),
						),
						Effect.tapError((error) =>
							Effect.logError('GitHub API pagination request failed', error).pipe(
								Effect.annotateLogs({ operation: input.operation }),
							),
						),
						Effect.catchTag('GitHubTransportError', (error) =>
							Effect.fail(classifyTransport(input.operation, error)),
						),
					)
					return retryWithFreshToken(input.ref, operation)
				},
			).pipe(
				Stream.runCollect,
				Effect.map((items) => Array.from(items)),
			)

		const fetchIssue = Effect.fn('github.api.fetch_issue')(function* (input: { issue: GitHubIssueRef }) {
			const value = yield* call({
				operation: 'fetch_issue',
				ref: input.issue,
				method: 'GET',
				path: `${repositoryPath(input.issue)}/issues/${input.issue.number}`,
				schema: GitHubApiIssue,
			})
			if (value.number !== input.issue.number) {
				return yield* GitHubApiError.make({
					operation: 'fetch_issue',
					reason: 'invalid_response',
					retryable: false,
				})
			}
			return GitHubIssueInfo.make({
				ref: input.issue,
				title: value.title,
				body: value.body,
				state: value.state,
				url: value.html_url,
				author: participant(value.user),
			})
		})

		const fetchPullRequest = Effect.fn('github.api.fetch_pull_request')(function* (input: {
			pullRequest: GitHubPullRequestRef
		}) {
			const value = yield* call({
				operation: 'fetch_pull_request',
				ref: input.pullRequest,
				method: 'GET',
				path: `${repositoryPath(input.pullRequest)}/pulls/${input.pullRequest.number}`,
				schema: GitHubApiPullRequest,
			})
			if (value.number !== input.pullRequest.number) {
				return yield* GitHubApiError.make({
					operation: 'fetch_pull_request',
					reason: 'invalid_response',
					retryable: false,
				})
			}
			return GitHubPullRequestInfo.make({
				ref: input.pullRequest,
				title: value.title,
				body: value.body,
				state: value.state,
				url: value.html_url,
				author: value.user === null ? null : participant(value.user),
				draft: value.draft,
				merged: value.merged,
				headRef: value.head.ref,
				headSha: value.head.sha,
				baseRef: value.base.ref,
				baseSha: value.base.sha,
			})
		})

		const listIssueComments = Effect.fn('github.api.list_issue_comments')(function* (input: {
			issue: GitHubIssueRef
		}): Effect.fn.Return<GitHubIssueComments, GitHubApiError> {
			const values = yield* list({
				operation: 'list_issue_comments',
				ref: input.issue,
				path: `${repositoryPath(input.issue)}/issues/${input.issue.number}/comments`,
				schema: GitHubApiIssueComment,
			})
			const discussion = { _tag: 'Issue', ref: input.issue } as const
			return values.map((value) => issueComment(discussion, value))
		})

		const listPullRequestComments = Effect.fn('github.api.list_pull_request_comments')(function* (input: {
			pullRequest: GitHubPullRequestRef
		}): Effect.fn.Return<GitHubIssueComments, GitHubApiError> {
			const values = yield* list({
				operation: 'list_pull_request_comments',
				ref: input.pullRequest,
				path: `${repositoryPath(input.pullRequest)}/issues/${input.pullRequest.number}/comments`,
				schema: GitHubApiIssueComment,
			})
			const discussion = { _tag: 'PullRequest', ref: input.pullRequest } as const
			return values.map((value) => issueComment(discussion, value))
		})

		const listPullRequestReviews = Effect.fn('github.api.list_reviews')(function* (input: {
			pullRequest: GitHubPullRequestRef
		}): Effect.fn.Return<GitHubReviews, GitHubApiError> {
			const values = yield* list({
				operation: 'list_pull_request_reviews',
				ref: input.pullRequest,
				path: `${repositoryPath(input.pullRequest)}/pulls/${input.pullRequest.number}/reviews`,
				schema: GitHubApiReview,
			})
			return values.map((value) =>
				GitHubReview.make({
					ref: { pullRequest: input.pullRequest, id: value.id, nodeId: value.node_id },
					body: value.body,
					author: value.user === null ? null : participant(value.user),
					state: reviewState(value.state),
					commitId: value.commit_id,
					url: value.html_url,
				}),
			)
		})

		const listPullRequestReviewComments = Effect.fn('github.api.list_review_comments')(function* (input: {
			pullRequest: GitHubPullRequestRef
		}): Effect.fn.Return<GitHubReviewComments, GitHubApiError> {
			const values = yield* list({
				operation: 'list_pull_request_review_comments',
				ref: input.pullRequest,
				path: `${repositoryPath(input.pullRequest)}/pulls/${input.pullRequest.number}/comments`,
				schema: GitHubApiReviewComment,
			})
			return values.map((value) => reviewComment(input.pullRequest, value))
		})

		const postIssueComment = Effect.fn('github.api.post_comment')(function* (input: {
			issue: GitHubIssueRef
			content: { readonly markdown: string }
		}) {
			const value = yield* call({
				operation: 'post_issue_comment',
				ref: input.issue,
				method: 'POST',
				path: `${repositoryPath(input.issue)}/issues/${input.issue.number}/comments`,
				schema: GitHubApiIssueComment,
				body: { body: input.content.markdown },
			})
			return issueComment({ _tag: 'Issue', ref: input.issue }, value)
		})

		const postPullRequestComment = Effect.fn('github.api.post_pull_request_comment')(function* (input: {
			pullRequest: GitHubPullRequestRef
			content: { readonly markdown: string }
		}) {
			const value = yield* call({
				operation: 'post_pull_request_comment',
				ref: input.pullRequest,
				method: 'POST',
				path: `${repositoryPath(input.pullRequest)}/issues/${input.pullRequest.number}/comments`,
				schema: GitHubApiIssueComment,
				body: { body: input.content.markdown },
			})
			return issueComment({ _tag: 'PullRequest', ref: input.pullRequest }, value)
		})

		const replyToReviewComment = Effect.fn('github.api.reply_to_review_comment')(function* (input: {
			pullRequest: GitHubPullRequestRef
			comment: GitHubReviewCommentRef
			content: { readonly markdown: string }
		}) {
			const value = yield* call({
				operation: 'reply_to_review_comment',
				ref: input.pullRequest,
				method: 'POST',
				path: `${repositoryPath(input.pullRequest)}/pulls/${input.pullRequest.number}/comments/${input.comment.id}/replies`,
				schema: GitHubApiReviewComment,
				body: { body: input.content.markdown },
			})
			return reviewComment(input.pullRequest, value)
		})

		const updateComment = Effect.fn('github.api.update_comment')(function* (input: GitHubUpdateComment) {
			const ref = input.comment
			const repository = commentRepository(ref)
			const value = yield* Schema.is(GitHubReviewCommentRef)(ref)
				? call({
						operation: 'update_comment',
						ref: repository,
						method: 'PATCH',
						path: commentPath(ref),
						schema: GitHubApiReviewComment,
						body: { body: input.content.markdown },
					}).pipe(Effect.map((comment) => reviewComment(ref.pullRequest, comment)))
				: call({
						operation: 'update_comment',
						ref: repository,
						method: 'PATCH',
						path: commentPath(ref),
						schema: GitHubApiIssueComment,
						body: { body: input.content.markdown },
					}).pipe(Effect.map((comment) => issueComment(ref.discussion, comment)))
			return value
		})

		const deleteComment = Effect.fn('github.api.delete_comment')(function* (input: GitHubDeleteComment) {
			const ref = input.comment
			yield* callVoid({
				operation: 'delete_comment',
				ref: commentRepository(ref),
				method: 'DELETE',
				path: commentPath(ref),
			})
		})

		const reactionPath = (comment: GitHubCommentRef) => `${commentPath(comment)}/reactions`

		const addReaction = Effect.fn('github.api.add_reaction')(function* (input: GitHubReactionRequest) {
			const ref = commentRepository(input.comment)
			yield* call({
				operation: 'add_reaction',
				ref,
				method: 'POST',
				path: reactionPath(input.comment),
				schema: GitHubApiReaction,
				body: { content: input.reaction },
			})
		})

		const removeReaction = Effect.fn('github.api.remove_reaction')(function* (input: GitHubReactionRequest) {
			const ref = commentRepository(input.comment)
			const ownUserId = yield* botUserId
			const reactionId = yield* list({
				operation: 'remove_reaction',
				ref,
				path: reactionPath(input.comment),
				schema: GitHubApiReaction,
			}).pipe(
				Effect.map((reactions) =>
					Option.fromUndefinedOr(
						reactions.find(
							(reaction) => reaction.content === input.reaction && reaction.user?.id === ownUserId,
						)?.id,
					),
				),
			)
			if (Option.isNone(reactionId)) return
			yield* callVoid({
				operation: 'remove_reaction',
				ref,
				method: 'DELETE',
				path: `${reactionPath(input.comment)}/${reactionId.value}`,
			})
		})

		return GitHubApi.of({
			fetchIssue,
			fetchPullRequest,
			listIssueComments,
			listPullRequestComments,
			listPullRequestReviews,
			listPullRequestReviewComments,
			postIssueComment,
			postPullRequestComment,
			replyToReviewComment,
			updateComment,
			deleteComment,
			addReaction,
			removeReaction,
		})
	}),
)

/** GitHub API implementation with Web Crypto signing and the standard Fetch transport. */
export const GitHubApiLive = GitHubApiLiveBase.pipe(
	Layer.provide(GitHubAppSigner.layerWebCrypto),
	Layer.provide(FetchHttpClient.layer),
)
