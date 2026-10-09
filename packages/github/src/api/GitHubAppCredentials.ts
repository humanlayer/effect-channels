/**
 * The GitHub App's credentials: its signed JWT, and the installation tokens it exchanges that JWT for.
 * GitHub's REST requests and authenticated git operations share one token cache, so both use the same
 * token for a repository until shortly before it expires.
 */
import { Cache, Clock, Config, Context, Duration, Effect, Exit, Layer, Redacted, Schema } from 'effect'
import { Base64Url } from 'effect/encoding'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientRequest from 'effect/http/HttpClientRequest'

import { GitHubId } from '../GitHubIdentity'
import type { GitHubRepositoryRef } from '../GitHubModels'
import { GitHubTransportError } from './GitHubApiErrors'
import { GitHubAppSigner } from './GitHubAppSigner'
import { gitHubRequest, inspectGitHubStatus } from './GitHubHttp'

const GitHubAppCredentialsConfig = Config.all({
	appId: Config.schema(GitHubId, 'GITHUB_APP_ID'),
	privateKey: Config.Redacted('GITHUB_PRIVATE_KEY'),
	apiOrigin: Config.URL('GITHUB_API_ORIGIN').pipe(Config.withDefault(new URL('https://api.github.com/'))),
})

const InstallationTokenResponse = Schema.Struct({ token: Schema.NonEmptyString, expires_at: Schema.String })
const InstallationTokenRequestBody = Schema.Struct({ repository_ids: Schema.Array(GitHubId) })
const AppJwtHeaderJson = Schema.fromJsonString(
	Schema.Struct({ alg: Schema.Literal('RS256'), typ: Schema.Literal('JWT') }),
)
const AppJwtClaimsJson = Schema.fromJsonString(Schema.Struct({ iss: Schema.String, iat: Schema.Int, exp: Schema.Int }))

/** A token is used until a minute before it expires, and for at most 59 minutes. */
const TOKEN_EXPIRY_MARGIN_MILLIS = 60_000
const TOKEN_MAX_TTL_MILLIS = 3_540_000

const cacheKey = (ref: GitHubRepositoryRef) => JSON.stringify([ref.installationId, ref.repositoryId])

export class GitHubAppCredentials extends Context.Service<
	GitHubAppCredentials,
	{
		/** A JWT signed with the App's private key, for the App's own endpoints. */
		readonly appJwt: Effect.Effect<string, GitHubTransportError>
		/** An installation token scoped to one repository, cached until shortly before it expires. */
		readonly installationToken: (
			repository: GitHubRepositoryRef,
		) => Effect.Effect<Redacted.Redacted<string>, GitHubTransportError>
		/** Forget the repository's cached token, after GitHub refused it. */
		readonly invalidateInstallationToken: (repository: GitHubRepositoryRef) => Effect.Effect<void>
	}
>()('@humanlayer/channels-github/internal/GitHubAppCredentials') {}

export const GitHubAppCredentialsLive = Layer.effect(
	GitHubAppCredentials,
	Effect.gen(function* () {
		const client = yield* HttpClient.HttpClient
		const signer = yield* GitHubAppSigner
		const config = yield* GitHubAppCredentialsConfig
		const apiOrigin = new URL(config.apiOrigin.toString().replace(/\/?$/, '/'))

		const appJwt = Effect.gen(function* () {
			const now = yield* Clock.currentTimeMillis
			const header = yield* Schema.encodeEffect(AppJwtHeaderJson)({ alg: 'RS256', typ: 'JWT' })
			const claims = yield* Schema.encodeEffect(AppJwtClaimsJson)({
				iss: String(config.appId),
				iat: Math.floor(now / 1_000) - 60,
				exp: Math.floor(now / 1_000) + 540,
			})
			const unsigned = `${Base64Url.encode(header)}.${Base64Url.encode(claims)}`
			const signature = yield* signer.sign({ privateKey: config.privateKey, data: unsigned }).pipe(
				Effect.tapError((error) => Effect.logError('GitHub App JWT signing failed', error)),
				Effect.mapError(() => GitHubTransportError.make({ stage: 'signing' })),
			)
			return `${unsigned}.${signature}`
		}).pipe(
			Effect.catchTag('SchemaError', () => Effect.fail(GitHubTransportError.make({ stage: 'signing' }))),
			Effect.withSpan('github.api.app_jwt'),
		)

		const tokenCache = yield* Cache.makeWith(
			(key: string) =>
				Effect.gen(function* () {
					const [installationId, repositoryId] = yield* Schema.decodeEffect(
						Schema.fromJsonString(Schema.Tuple([GitHubId, GitHubId])),
					)(key).pipe(Effect.mapError(() => GitHubTransportError.make({ stage: 'decode' })))
					const jwt = yield* appJwt
					const request = yield* gitHubRequest(
						'POST',
						new URL(`app/installations/${installationId}/access_tokens`, apiOrigin).toString(),
					).pipe(
						HttpClientRequest.bearerToken(jwt),
						HttpClientRequest.schemaBodyJson(InstallationTokenRequestBody)({
							repository_ids: [repositoryId],
						}),
						Effect.mapError(() => GitHubTransportError.make({ stage: 'decode' })),
					)
					const response = yield* client.execute(request).pipe(
						Effect.mapError(() => GitHubTransportError.make({ stage: 'transport' })),
						Effect.flatMap(inspectGitHubStatus),
						Effect.flatMap((value) =>
							value.json.pipe(
								Effect.flatMap(Schema.decodeUnknownEffect(InstallationTokenResponse)),
								Effect.mapError(() => GitHubTransportError.make({ stage: 'decode' })),
							),
						),
					)
					const expiresAt = Date.parse(response.expires_at)
					const receivedAt = yield* Clock.currentTimeMillis
					if (!Number.isFinite(expiresAt) || expiresAt <= receivedAt + TOKEN_EXPIRY_MARGIN_MILLIS)
						return yield* GitHubTransportError.make({ stage: 'token_expiry' })
					return {
						token: Redacted.make(response.token),
						ttl: Math.min(expiresAt - receivedAt - TOKEN_EXPIRY_MARGIN_MILLIS, TOKEN_MAX_TTL_MILLIS),
					}
				}).pipe(Effect.tapError((error) => Effect.logError('GitHub installation token request failed', error))),
			{
				capacity: 1_000,
				timeToLive: (exit) => (Exit.isSuccess(exit) ? Duration.millis(exit.value.ttl) : Duration.zero),
			},
		)

		return GitHubAppCredentials.of({
			appJwt,
			installationToken: (repository) =>
				Cache.get(tokenCache, cacheKey(repository)).pipe(
					Effect.map(({ token }) => token),
					Effect.withSpan('github.api.installation_token'),
				),
			invalidateInstallationToken: (repository) => Cache.invalidate(tokenCache, cacheKey(repository)),
		})
	}),
)
