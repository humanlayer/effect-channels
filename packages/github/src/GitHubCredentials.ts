import {
	Cache,
	Clock,
	Config,
	Context,
	DateTime,
	Duration,
	Effect,
	Encoding,
	Exit,
	Layer,
	Option,
	Redacted,
	Schema,
} from 'effect'
import { HttpClient, HttpClientRequest } from 'effect/unstable/http'

import { GitHubCrypto } from './GitHubCrypto'
import { GitHubError } from './GitHubErrors'
import { apiRequest, requestJson } from './GitHubHttp'
import { GitHubId, type GitHubRepository } from './GitHubResource'

export const GitHubAppOptions = Schema.Struct({
	appId: GitHubId,
	privateKey: Schema.Redacted(Schema.NonEmptyString),
	installationIds: Schema.NonEmptyArray(GitHubId),
	botUserId: GitHubId,
	apiUrl: Schema.String,
})
export interface GitHubAppOptions extends Schema.Schema.Type<typeof GitHubAppOptions> {}
const Token = Schema.Struct({ token: Schema.NonEmptyString, expires_at: Schema.String })

const JwtHeader = Schema.fromJsonString(Schema.Struct({ alg: Schema.Literal('RS256'), typ: Schema.Literal('JWT') }))
const JwtClaims = Schema.fromJsonString(Schema.Struct({ iss: Schema.String, iat: Schema.Int, exp: Schema.Int }))
type CacheKey = readonly [installationId: number, repositoryId: number]

class GitHubTokenFailure extends Schema.TaggedError<GitHubTokenFailure>()('GitHubTokenFailure', {
	stage: Schema.Literal('expiry'),
}) {}

export class GitHubCredentials extends Context.Service<
	GitHubCredentials,
	{
		readonly apiUrl: string
		readonly botUserId: number
		readonly acceptsInstallation: (input: { readonly installationId: number }) => boolean
		readonly token: (input: GitHubRepository) => Effect.Effect<Redacted.Redacted<string>, GitHubError>
		readonly invalidate: (input: GitHubRepository) => Effect.Effect<void>
	}
>()('github/GitHubCredentials') {
	static readonly layer = (options: GitHubAppOptions) =>
		Layer.effect(
			GitHubCredentials,
			Effect.gen(function* () {
				const config = yield* GitHubAppOptions.makeEffect(options).pipe(
					Effect.mapError(() => GitHubError.make({ reason: 'configuration' })),
				)
				const url = yield* Effect.try({
					try: () => new URL(config.apiUrl),
					catch: () => GitHubError.make({ reason: 'configuration' }),
				})
				if (
					url.username !== '' ||
					url.password !== '' ||
					url.search !== '' ||
					url.hash !== '' ||
					(url.protocol !== 'https:' &&
						!(url.protocol === 'http:' && ['127.0.0.1', 'localhost', '[::1]'].includes(url.hostname)))
				)
					return yield* GitHubError.make({ reason: 'configuration' })
				const apiUrl = url.toString().replace(/\/$/, '')
				const http = yield* HttpClient.HttpClient
				const crypto = yield* GitHubCrypto
				const installations = new Set(config.installationIds)
				const cache = yield* Cache.makeWith(
					([installationId, repositoryId]: CacheKey) =>
						Effect.gen(function* () {
							const now = yield* Clock.currentTimeMillis
							const [header, payload] = yield* Effect.all([
								Schema.encodeEffect(JwtHeader)({ alg: 'RS256', typ: 'JWT' }),
								Schema.encodeEffect(JwtClaims)({
									iss: String(config.appId),
									iat: Math.floor(now / 1000) - 60,
									exp: Math.floor(now / 1000) + 540,
								}),
							]).pipe(Effect.mapError(() => GitHubError.make({ reason: 'configuration' })))
							const data = `${Encoding.encodeBase64Url(header)}.${Encoding.encodeBase64Url(payload)}`
							const signature = yield* crypto.signApp({ privateKey: config.privateKey, data })
							const request = yield* apiRequest({
								baseUrl: apiUrl,
								path: `/app/installations/${installationId}/access_tokens`,
								method: 'POST',
							}).pipe(
								HttpClientRequest.bearerToken(`${data}.${signature}`),
								HttpClientRequest.bodyJson({
									repository_ids: [repositoryId],
									permissions: { issues: 'write' },
								}),
								Effect.mapError(() => GitHubError.make({ reason: 'invalid_input' })),
							)
							const token = yield* requestJson(request, Token).pipe(
								Effect.provideService(HttpClient.HttpClient, http),
							)
							const expiresAt = Option.map(DateTime.make(token.expires_at), DateTime.toEpochMillis)
							const receivedAt = yield* Clock.currentTimeMillis
							if (Option.isNone(expiresAt) || expiresAt.value <= receivedAt + 60_000)
								return yield* GitHubTokenFailure.make({ stage: 'expiry' })
							return {
								token: Redacted.make(token.token),
								ttl: Math.min(expiresAt.value - receivedAt - 60_000, 3_540_000),
							}
						}).pipe(
							Effect.tapErrorTag('GitHubTokenFailure', (error) =>
								Effect.logError('GitHub token rejected', error),
							),
							Effect.catchTag('GitHubTokenFailure', () =>
								Effect.fail(GitHubError.make({ reason: 'response' })),
							),
						),
					{
						capacity: 1_000,
						timeToLive: (exit) => (Exit.isSuccess(exit) ? Duration.millis(exit.value.ttl) : Duration.zero),
					},
				)
				const key = (input: GitHubRepository): CacheKey => [input.installationId, input.id]
				return GitHubCredentials.of({
					apiUrl,
					botUserId: config.botUserId,
					acceptsInstallation: (input) => installations.has(input.installationId),
					token: Effect.fn('github.credentials.token')((input) =>
						installations.has(input.installationId)
							? Cache.get(cache, key(input)).pipe(Effect.map((entry) => entry.token))
							: Effect.fail(GitHubError.make({ reason: 'authentication' })),
					),
					invalidate: Effect.fn('github.credentials.invalidate')((input) =>
						Cache.invalidate(cache, key(input)),
					),
				})
			}),
		)

	static readonly layerConfig = Layer.unwrap(
		Effect.gen(function* () {
			const appId = yield* Config.schema(GitHubId, 'GITHUB_APP_ID')
			const privateKey = yield* Config.redacted('GITHUB_PRIVATE_KEY')
			const installationId = yield* Config.schema(GitHubId, 'GITHUB_INSTALLATION_ID')
			const botUserId = yield* Config.schema(GitHubId, 'GITHUB_BOT_USER_ID')
			const apiUrl = yield* Config.string('GITHUB_API_URL').pipe(Config.withDefault('https://api.github.com'))
			return GitHubCredentials.layer({ appId, privateKey, installationIds: [installationId], botUserId, apiUrl })
		}),
	)
}
