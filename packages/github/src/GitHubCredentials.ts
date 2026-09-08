import { Cache, Clock, Config, Context, Duration, Effect, Encoding, Exit, Layer, Redacted, Schema } from 'effect'
import { HttpClient, HttpClientRequest } from 'effect/unstable/http'

import { GitHubCrypto } from './GitHubCrypto.js'
import { GitHubError } from './GitHubErrors.js'
import { apiRequest, requestJson } from './GitHubHttp.js'
import { GitHubId, type GitHubRepository } from './GitHubResource.js'

export const GitHubAppOptions = Schema.Struct({
	appId: GitHubId,
	privateKey: Schema.Redacted(Schema.NonEmptyString),
	installationIds: Schema.NonEmptyArray(GitHubId),
	botUserId: GitHubId,
	apiUrl: Schema.String,
})
export interface GitHubAppOptions extends Schema.Schema.Type<typeof GitHubAppOptions> {}
const Token = Schema.Struct({ token: Schema.NonEmptyString, expires_at: Schema.String })

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
					(key: string) =>
						Effect.gen(function* () {
							const [installationId, repositoryId] = yield* Schema.decodeEffect(
								Schema.fromJsonString(Schema.Tuple([GitHubId, GitHubId])),
							)(key).pipe(Effect.mapError(() => GitHubError.make({ reason: 'invalid_input' })))
							const now = yield* Clock.currentTimeMillis
							const header = Encoding.encodeBase64Url(JSON.stringify({ alg: 'RS256', typ: 'JWT' }))
							const payload = Encoding.encodeBase64Url(
								JSON.stringify({
									iss: String(config.appId),
									iat: Math.floor(now / 1000) - 60,
									exp: Math.floor(now / 1000) + 540,
								}),
							)
							const data = `${header}.${payload}`
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
							const expiresAt = Date.parse(token.expires_at)
							const receivedAt = yield* Clock.currentTimeMillis
							if (!Number.isFinite(expiresAt) || expiresAt <= receivedAt + 60_000)
								return yield* GitHubTokenFailure.make({ stage: 'expiry' })
							return {
								token: Redacted.make(token.token),
								ttl: Math.min(expiresAt - receivedAt - 60_000, 3_540_000),
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
				const key = (input: GitHubRepository) => JSON.stringify([input.installationId, input.id])
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
