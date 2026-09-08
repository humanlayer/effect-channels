import { assert, it } from '@effect/vitest'
import { ConfigProvider, Context, Effect, Layer, Redacted } from 'effect'
import { FetchHttpClient } from 'effect/unstable/http'

import { GitHubCredentials, GitHubCrypto } from '../src/index.js'

it.effect('config parses environment strings without HTTP or signing on acquisition', () =>
	Effect.gen(function* () {
		const context = yield* Layer.build(
			GitHubCredentials.layerConfig.pipe(
				Layer.provide(Layer.mock(GitHubCrypto, {})),
				Layer.provide(FetchHttpClient.layer),
				Layer.provide(
					ConfigProvider.layer(
						ConfigProvider.fromUnknown({
							GITHUB_APP_ID: '42',
							GITHUB_PRIVATE_KEY: 'unused-at-acquisition',
							GITHUB_INSTALLATION_ID: '100',
							GITHUB_BOT_USER_ID: '99',
						}),
					),
				),
			),
		)
		const credentials = Context.get(context, GitHubCredentials)
		assert.equal(credentials.apiUrl, 'https://api.github.com')
		assert.equal(credentials.botUserId, 99)
		assert.ok(credentials.acceptsInstallation({ installationId: 100 }))
	}),
)

for (const apiUrl of [
	'http://untrusted.example',
	'https://token@api.github.com',
	'https://api.github.com?secret=x',
	'not a url',
]) {
	it.effect(`rejects unsafe configured API origin ${apiUrl}`, () =>
		Effect.gen(function* () {
			const failure = yield* Layer.build(
				GitHubCredentials.layer({
					appId: 42,
					privateKey: Redacted.make('never-log-key'),
					installationIds: [100],
					botUserId: 99,
					apiUrl,
				}).pipe(Layer.provide(Layer.mock(GitHubCrypto, {})), Layer.provide(FetchHttpClient.layer)),
			).pipe(Effect.flip)
			assert.equal(failure.reason, 'configuration')
		}),
	)
}
