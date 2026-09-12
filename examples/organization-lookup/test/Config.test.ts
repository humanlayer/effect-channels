import { assert, it } from '@effect/vitest'
import { Cause, ConfigProvider, Effect, Exit, Layer } from 'effect'

import { makeHost } from '../src/fetch.js'
import { server } from '../src/server.js'

it.effect('server fails with typed configuration errors rather than defaulting credentials', () =>
	Effect.gen(function* () {
		const exit = yield* Layer.build(server).pipe(
			Effect.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({ PORT: 0 }))),
			Effect.exit,
		)
		assert.ok(Exit.isFailure(exit))
		assert.ok(Cause.hasFails(exit.cause))
		assert.isFalse(Cause.hasDies(exit.cause))
	}),
)

it.effect('malformed server port remains a typed startup failure; Fetch construction is lazy and disposable', () =>
	Effect.gen(function* () {
		const exit = yield* Layer.build(server).pipe(
			Effect.provide(
				ConfigProvider.layer(
					ConfigProvider.fromUnknown({
						PORT: 'invalid-port',
						SLACK_TEAM_ID: 'T_NORTH',
						SLACK_BOT_TOKEN: 'test-only-token',
						SLACK_BOT_USER_ID: 'U_TEST',
						SLACK_BOT_ID: 'B_TEST',
						SLACK_SIGNING_SECRET: 'test-only-signing-secret',
						GITHUB_APP_ID: 42,
						GITHUB_PRIVATE_KEY: 'test-only-unused-private-key',
						GITHUB_INSTALLATION_ID: 100,
						GITHUB_BOT_USER_ID: 999999,
						GITHUB_BOT_LOGIN: 'channels[bot]',
						GITHUB_WEBHOOK_SECRET: 'test-only-webhook-secret',
					}),
				),
			),
			Effect.exit,
		)
		assert.ok(Exit.isFailure(exit))
		assert.ok(Cause.hasFails(exit.cause))
		assert.isFalse(Cause.hasDies(exit.cause))
		assert.include(Cause.pretty(exit.cause), 'PORT')
		const host = makeHost()
		yield* Effect.promise(host.dispose)
	}),
)
