import { assert, it } from '@effect/vitest'
import { ConfigProvider, Effect, Redacted } from 'effect'

import { seedRecordsFromConfig } from '../src/seed-config.js'

const primary = {
	SLACK_TEAM_ID: 'T_ONE',
	SLACK_BOT_TOKEN: 'xoxb-fixture',
	SLACK_BOT_USER_ID: 'U_ONE',
	SLACK_BOT_ID: 'B_ONE',
}

it.effect('reads complete primary and secondary installations without database access', () =>
	Effect.gen(function* () {
		const seeds = yield* seedRecordsFromConfig
		assert.deepStrictEqual(
			seeds.map((seed) => seed.workspaceId),
			['T_ONE', 'T_TWO'],
		)
		const first = seeds.at(0)
		assert.ok(first !== undefined)
		assert.strictEqual(Redacted.value(first.connection.credentials.botToken), 'xoxb-fixture')
	}).pipe(
		Effect.provide(
			ConfigProvider.layer(
				ConfigProvider.fromUnknown({
					...primary,
					SLACK_TEAM_ID_2: 'T_TWO',
					SLACK_BOT_TOKEN_2: 'xoxb-two',
					SLACK_BOT_USER_ID_2: 'U_TWO',
					SLACK_BOT_ID_2: 'B_TWO',
				}),
			),
		),
	),
)

it.effect('allows an absent second installation', () =>
	Effect.gen(function* () {
		assert.strictEqual((yield* seedRecordsFromConfig).length, 1)
	}).pipe(Effect.provide(ConfigProvider.layer(ConfigProvider.fromUnknown(primary)))),
)

it.effect('rejects missing primary and partial secondary configuration before any writes', () =>
	Effect.gen(function* () {
		const missing = yield* seedRecordsFromConfig.pipe(
			Effect.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({}))),
			Effect.flip,
		)
		assert.strictEqual(missing._tag, 'ConfigError')
		const partial = yield* seedRecordsFromConfig.pipe(
			Effect.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({ ...primary, SLACK_TEAM_ID_2: 'T_TWO' }))),
			Effect.flip,
		)
		assert.strictEqual(partial._tag, 'ConfigError')
	}),
)
