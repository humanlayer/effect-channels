import { assert, it } from '@effect/vitest'
import { SlackConnectionStore, SlackState, SlackTeamId } from '@humanlayer/channels-slack'
import { connections } from '@humanlayer/channels-slack/memory'
import { ConfigProvider, Effect, Layer, Logger, Redacted, Ref, Result } from 'effect'

import { seed } from '../src/seed.ts'

const workspaceId = SlackTeamId.make('T_SEED')
const config = {
	SLACK_TEAM_ID: workspaceId,
	SLACK_BOT_TOKEN: 'xoxb-seed-secret',
	SLACK_BOT_USER_ID: 'U_SEED',
	SLACK_BOT_ID: 'B_SEED',
}
const configuredSeed = (values: Record<string, string>) =>
	seed.pipe(Effect.provide(ConfigProvider.layer(ConfigProvider.fromUnknown(values))))

const makeStorage = Effect.gen(function* () {
	const writes = yield* Ref.make(0)
	const store = Layer.effect(
		SlackConnectionStore,
		Effect.gen(function* () {
			const memory = yield* SlackConnectionStore
			return SlackConnectionStore.of({
				get: memory.get,
				remove: memory.remove,
				upsert: (input) => Ref.update(writes, (count) => count + 1).pipe(Effect.andThen(memory.upsert(input))),
			})
		}),
	).pipe(Layer.provide(connections({ capacity: 1 })))
	return { writes, layer: SlackState.layer.pipe(Layer.provideMerge(store)) }
})

it.effect('seed upserts through SlackState and replaces the installation without logging credentials', () =>
	Effect.gen(function* () {
		const storage = yield* makeStorage
		const logs: Array<string> = []
		yield* Effect.gen(function* () {
			const store = yield* SlackConnectionStore
			yield* configuredSeed(config)
			const first = yield* store.get({ workspaceId })
			assert.ok(first !== undefined)
			assert.ok(Redacted.value(first.credentials.botToken) === config.SLACK_BOT_TOKEN)
			assert.deepStrictEqual(first.credentials, {
				botToken: Redacted.make(config.SLACK_BOT_TOKEN),
				botUserId: 'U_SEED',
				botId: 'B_SEED',
			})
			yield* configuredSeed({
				...config,
				SLACK_BOT_TOKEN: 'xoxb-replacement-secret',
				SLACK_BOT_USER_ID: 'U_REPLACED',
				SLACK_BOT_ID: 'B_REPLACED',
			})
			const replacement = yield* store.get({ workspaceId })
			assert.ok(replacement !== undefined)
			assert.ok(Redacted.value(replacement.credentials.botToken) === 'xoxb-replacement-secret')
			assert.deepStrictEqual(replacement.credentials, {
				botToken: Redacted.make('xoxb-replacement-secret'),
				botUserId: 'U_REPLACED',
				botId: 'B_REPLACED',
			})
			assert.strictEqual(yield* Ref.get(storage.writes), 2)
		}).pipe(
			Effect.provide(
				Layer.merge(
					storage.layer,
					Logger.layer([Logger.make((entry) => logs.push(JSON.stringify(entry.message)))]),
				),
			),
		)
		assert.deepStrictEqual(logs, ['["Seeded Slack connection"]', '["Seeded Slack connection"]'])
	}).pipe(Effect.scoped),
)

it.effect('missing or malformed config fails without writing or replacing an existing installation', () =>
	Effect.gen(function* () {
		const storage = yield* makeStorage
		yield* Effect.gen(function* () {
			const store = yield* SlackConnectionStore
			for (const values of [{}, { ...config, SLACK_BOT_USER_ID: '' }]) {
				const result = yield* configuredSeed(values).pipe(Effect.result)
				assert.ok(Result.isFailure(result))
				assert.strictEqual(result.failure._tag, 'ConfigError')
				assert.strictEqual(yield* store.get({ workspaceId }), undefined)
			}
			assert.strictEqual(yield* Ref.get(storage.writes), 0)
			yield* configuredSeed(config)
			const before = yield* store.get({ workspaceId })
			const result = yield* configuredSeed({ ...config, SLACK_BOT_ID: '' }).pipe(Effect.result)
			assert.ok(Result.isFailure(result))
			assert.strictEqual(result.failure._tag, 'ConfigError')
			assert.deepStrictEqual(yield* store.get({ workspaceId }), before)
			assert.strictEqual(yield* Ref.get(storage.writes), 1)
		}).pipe(Effect.provide(storage.layer))
	}).pipe(Effect.scoped),
)
