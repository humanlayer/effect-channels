import { NodeRuntime } from '@effect/platform-node'
import { PgClient } from '@effect/sql-pg'
import { SlackState } from '@humanlayer/channels-slack'
import { connections } from '@humanlayer/channels-slack/postgres'
import { Config, Effect, Layer } from 'effect'

import { seedRecordsFromConfig } from '../src/seed-config.ts'

const DatabaseLive = PgClient.layerConfig({ url: Config.redacted('DATABASE_URL') })
const SeedLive = SlackState.layer.pipe(Layer.provide(connections), Layer.provide(DatabaseLive))

const program = Effect.gen(function* () {
	const seeds = yield* seedRecordsFromConfig
	yield* Effect.flatMap(SlackState, (state) => Effect.forEach(seeds, state.upsertConnection, { discard: true })).pipe(
		Effect.provide(SeedLive),
	)
	yield* Effect.logInfo(`Seeded ${seeds.length} Slack connection record(s)`)
})

NodeRuntime.runMain(program)
