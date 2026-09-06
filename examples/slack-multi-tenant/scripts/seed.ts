import { NodeRuntime } from '@effect/platform-node'
import { PgClient } from '@effect/sql-pg'
import { Config, Effect, Layer } from 'effect'

import { seedRecordsFromConfig } from '../src/seed-config.ts'
import { seedSlackConnection, SlackConnectionRepositoryLive } from '../src/store.ts'

const DatabaseLive = PgClient.layerConfig({ url: Config.redacted('DATABASE_URL') })
const SeedLive = SlackConnectionRepositoryLive.pipe(Layer.provideMerge(DatabaseLive))

const program = Effect.gen(function* () {
	const seeds = yield* seedRecordsFromConfig
	yield* Effect.forEach(seeds, seedSlackConnection, { discard: true }).pipe(Effect.provide(SeedLive))
	yield* Effect.logInfo(`Seeded ${seeds.length} Slack connection record(s)`)
})

NodeRuntime.runMain(program)
