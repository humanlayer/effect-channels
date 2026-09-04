import { NodeRuntime } from '@effect/platform-node'
import { PgClient } from '@effect/sql-pg'
import { Config, Effect, Layer, Option } from 'effect'

import { seedSlackConnection, SlackConnectionRepositoryLive, SlackConnectionSeed } from '../src/store.ts'

const seedFromConfig = (suffix: '' | '_2') =>
	Config.all({
		workspaceId: Config.option(Config.string(`SLACK_TEAM_ID${suffix}`)),
		organizationId: Config.string(`CHANNELS_ORGANIZATION_ID${suffix}`).pipe(
			Config.withDefault(suffix === '' ? 'local-org' : 'local-org-2'),
		),
		botToken: Config.option(Config.redacted(`SLACK_BOT_TOKEN${suffix}`)),
		botUserId: Config.option(Config.string(`SLACK_BOT_USER_ID${suffix}`)),
		botId: Config.option(Config.string(`SLACK_BOT_ID${suffix}`)),
		enabled: Config.boolean(`SLACK_CONNECTION_ENABLED${suffix}`).pipe(Config.withDefault(true)),
	}).pipe(
		Config.map((config) => {
			if (
				Option.isNone(config.workspaceId) ||
				Option.isNone(config.botToken) ||
				Option.isNone(config.botUserId) ||
				Option.isNone(config.botId)
			) {
				return Option.none<SlackConnectionSeed>()
			}
			return Option.some(
				SlackConnectionSeed.make({
					workspaceId: config.workspaceId.value,
					organizationId: config.organizationId,
					enabled: config.enabled,
					botToken: config.botToken.value,
					botUserId: config.botUserId.value,
					botId: config.botId.value,
				}),
			)
		}),
	)

const program = Effect.gen(function* () {
	const seeds = [yield* seedFromConfig(''), yield* seedFromConfig('_2')].flatMap(Option.toArray)
	if (seeds.length === 0) {
		return yield* Effect.logWarning('No complete Slack connection environment record was provided')
	}
	yield* Effect.forEach(seeds, seedSlackConnection, { discard: true })
	yield* Effect.logInfo(`Seeded ${seeds.length} Slack connection record(s)`)
})

const DatabaseLive = PgClient.layerConfig({ url: Config.redacted('DATABASE_URL') })
const SeedLive = SlackConnectionRepositoryLive.pipe(Layer.provideMerge(DatabaseLive))

NodeRuntime.runMain(program.pipe(Effect.provide(SeedLive)))
