import { connectionFromConfig } from '@humanlayer/channels-slack'
import { Config, Effect, Option } from 'effect'

export const seedRecordsFromConfig = Effect.gen(function* () {
	const primary = yield* connectionFromConfig()
	const optionalFields = yield* Config.all(
		['SLACK_TEAM_ID_2', 'SLACK_BOT_TOKEN_2', 'SLACK_BOT_USER_ID_2', 'SLACK_BOT_ID_2'].map((name) =>
			Config.option(Config.redacted(name)),
		),
	)
	if (optionalFields.every(Option.isNone)) return [primary]
	const secondary = yield* connectionFromConfig({ suffix: '_2' })
	return [primary, secondary]
}).pipe(Effect.withSpan('example.slack.credentials.seed_config'))
