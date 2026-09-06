import { Config, Effect, Option, Schema } from 'effect'

import { SlackConnectionSeed } from './store.ts'

const record = (suffix: '' | '_2') =>
	Config.all({
		workspaceId: Config.schema(Schema.NonEmptyString, `SLACK_TEAM_ID${suffix}`),
		botToken: Config.redacted(`SLACK_BOT_TOKEN${suffix}`),
		botUserId: Config.schema(Schema.NonEmptyString, `SLACK_BOT_USER_ID${suffix}`),
		botId: Config.schema(Schema.NonEmptyString, `SLACK_BOT_ID${suffix}`),
	}).pipe(Effect.flatMap((input) => SlackConnectionSeed.makeEffect(input)))

export const seedRecordsFromConfig = Effect.gen(function* () {
	const primary = yield* record('')
	const optionalFields = yield* Config.all(
		['SLACK_TEAM_ID_2', 'SLACK_BOT_TOKEN_2', 'SLACK_BOT_USER_ID_2', 'SLACK_BOT_ID_2'].map((name) =>
			Config.option(Config.redacted(name)),
		),
	)
	if (optionalFields.every(Option.isNone)) return [primary]
	const secondary = yield* record('_2')
	return [primary, secondary]
}).pipe(Effect.withSpan('example.slack.credentials.seed_config'))
