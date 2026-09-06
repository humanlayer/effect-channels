import { connectionFromConfig, SlackState } from '@humanlayer/channels-slack'
import { Effect } from 'effect'

export const seed = Effect.gen(function* () {
	const connection = yield* connectionFromConfig()
	const state = yield* SlackState
	yield* state.upsertConnection(connection)
	yield* Effect.logInfo('Seeded Slack connection')
}).pipe(Effect.withSpan('example.storage.seed'))
