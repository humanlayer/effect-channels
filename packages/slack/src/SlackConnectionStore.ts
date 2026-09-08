import { Config, Context, Effect, Schema } from 'effect'

import { SlackConnection, SlackConnectionLookupInput } from './SlackConnection.js'

export const UpsertSlackConnection = Schema.Struct({
	...SlackConnectionLookupInput.fields,
	connection: SlackConnection,
})
export type UpsertSlackConnection = typeof UpsertSlackConnection.Type

export class SlackConnectionStoreError extends Schema.TaggedError<SlackConnectionStoreError>()(
	'SlackConnectionStoreError',
	{ operation: Schema.Literals(['get', 'upsert', 'remove', 'initialize']) },
) {}

/** Authoritative installation storage. Implement atomic upsert and idempotent remove; no cache or SQL rows cross this seam. */
export class SlackConnectionStore extends Context.Service<
	SlackConnectionStore,
	{
		readonly get: (
			input: SlackConnectionLookupInput,
		) => Effect.Effect<SlackConnection | undefined, SlackConnectionStoreError>
		readonly upsert: (input: UpsertSlackConnection) => Effect.Effect<void, SlackConnectionStoreError>
		readonly remove: (input: SlackConnectionLookupInput) => Effect.Effect<void, SlackConnectionStoreError>
	}
>()('slack/SlackConnectionStore') {}

/** Optional bootstrap recipe; it does not acquire storage or write an installation. */
export const connectionFromConfig = (input: { readonly suffix?: string } = {}) => {
	const suffix = input.suffix ?? ''
	return Config.all({
		workspaceId: Config.schema(SlackConnectionLookupInput.fields.workspaceId, `SLACK_TEAM_ID${suffix}`),
		botToken: Config.redacted(`SLACK_BOT_TOKEN${suffix}`),
		botUserId: Config.schema(Schema.NonEmptyString, `SLACK_BOT_USER_ID${suffix}`),
		botId: Config.schema(Schema.NonEmptyString, `SLACK_BOT_ID${suffix}`),
	}).pipe(
		Effect.flatMap(({ workspaceId, ...credentials }) =>
			UpsertSlackConnection.makeEffect({ workspaceId, connection: { credentials } }),
		),
	)
}
