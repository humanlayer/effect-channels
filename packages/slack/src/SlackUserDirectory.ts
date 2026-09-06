import { Cache, Cause, Context, Data, Duration, Effect, Exit, Layer, Option, Schema } from 'effect'

import { UnknownTenant, UserLookupFailed } from './DomainErrors.ts'
import type { UserProfile } from './Model.ts'
import type { GetUserInput } from './Operations.ts'
import { SlackGetUserInput, SlackTeamId } from './Schema.ts'
import { SlackClient } from './SlackClient.ts'

class UserKey extends Data.Class<GetUserInput> {}
const retryableApiErrors = new Set(['ratelimited', 'internal_error', 'fatal_error', 'service_unavailable'])

const lookup = Effect.fn('slack.users.fetch')(function* (input: GetUserInput) {
	const client = yield* SlackClient
	const failed = (reason: UserLookupFailed['reason'], retryable: boolean) =>
		Effect.fail(
			UserLookupFailed.make({
				provider: 'slack',
				tenant: input.tenant,
				userId: input.userId,
				reason,
				retryable,
			}),
		)
	return yield* client
		.getUser(SlackGetUserInput.make({ teamId: SlackTeamId.make(input.tenant), userId: input.userId }))
		.pipe(
			Effect.tapError((error) =>
				Effect.logWarning('Slack user lookup failed', error).pipe(
					Effect.annotateLogs({ tenant: input.tenant, user_id: input.userId }),
				),
			),
			Effect.catchTags({
				SlackTransportError: () => failed('transport', true),
				SlackApiError: (error) =>
					error.code === 'user_not_found'
						? failed('not_found', false)
						: failed('api', retryableApiErrors.has(error.code)),
			}),
		)
})

/** Workspace-scoped, single-flight user lookup. Cache misses fetch and remember the profile. */
export class SlackUserDirectory extends Context.Service<
	SlackUserDirectory,
	{
		readonly getUser: (input: GetUserInput) => Effect.Effect<UserProfile, UnknownTenant | UserLookupFailed>
	}
>()('slack/SlackUserDirectory') {
	static readonly layer = Layer.effect(
		SlackUserDirectory,
		Effect.gen(function* () {
			const profiles = yield* Cache.makeWith((key: UserKey) => lookup(key), {
				capacity: 1_000,
				timeToLive: (exit) => {
					if (Exit.isSuccess(exit)) return '5 minutes'
					const error = Cause.findErrorOption(exit.cause)
					return Option.isSome(error) &&
						Schema.is(UserLookupFailed)(error.value) &&
						error.value.reason === 'not_found'
						? '1 minute'
						: Duration.zero
				},
			})
			return SlackUserDirectory.of({
				getUser: Effect.fn('slack.users.get')((input: GetUserInput) => Cache.get(profiles, new UserKey(input))),
			})
		}),
	)
}
