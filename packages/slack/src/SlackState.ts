import { Cache, Cause, Context, Data, Duration, Effect, Exit, Layer, Option, Schema } from 'effect'

import { UnknownTenant, UserLookupFailed } from './DomainErrors.ts'
import type { UserProfile } from './Model.ts'
import type { GetUserInput } from './Operations.ts'
import { SlackConnection, SlackConnectionLookupInput, SlackGetUserInput, SlackTeamId } from './Schema.ts'
import { SlackClient } from './SlackClient.ts'
import { SlackConnectionStore, SlackConnectionStoreError, UpsertSlackConnection } from './SlackConnectionStore.ts'
import { SlackCredentialSnapshot } from './SlackCredentialSnapshot.ts'

class UserKey extends Data.Class<GetUserInput & SlackConnection['credentials']> {}
const retryableApiErrors = new Set(['ratelimited', 'internal_error', 'fatal_error', 'service_unavailable'])

const lookup = Effect.fn('slack.users.fetch')(function* (input: UserKey) {
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
			Effect.provideService(SlackCredentialSnapshot, {
				workspaceId: SlackTeamId.make(input.tenant),
				credentials: { botToken: input.botToken, botUserId: input.botUserId, botId: input.botId },
			}),
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

/** Owns installation access and disposable user state. Credentials are read authoritatively, never TTL-cached. */
export class SlackState extends Context.Service<
	SlackState,
	{
		readonly getConnection: (
			input: SlackConnectionLookupInput,
		) => Effect.Effect<SlackConnection | undefined, SlackConnectionStoreError>
		readonly upsertConnection: (input: UpsertSlackConnection) => Effect.Effect<void, SlackConnectionStoreError>
		readonly removeConnection: (input: SlackConnectionLookupInput) => Effect.Effect<void, SlackConnectionStoreError>
		readonly getUser: (
			input: GetUserInput,
		) => Effect.Effect<UserProfile, UnknownTenant | UserLookupFailed, SlackClient>
	}
>()('slack/SlackState') {
	static readonly layer = Layer.effect(
		SlackState,
		Effect.gen(function* () {
			const store = yield* SlackConnectionStore
			const profiles = yield* Cache.makeWith(lookup, {
				requireServicesAt: 'lookup',
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
			const invalidate = (input: SlackConnectionLookupInput) =>
				Cache.keys(profiles).pipe(
					Effect.flatMap((keys) =>
						Effect.forEach(
							[...keys].filter((key) => String(key.tenant) === input.workspaceId),
							(key) => Cache.invalidate(profiles, key),
							{ discard: true },
						),
					),
				)
			return SlackState.of({
				getConnection: Effect.fn('slack.state.get_connection')((input) => store.get(input)),
				upsertConnection: Effect.fn('slack.state.upsert_connection')((input) =>
					store.upsert(input).pipe(Effect.ensuring(invalidate(input))),
				),
				removeConnection: Effect.fn('slack.state.remove_connection')((input) =>
					store.remove(input).pipe(Effect.ensuring(invalidate(input))),
				),
				getUser: Effect.fn('slack.state.get_user')(function* (input) {
					const connection = yield* store.get({ workspaceId: SlackTeamId.make(input.tenant) }).pipe(
						Effect.tapError(() =>
							Effect.logError('Slack user credential lookup failed', { tenant: input.tenant }),
						),
						Effect.mapError(() =>
							UserLookupFailed.make({
								provider: 'slack',
								tenant: input.tenant,
								userId: input.userId,
								reason: 'transport',
								retryable: true,
							}),
						),
					)
					if (connection === undefined)
						return yield* UnknownTenant.make({ provider: 'slack', tenant: input.tenant })
					return yield* Cache.get(profiles, new UserKey({ ...input, ...connection.credentials }))
				}),
			})
		}),
	)
}
