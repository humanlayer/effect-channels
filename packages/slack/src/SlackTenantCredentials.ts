import { Cache, Config, Context, Duration, Effect, Exit, Layer, Option, Schema } from 'effect'

import { CredentialStoreError } from './Errors.ts'
import type { SlackLoadCredentialsInput, SlackSaveCredentialsInput, SlackTenantCreds } from './Schema.ts'
import { SlackTenantCreds as SlackTenantCredsSchema } from './Schema.ts'
import { SlackConnection, SlackConnectionLookupInput, SlackTeamId } from './Schema.ts'
import { SlackCredentialSnapshot } from './SlackCredentialSnapshot.ts'
import { SlackState } from './SlackState.ts'

export class SlackTenantCredentials extends Context.Service<
	SlackTenantCredentials,
	{
		readonly load: (
			input: SlackLoadCredentialsInput,
		) => Effect.Effect<Option.Option<SlackTenantCreds>, CredentialStoreError>
		readonly save: (input: SlackSaveCredentialsInput) => Effect.Effect<void, CredentialStoreError>
	}
>()('channels/SlackTenantCredentials') {
	/** The mutable store path deliberately does not cache credentials or missing installations. */
	static readonly layer = Layer.effect(
		SlackTenantCredentials,
		Effect.gen(function* () {
			const state = yield* SlackState
			return SlackTenantCredentials.of({
				load: Effect.fn('slack.credentials.load')(function* (input) {
					const snapshot = yield* Effect.serviceOption(SlackCredentialSnapshot)
					if (Option.isSome(snapshot) && snapshot.value.workspaceId === input.teamId)
						return Option.some(snapshot.value.credentials)
					const connection = yield* state
						.getConnection({ workspaceId: input.teamId })
						.pipe(
							Effect.mapError(() =>
								CredentialStoreError.make({ operation: 'load', teamId: input.teamId }),
							),
						)
					return Option.fromUndefinedOr(connection?.credentials)
				}),
				save: Effect.fn('slack.credentials.save')(function* (input) {
					const connection = yield* Schema.decodeUnknownEffect(Schema.toType(SlackConnection))({
						credentials: input.credentials,
					}).pipe(
						Effect.mapError(() => CredentialStoreError.make({ operation: 'save', teamId: input.teamId })),
					)
					yield* state
						.upsertConnection({ workspaceId: input.teamId, connection })
						.pipe(
							Effect.mapError(() =>
								CredentialStoreError.make({ operation: 'save', teamId: input.teamId }),
							),
						)
				}),
			})
		}),
	)
	static readonly layerWithLookup = <E, R>(options: {
		readonly loadConnection: (input: SlackConnectionLookupInput) => Effect.Effect<unknown, E, R>
	}) =>
		Layer.effect(
			SlackTenantCredentials,
			Effect.gen(function* () {
				const context = yield* Effect.context<R>()
				const cache = yield* Cache.makeWith(
					(workspaceId: SlackTeamId) =>
						options.loadConnection({ workspaceId }).pipe(
							Effect.provide(context),
							Effect.flatMap(Schema.decodeUnknownEffect(Schema.UndefinedOr(SlackConnection))),
							Effect.map(Option.fromUndefinedOr),
							Effect.tapError(() =>
								Effect.logError('Slack connection lookup failed').pipe(
									Effect.annotateLogs({ workspaceId }),
								),
							),
							Effect.mapError(() =>
								CredentialStoreError.make({ operation: 'load', teamId: workspaceId }),
							),
						),
					{
						capacity: 1_000,
						timeToLive: (exit) => (Exit.isSuccess(exit) ? '1 minute' : Duration.zero),
					},
				)
				return SlackTenantCredentials.of({
					load: Effect.fn('slack.credentials.load')(function* (input) {
						return (yield* Cache.get(cache, input.teamId)).pipe(
							Option.map((connection) => SlackTenantCredsSchema.make(connection.credentials)),
						)
					}),
					save: Effect.fn('slack.credentials.save')((input) =>
						Effect.fail(CredentialStoreError.make({ operation: 'save', teamId: input.teamId })),
					),
				})
			}),
		)

	static readonly layerFromConfig = Layer.effect(
		SlackTenantCredentials,
		Effect.map(Config.redacted('SLACK_BOT_TOKEN'), (botToken) => {
			const credentials = SlackTenantCredsSchema.make({ botToken })
			return SlackTenantCredentials.of({
				load: Effect.fn('slack.credentials.load')(() => Effect.succeed(Option.some(credentials))),
				save: Effect.fn('slack.credentials.save')((input) =>
					Effect.fail(CredentialStoreError.make({ operation: 'save_not_supported', teamId: input.teamId })),
				),
			})
		}),
	)

	static make(operations: {
		readonly load: (
			input: SlackLoadCredentialsInput,
		) => Effect.Effect<Option.Option<SlackTenantCreds>, CredentialStoreError>
		readonly save: (input: SlackSaveCredentialsInput) => Effect.Effect<void, CredentialStoreError>
	}) {
		return Layer.succeed(SlackTenantCredentials, SlackTenantCredentials.of(operations))
	}
}
