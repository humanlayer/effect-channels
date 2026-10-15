import { type ChannelsProvider, SerialDeliveryMode } from '@humanlayer/channels-delivery'
import { Effect, Match, Option, Schema } from 'effect'
import type { Config, Redacted } from 'effect'

import type { LinearApi } from './LinearApi'
import type { AuthenticationInput } from './LinearAuth'
import * as LinearAuth from './LinearAuth'
import type { LinearCallbackHandlers } from './LinearCallbacks'
import { makeLinearOutputProcessor } from './LinearDeliveryOutput'
import { makeLinearEventProcessor } from './LinearEventProcessor'
import { LinearOrganizationId, LinearUserId } from './LinearIdentity'
import { makeLinearWebhookProvider } from './LinearWebhookProvider'

export const LinearBotConfiguration = Schema.Struct({
	organizationId: LinearOrganizationId,
	appUserId: LinearUserId,
})
export type LinearBotConfiguration = typeof LinearBotConfiguration.Type

/**
 * @property webhookSecret - read when the bot is built, so the bot itself can be declared at the top of a module
 * @property bot - the workspace and app user this bot answers for; webhooks for any other are refused
 * @property auth - the app's credentials. The bot reads only the OAuth client ID from them, to check that
 * webhooks name this app; it does not build its own `LinearApi`
 * @property handlers - the callbacks. The application provides `LinearApi` and anything else they need, and
 * the storage supplies `MailboxSubscriptions`. Usually `LinearApi` is
 * `makeLinearApiLive(LinearApiLiveOptions.make({ auth, organizationId, appUserId }))` with the same `auth` and
 * `bot`, or `LinearApiLive`, read from the environment. Sent output goes through the same `LinearApi`.
 */
export type MakeOptions<E, R> = {
	readonly webhookSecret: Config.Config<Redacted.Redacted<string>>
	readonly bot: LinearBotConfiguration | Config.Config<LinearBotConfiguration>
	readonly auth: AuthenticationInput
	readonly handlers: LinearCallbackHandlers<E, R>
	readonly maxBodyBytes?: number
	readonly maxTimestampAgeMs?: number
}

export const make = <E, R>(
	options: MakeOptions<E, R>,
): ChannelsProvider<{
	readonly build: never
	readonly process: R | LinearApi
	readonly error: Config.ConfigError
}> => {
	const readBotConfiguration = Schema.is(LinearBotConfiguration)(options.bot)
		? Effect.succeed(options.bot)
		: options.bot
	const readOauthClientId = LinearAuth.resolve(options.auth).pipe(
		Effect.flatMap((auth) =>
			Match.value(auth).pipe(
				Match.tagsExhaustive({
					LinearClientCredentials: (credentials) => credentials.clientId.pipe(Effect.asSome),
					LinearDeveloperToken: () => Effect.succeed(Option.none<string>()),
				}),
			),
		),
	)

	return {
		providerName: 'linear',
		/** Agent Session events must begin processing immediately and one at a time so the automatic acknowledgement can satisfy Linear's ten-second deadline. */
		deliveryMode: SerialDeliveryMode.make({}),
		webhookProvider: Effect.fn('linear.bot.build_webhook_provider')(function* ({ namespace }) {
			const webhookSecret = yield* options.webhookSecret
			const bot = yield* readBotConfiguration
			const oauthClientId = yield* readOauthClientId
			return makeLinearWebhookProvider({
				namespace,
				webhookSecret,
				organizationId: bot.organizationId,
				appUserId: bot.appUserId,
				oauthClientId: Option.getOrUndefined(oauthClientId),
				maxBodyBytes: options.maxBodyBytes,
				maxTimestampAgeMs: options.maxTimestampAgeMs,
			})
		}),
		eventProcessor: Effect.fn('linear.bot.build_event_processor')(function* ({ namespace }) {
			const bot = yield* readBotConfiguration
			const oauthClientId = yield* readOauthClientId
			return makeLinearEventProcessor({
				namespace,
				bot,
				oauthClientId: Option.getOrUndefined(oauthClientId),
				handlers: options.handlers,
			})
		}),
		/** Sends a handed-off delivery's output: Agent Activities for a session, comments for an issue. */
		outputProcessor: Effect.fn('linear.bot.build_output_processor')(function* ({ namespace }) {
			const bot = yield* readBotConfiguration
			return yield* makeLinearOutputProcessor({ namespace, bot })
		}),
	}
}
