import {
	type ChannelsProvider,
	type DeliveryAdmissionBatch,
	type ProviderDeliveryExecution,
	SerialDeliveryMode,
} from '@humanlayer/channels-delivery-next'
import { Config, Effect, Layer, Match, Option, Predicate, Redacted, Schema } from 'effect'
import type { Context, Scope } from 'effect'

import { LinearApi } from './LinearApi'
import { LinearApiLiveOptions, makeLinearApiLive } from './LinearApiLive'
import type { AuthenticationInput } from './LinearAuth'
import * as LinearAuth from './LinearAuth'
import { LinearCallbacks, type LinearCallbackHandlers } from './LinearCallbacks'
import { makeLinearOutputProcessor } from './LinearDeliveryOutput'
import { makeLinearEventProcessor } from './LinearEventProcessor'
import { LinearOrganizationId, LinearUserId } from './LinearIdentity'
import { makeLinearWebhookProvider } from './LinearWebhookProvider'

export const LinearBotConfiguration = Schema.Struct({
	organizationId: LinearOrganizationId,
	appUserId: LinearUserId,
})
export type LinearBotConfiguration = typeof LinearBotConfiguration.Type

export type MakeOptions<E, R, ApiError, ApiRequirements> = {
	readonly webhookSecret: Config.Config<Redacted.Redacted<string>>
	readonly bot: LinearBotConfiguration | Config.Config<LinearBotConfiguration>
	readonly auth: AuthenticationInput
	readonly handlers: LinearCallbackHandlers<E, R>
	readonly linearApi?: Layer.Layer<LinearApi, ApiError, ApiRequirements>
	readonly maxBodyBytes?: number
	readonly maxTimestampAgeMs?: number
}

export const make = <E, R, ApiError = never, ApiRequirements = never>(
	options: MakeOptions<E, R, ApiError, ApiRequirements>,
): ChannelsProvider<{
	readonly build: ApiRequirements
	readonly process: Exclude<R, LinearApi>
	readonly error: Config.ConfigError | ApiError
}> => {
	const callbacks = LinearCallbacks.layer(options.handlers)
	const readBotConfiguration = Schema.is(LinearBotConfiguration)(options.bot)
		? Effect.succeed(options.bot)
		: options.bot
	const botConfig = Schema.is(LinearBotConfiguration)(options.bot) ? Config.succeed(options.bot) : options.bot
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
	const defaultApi = makeLinearApiLive(
		LinearApiLiveOptions.make({
			auth: options.auth,
			organizationId: botConfig.pipe(Config.map((bot) => bot.organizationId)),
			appUserId: botConfig.pipe(Config.map((bot) => bot.appUserId)),
		}),
	)
	const buildLinearApi: Effect.Effect<
		Context.Context<LinearApi>,
		Config.ConfigError | ApiError,
		ApiRequirements | Scope.Scope
	> = Predicate.isUndefined(options.linearApi)
		? Layer.build(defaultApi)
		: Layer.build(options.linearApi)
	/** Builds the API layer so the host discovers its configuration; the live layer makes no requests until an operation runs. */
	const discoverLinearApiConfiguration = Effect.asVoid(buildLinearApi)

	return {
		providerName: 'linear',
		/** Agent Session events must begin processing immediately and one at a time so the automatic acknowledgement can satisfy Linear's ten-second deadline. */
		deliveryMode: SerialDeliveryMode.make({}),
		webhookProvider: Effect.fn('linear.bot.build_webhook_provider')(function* ({ namespace }) {
			const webhookSecret = yield* options.webhookSecret
			const bot = yield* readBotConfiguration
			const oauthClientId = yield* readOauthClientId
			yield* discoverLinearApiConfiguration
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
			const linearApi = yield* buildLinearApi
			const processor = makeLinearEventProcessor({
				namespace,
				bot,
				oauthClientId: Option.getOrUndefined(oauthClientId),
			})
			return {
				namespace: processor.namespace,
				providerName: processor.providerName,
				process: (admissions: DeliveryAdmissionBatch, execution: ProviderDeliveryExecution) =>
					processor.process(admissions, execution).pipe(Effect.provide(callbacks), Effect.provide(linearApi)),
			}
		}),
		/** Sends a handed-off delivery's output: Agent Activities for a session, comments for an issue. */
		outputProcessor: Effect.fn('linear.bot.build_output_processor')(function* ({ namespace }) {
			const bot = yield* readBotConfiguration
			const linearApi = yield* buildLinearApi
			return yield* makeLinearOutputProcessor({ namespace, bot }).pipe(Effect.provide(linearApi))
		}),
	}
}
