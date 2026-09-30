import {
	ChannelsProviderUnavailable,
	type ChannelsProvider,
	type DeliveryAdmissionBatch,
	type ProviderDeliveryExecution,
	SerialDeliveryMode,
} from '@humanlayer/channels-delivery-next'
import { Config, Effect, Layer, Match, Option, Predicate, Redacted, Schema } from 'effect'

import { LinearApi } from './LinearApi'
import { LinearApiLiveOptions, makeLinearApiLive } from './LinearApiLive'
import type { AuthenticationInput } from './LinearAuth'
import * as LinearAuth from './LinearAuth'
import { LinearCallbacks, type LinearCallbackHandlers } from './LinearCallbacks'
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

const unavailable = <A, E, R>(step: string, effect: Effect.Effect<A, E, R>) =>
	effect.pipe(
		Effect.tapError((error) =>
			Effect.logError('Linear bot could not be built', error).pipe(Effect.annotateLogs({ step })),
		),
		Effect.mapError(() => ChannelsProviderUnavailable.make({ provider: 'linear' })),
	)

export const make = <E, R, ApiError = never, ApiRequirements = never>(
	options: MakeOptions<E, R, ApiError, ApiRequirements>,
): ChannelsProvider<{ readonly build: ApiRequirements; readonly process: Exclude<R, LinearApi> }> => {
	const callbacks = LinearCallbacks.layer(options.handlers)
	const readBotConfiguration = Schema.is(LinearBotConfiguration)(options.bot)
		? Effect.succeed(options.bot)
		: unavailable('read_bot_configuration', options.bot)
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
	const buildLinearApi = Predicate.isUndefined(options.linearApi)
		? unavailable('build_linear_api', Layer.build(defaultApi))
		: unavailable('build_linear_api', Layer.build(options.linearApi))
	/** Builds the API layer so the host discovers its configuration; the live layer makes no requests until an operation runs. */
	const discoverLinearApiConfiguration = Effect.asVoid(buildLinearApi)

	return {
		providerName: 'linear',
		/** Agent Session events must begin processing immediately and one at a time so the automatic acknowledgement can satisfy Linear's ten-second deadline. */
		deliveryMode: SerialDeliveryMode.make({}),
		webhookProvider: Effect.fn('linear.bot.build_webhook_provider')(function* ({ namespace }) {
			const webhookSecret = yield* unavailable('read_webhook_secret', options.webhookSecret)
			const bot = yield* readBotConfiguration
			const oauthClientId = yield* unavailable('read_client_id', readOauthClientId)
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
			const oauthClientId = yield* unavailable('read_client_id', readOauthClientId)
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
	}
}
