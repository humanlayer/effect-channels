import {
	ChannelsProviderUnavailable,
	type ChannelsProvider,
	type DeliveryAdmissionBatch,
	type DeliveryMode,
} from '@humanlayer/channels-delivery-next'
import { Config, Effect, Layer, Predicate, Redacted, Schema } from 'effect'

import { LinearApi } from './LinearApi'
import { LinearApiLiveOptions, makeLinearApiLive } from './LinearApiLive'
import type { ClientCredentials } from './LinearAuth'
import { LinearCallbacks, type LinearCallbackHandlers } from './LinearCallbacks'
import { makeLinearEventProcessor } from './LinearEventProcessor'
import { LinearOrganizationId, LinearUserId } from './LinearIdentity'
import { makeLinearWebhookProvider } from './LinearWebhookProvider'

export const LinearBotConfiguration = Schema.Struct({
	organizationId: LinearOrganizationId,
	appUserId: LinearUserId,
})
export interface LinearBotConfiguration extends Schema.Schema.Type<typeof LinearBotConfiguration> {}

export type MakeOptions<E, R, ApiError, ApiRequirements> = {
	readonly webhookSecret: Config.Config<Redacted.Redacted<string>>
	readonly deliveryMode: DeliveryMode
	readonly bot: LinearBotConfiguration | Config.Config<LinearBotConfiguration>
	readonly auth: ClientCredentials
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
	const botConfig = Schema.is(LinearBotConfiguration)(options.bot)
		? Config.succeed(options.bot)
		: options.bot
	const defaultApi = makeLinearApiLive(LinearApiLiveOptions.make({
		auth: options.auth,
		organizationId: botConfig.pipe(Config.map((bot) => bot.organizationId)),
		appUserId: botConfig.pipe(Config.map((bot) => bot.appUserId)),
	}))
	const buildLinearApi = Predicate.isUndefined(options.linearApi)
		? unavailable('build_linear_api', Layer.build(defaultApi))
		: unavailable('build_linear_api', Layer.build(options.linearApi))

	return {
		providerName: 'linear',
		deliveryMode: options.deliveryMode,
		webhookProvider: ({ namespace }) =>
			Effect.gen(function* () {
				const webhookSecret = yield* unavailable('read_webhook_secret', options.webhookSecret)
				const bot = yield* readBotConfiguration
				yield* buildLinearApi
				return makeLinearWebhookProvider({
					namespace,
					webhookSecret,
					organizationId: bot.organizationId,
					...(options.maxBodyBytes === undefined ? {} : { maxBodyBytes: options.maxBodyBytes }),
					...(options.maxTimestampAgeMs === undefined ? {} : { maxTimestampAgeMs: options.maxTimestampAgeMs }),
				})
			}).pipe(Effect.withSpan('linear.bot.build_webhook_provider')),
		eventProcessor: ({ namespace }) =>
			Effect.gen(function* () {
				yield* readBotConfiguration
				const linearApi = yield* buildLinearApi
				const processor = makeLinearEventProcessor({ namespace })
				return {
					namespace: processor.namespace,
					providerName: processor.providerName,
					process: (admissions: DeliveryAdmissionBatch) =>
						processor.process(admissions).pipe(Effect.provide(callbacks), Effect.provide(linearApi)),
				}
			}).pipe(Effect.withSpan('linear.bot.build_event_processor')),
	}
}
