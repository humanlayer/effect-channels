/**
 * This file defines `SlackBot.make`: the Slack provider as `Channels.make` takes it.
 */
import {
	type ChannelsProvider,
	type DeliveryMode,
	type DeliveryAdmissionBatch,
	type ProviderDeliveryExecution,
	type RawWebhookInput,
} from '@humanlayer/channels-delivery'
import { Effect, Layer, Predicate } from 'effect'
import type { Config, Redacted } from 'effect'

import { SlackApi } from './SlackApi'
import { SlackApiLive } from './SlackApiLive'
import { SlackCallbacks, type SlackCallbackHandlers } from './SlackCallbacks'
import { makeSlackOutputProcessor } from './SlackDeliveryOutput'
import { makeSlackEventProcessor } from './SlackEventProcessor'
import { makeSlackWebhookProvider } from './SlackWebhookProvider'

/**
 * @property signingSecret - read when the bot is built, so the bot itself can be declared at the top of a module
 * @property deliveryMode - when a thread's mailbox runs and what each batch holds
 * @property handlers - the callbacks; the bot supplies `SlackApi` to them, the storage supplies
 * `MailboxSubscriptions`, and anything else they need comes from the application
 * @property slackApi - how the bot talks to Slack; defaults to `SlackApiLive`, which reads one bot token
 * from the configuration
 */
export type MakeOptions<E, R, ApiError, ApiRequirements> = {
	readonly signingSecret: Config.Config<Redacted.Redacted<string>>
	readonly deliveryMode: DeliveryMode
	readonly handlers: SlackCallbackHandlers<E, R>
	readonly slackApi?: Layer.Layer<SlackApi, ApiError, ApiRequirements>
}

export const make = <E, R, ApiError = never, ApiRequirements = never>(
	options: MakeOptions<E, R, ApiError, ApiRequirements>,
): ChannelsProvider<{
	readonly build: ApiRequirements
	readonly process: Exclude<R, SlackApi>
	readonly error: Config.ConfigError | ApiError
}> => {
	const callbacks = SlackCallbacks.layer(options.handlers)
	const buildSlackApi = Predicate.isUndefined(options.slackApi)
		? Layer.build(SlackApiLive)
		: Layer.build(options.slackApi)

	return {
		providerName: 'slack',
		deliveryMode: options.deliveryMode,
		webhookProvider: Effect.fn('slack.bot.build_webhook_provider')(function* ({ namespace }) {
			const signingSecret = yield* options.signingSecret
			const slackApi = yield* buildSlackApi
			const webhookProvider = makeSlackWebhookProvider({ namespace, signingSecret })
			return {
				providerName: webhookProvider.providerName,
				handle: (input: RawWebhookInput) => webhookProvider.handle(input).pipe(Effect.provide(slackApi)),
			}
		}),
		eventProcessor: Effect.fn('slack.bot.build_event_processor')(function* ({ namespace }) {
			const slackApi = yield* buildSlackApi
			const eventProcessor = makeSlackEventProcessor({ namespace })
			return {
				namespace: eventProcessor.namespace,
				providerName: eventProcessor.providerName,
				/** The callbacks are wrapped per batch because they read the services of the running batch. */
				process: (admissions: DeliveryAdmissionBatch, execution: ProviderDeliveryExecution) =>
					eventProcessor
						.process(admissions, execution)
						.pipe(Effect.provide(callbacks), Effect.provide(slackApi)),
			}
		}),
		outputProcessor: Effect.fn('slack.bot.build_output_processor')(function* ({ namespace }) {
			const slackApi = yield* buildSlackApi
			return yield* makeSlackOutputProcessor({ namespace }).pipe(Effect.provide(slackApi))
		}),
	}
}
