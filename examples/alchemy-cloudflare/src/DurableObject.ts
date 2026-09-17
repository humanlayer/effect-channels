import {
	MailboxProcessingBackendFromDurableObjectStorage,
	makeDeliverFromDurableObjectStorage,
} from '@humanlayer/channels-alchemy-cloudflare'
import { MailboxProcessing, MailboxProcessingLive } from '@humanlayer/channels-delivery-next'
import * as Cloudflare from 'alchemy/Cloudflare'
import { Clock, Config, Effect, Layer } from 'effect'

import { ProviderEventDispatcherAlchemyCloudflare } from './SlackProvider'

/** The application-owned mailbox Durable Object and its alarm handler. */
export class DeliveryMailbox extends Cloudflare.DurableObject<DeliveryMailbox>()(
	'DeliveryMailbox',
	Effect.gen(function* () {
		const state = yield* Cloudflare.DurableObjectState
		const botToken = yield* Config.redacted('SLACK_BOT_TOKEN').pipe(
			Effect.tapError((error) => Effect.logError('SLACK_BOT_TOKEN configuration is invalid', error)),
			Effect.catchTag('ConfigError', Effect.die),
		)

		const recoveryAfterMs = 30_000

		const MailboxProcessingAlchemyCloudflare = MailboxProcessingLive({
			concurrency: 1,
			maxAttempts: 5,
		}).pipe(
			Layer.provide(MailboxProcessingBackendFromDurableObjectStorage({ recoveryAfterMs })),
			Layer.provide(ProviderEventDispatcherAlchemyCloudflare(botToken)),
		)

		return Effect.gen(function* () {
			const deliver = yield* makeDeliverFromDurableObjectStorage
			const processing = yield* MailboxProcessing

			return {
				deliver,
				alarm: () =>
					processing.processReady.pipe(
						Effect.asVoid,
						Effect.catch((error) =>
							Effect.logError('Mailbox alarm could not claim ready work', error).pipe(
								Effect.andThen(Clock.currentTimeMillis),
								Effect.flatMap((now) => state.storage.setAlarm(now + recoveryAfterMs)),
							),
						),
					),
			}
		}).pipe(Effect.provide(MailboxProcessingAlchemyCloudflare))
	}),
) {}
