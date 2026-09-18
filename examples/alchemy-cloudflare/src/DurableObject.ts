import {
	MailboxProcessingBackendFromDurableObjectStorage,
	MailboxSubscriptionsFromDurableObjectStorage,
	makeDeliverFromDurableObjectStorage,
} from '@humanlayer/channels-alchemy-cloudflare'
import { MailboxProcessing, MailboxProcessingLive } from '@humanlayer/channels-delivery-next'
import * as Cloudflare from 'alchemy/Cloudflare'
import { Clock, Effect, Layer } from 'effect'

import { ProviderEventDispatcherSlack } from './SlackProvider'

/** The application-owned mailbox Durable Object and its alarm handler. */
export class DeliveryMailbox extends Cloudflare.DurableObject<DeliveryMailbox>()(
	'DeliveryMailbox',
	Effect.gen(function* () {
		const state = yield* Cloudflare.DurableObjectState
		const recoveryAfterMs = 30_000

		const MailboxProcessingAlchemyCloudflare = MailboxProcessingLive({
			concurrency: 1,
			maxAttempts: 5,
		}).pipe(
			Layer.provide(MailboxProcessingBackendFromDurableObjectStorage({ recoveryAfterMs })),
			Layer.provide(
				ProviderEventDispatcherSlack.pipe(Layer.provide(MailboxSubscriptionsFromDurableObjectStorage)),
			),
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
