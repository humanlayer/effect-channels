import {
	MailboxProcessingBackendFromDurableObjectStorage,
	MailboxSubscriptionsFromDurableObjectStorage,
	makeDeliverFromDurableObjectStorage,
	makeMailboxAlarmHandler,
} from '@humanlayer/channels-alchemy-cloudflare'
import { DebounceDeliveryMode, MailboxProcessingLive, QueueDeliveryMode } from '@humanlayer/channels-delivery-next'
import * as Cloudflare from 'alchemy/Cloudflare'
import { Effect, Layer } from 'effect'

import { ProviderEventDispatcherSlack } from './SlackProvider'

/** The application-owned mailbox Durable Object and its alarm handler. */
export class DeliveryMailbox extends Cloudflare.DurableObject<DeliveryMailbox>()(
	'DeliveryMailbox',
	Effect.gen(function* () {
		const leaseMs = 30_000

		const MailboxProcessingAlchemyCloudflare = MailboxProcessingLive({
			concurrency: 1,
			maxAttempts: 5,
			leaseMs,
			polling: 'disabled',
			deliveryModeFor: (provider) =>
				provider === 'slack'
					? DebounceDeliveryMode.make({ quietPeriodMs: 2_000, maxWaitMs: 10_000 })
					: QueueDeliveryMode.make({}),
		}).pipe(
			Layer.provide(MailboxProcessingBackendFromDurableObjectStorage),
			Layer.provide(
				ProviderEventDispatcherSlack.pipe(Layer.provide(MailboxSubscriptionsFromDurableObjectStorage)),
			),
		)

		return Effect.gen(function* () {
			const deliver = yield* makeDeliverFromDurableObjectStorage
			const runMailboxAlarm = yield* makeMailboxAlarmHandler({ rearmAfterMs: 1_000 })

			return { deliver, alarm: () => runMailboxAlarm }
		}).pipe(Effect.provide(MailboxProcessingAlchemyCloudflare))
	}),
) {}
