/**
 * This file defines `ChannelsCloudflare.make`: one bot split across a Worker and a Durable Object.
 *
 * Cloudflare differs from the other storage packages because the application declares both classes
 * and the two halves run in different places. The Worker takes webhooks and hands each event to the
 * mailbox's Durable Object. The Durable Object stores the mailbox and runs the callbacks when its
 * alarm fires. Both halves are built from the same options, so they cannot drift apart.
 */
import {
	Channels,
	DeliveryControlLive,
	deliveryApiRoutes,
	type ChannelsProviderRequirements,
} from '@humanlayer/channels-delivery'
import { Effect } from 'effect'

import { DeliveryControlAlchemyCloudflare, makeDeliveryRequestHandler } from './DeliveryControl'
import { DeliveryControlBackendFromDurableObjectStorage } from './DeliveryControlBackend'
import { makeMailboxAlarmHandler, type MailboxAlarmHandlerOptions } from './MailboxAlarm'
import { MailboxDeliveryAlchemyCloudflare, makeDeliverFromDurableObjectStorage } from './MailboxDelivery'
import { MailboxProcessingBackendFromDurableObjectStorage } from './MailboxProcessingBackend'
import { MailboxStorageFromDurableObjectState } from './MailboxStorage'
import { MailboxSubscriptionsFromDurableObjectStorage } from './MailboxSubscriptions'

/** Build a mailbox's RPC and alarm handlers from the services supplied by its program entrypoint. */
export const makeMailbox = (alarmOptions: MailboxAlarmHandlerOptions) =>
	Effect.gen(function* () {
		const deliver = yield* makeDeliverFromDurableObjectStorage
		const deliveryRequest = yield* makeDeliveryRequestHandler
		const runMailboxAlarm = yield* makeMailboxAlarmHandler(alarmOptions)
		return { deliver, deliveryRequest, alarm: runMailboxAlarm }
	})

export const make = <const Requirements extends ReadonlyArray<ChannelsProviderRequirements>>(
	options: Channels.Options<Requirements>,
) => {
	/** Build the Durable Object handlers from services supplied by the Durable Object entrypoint. */
	const mailbox = makeMailbox

	/** Provider webhook routes. The Worker entrypoint supplies `MailboxDelivery`. */
	const routes = Channels.routesLayer(options)

	/** Remote delivery routes. The Worker entrypoint supplies `DeliveryControl`. */
	const deliveryApi = deliveryApiRoutes(options)

	return {
		mailbox,
		routes,
		deliveryApi,
		routerConfig: Channels.routerConfig,
		layers: {
			worker: {
				mailboxDelivery: MailboxDeliveryAlchemyCloudflare,
				deliveryControl: DeliveryControlAlchemyCloudflare,
			},
			mailbox: {
				processing: Channels.processingLayer(options, 'disabled'),
				deliveryControl: DeliveryControlLive,
				processingBackend: MailboxProcessingBackendFromDurableObjectStorage,
				subscriptions: MailboxSubscriptionsFromDurableObjectStorage,
				deliveryControlBackend: DeliveryControlBackendFromDurableObjectStorage,
				storage: MailboxStorageFromDurableObjectState,
			},
		},
	}
}
