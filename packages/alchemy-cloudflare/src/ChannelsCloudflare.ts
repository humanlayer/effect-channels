/**
 * This file defines `ChannelsCloudflare.make`: one bot split across a Worker and a Durable Object.
 *
 * Cloudflare differs from the other storage packages because the application declares both classes
 * and the two halves run in different places. The Worker takes webhooks and hands each event to the
 * mailbox's Durable Object. The Durable Object stores the mailbox and runs the callbacks when its
 * alarm fires. Both halves are built from the same options, so they cannot drift apart.
 */
import { Channels, type ChannelsProviderRequirements } from '@humanlayer/channels-delivery-next'
import { Effect, Layer } from 'effect'
import * as HttpRouter from 'effect/unstable/http/HttpRouter'

import { makeMailboxAlarmHandler, type MailboxAlarmHandlerOptions } from './MailboxAlarm'
import {
	MailboxDeliveryAlchemyCloudflare,
	makeDeliverFromDurableObjectStorage,
	type DeliveryMailboxNamespace,
} from './MailboxDelivery'
import { MailboxProcessingBackendFromDurableObjectStorage } from './MailboxProcessingBackend'
import { type MailboxStorage, MailboxStorageFromDurableObjectState } from './MailboxStorage'
import { MailboxSubscriptionsFromDurableObjectStorage } from './MailboxSubscriptions'

/**
 * The Durable Object half over the given `MailboxStorage`. `make` runs it over the Durable Object's own storage.
 *
 * A provider that cannot be built, such as one with a missing secret, is logged by the provider
 * and then dies here: a Durable Object has no caller that could handle the failure.
 */
export const makeMailbox = <const Requirements extends ReadonlyArray<ChannelsProviderRequirements>, R>(
	options: Channels.Options<Requirements>,
	alarmOptions: MailboxAlarmHandlerOptions,
	storage: Layer.Layer<MailboxStorage, never, R>,
) =>
	Effect.gen(function* () {
		const deliver = yield* makeDeliverFromDurableObjectStorage
		const runMailboxAlarm = yield* makeMailboxAlarmHandler(alarmOptions)
		return { deliver, alarm: runMailboxAlarm }
	}).pipe(
		Effect.provide(
			Channels.processingLayer(options, 'disabled').pipe(
				Layer.provide(
					Layer.merge(
						MailboxProcessingBackendFromDurableObjectStorage,
						MailboxSubscriptionsFromDurableObjectStorage,
					),
				),
				Layer.provideMerge(storage),
			),
		),
		Effect.orDie,
	)

export const make = <const Requirements extends ReadonlyArray<ChannelsProviderRequirements>>(
	options: Channels.Options<Requirements>,
) => {
	/**
	 * The Durable Object half. Run it inside the application's own Durable Object class and return
	 * `deliver` and `alarm` from it, beside any methods of the application's own.
	 * The class cannot use its alarm for anything else: a Durable Object has one, and the mailbox needs it.
	 */
	const mailbox = (alarmOptions: MailboxAlarmHandlerOptions) =>
		makeMailbox(options, alarmOptions, MailboxStorageFromDurableObjectState)

	/**
	 * The Worker half. Use one of the two: mount `routes` on the application's own router,
	 * or yield `fetch` for a Worker that serves nothing else.
	 */
	const ingress = (mailboxes: DeliveryMailboxNamespace) => {
		const routes = Channels.routesLayer(options).pipe(Layer.provide(MailboxDeliveryAlchemyCloudflare(mailboxes)))
		return { routes, fetch: HttpRouter.toHttpEffect(routes).pipe(Effect.orDie) }
	}

	return { mailbox, ingress }
}
