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
	type MailboxSubscriptions,
} from '@humanlayer/channels-delivery-next'
import { RuntimeContext } from 'alchemy/RuntimeContext'
import { Context, type Crypto, Effect, Layer } from 'effect'
import * as HttpRouter from 'effect/unstable/http/HttpRouter'

import { DeliveryControlAlchemyCloudflare, makeDeliveryRequestHandler } from './DeliveryControl'
import { DeliveryControlBackendFromDurableObjectStorage } from './DeliveryControlBackend'
import { makeMailboxAlarmHandler, type MailboxAlarmHandlerOptions } from './MailboxAlarm'
import {
	MailboxDeliveryAlchemyCloudflare,
	makeDeliverFromDurableObjectStorage,
} from './MailboxDelivery'
import { MailboxProcessingBackendFromDurableObjectStorage } from './MailboxProcessingBackend'
import { type MailboxStorage, MailboxStorageFromDurableObjectState } from './MailboxStorage'
import { MailboxSubscriptionsFromDurableObjectStorage } from './MailboxSubscriptions'

/**
 * The Durable Object half over the given `MailboxStorage`. `make` runs it over the Durable Object's own storage.
 *
 * Needs `Crypto` to make each new batch's ID and token; a Durable Object can provide `NodeCrypto.layer`.
 * A provider that cannot be built, such as one with a missing secret, dies here: a Durable Object has
 * no caller that could handle the failure. The Worker half reports the same failure at deploy.
 */
export const makeMailbox = <const Requirements extends ReadonlyArray<ChannelsProviderRequirements>, R>(
	options: Channels.Options<Requirements>,
	alarmOptions: MailboxAlarmHandlerOptions,
	storage: Layer.Layer<MailboxStorage, never, R>,
) =>
	Effect.gen(function* () {
		const deliver = yield* makeDeliverFromDurableObjectStorage
		const deliveryRequest = yield* makeDeliveryRequestHandler
		const runMailboxAlarm = yield* makeMailboxAlarmHandler(alarmOptions)
		return { deliver, deliveryRequest, alarm: runMailboxAlarm }
	}).pipe(
		Effect.provide(
			Layer.merge(Channels.processingLayer(options, 'disabled'), DeliveryControlLive).pipe(
				Layer.provide(
					Layer.mergeAll(
						MailboxProcessingBackendFromDurableObjectStorage,
						MailboxSubscriptionsFromDurableObjectStorage,
						DeliveryControlBackendFromDurableObjectStorage,
					),
				),
				Layer.provideMerge(storage),
			),
		),
		Effect.orDie,
	)

/**
 * What a mailbox needs from its host: `Crypto`, and whatever the providers' callbacks need. The mailbox
 * itself supplies `RuntimeContext`, so callbacks can call other Durable Objects.
 */
export type MailboxServices<Requirements extends ReadonlyArray<ChannelsProviderRequirements>> = Exclude<
	| Crypto.Crypto
	| Requirements[number]['build']
	| Exclude<Requirements[number]['process'], MailboxSubscriptions>,
	RuntimeContext
>

export const make = <const Requirements extends ReadonlyArray<ChannelsProviderRequirements>>(
	options: Channels.Options<Requirements>,
) => {
	/**
	 * The Durable Object half, as Alchemy's two-phase implementation: pass it to the mailbox class's
	 * `.make(...)`. The outer Effect runs when the object is built and takes the services the callbacks
	 * need, such as `Crypto`, from the host Worker's layers; those become requirements of the class's
	 * layer. The inner Effect runs per instance over the object's own storage and returns `deliver`,
	 * `deliveryRequest` and `alarm`; the callbacks get the instance's `RuntimeContext`. The class cannot
	 * use its alarm for anything else: a Durable Object has one, and the mailbox needs it.
	 */
	const mailbox = (alarmOptions: MailboxAlarmHandlerOptions) =>
		Effect.context<MailboxServices<Requirements>>().pipe(
			Effect.map((services) =>
				Effect.gen(function* () {
					const runtimeContext = yield* RuntimeContext
					return yield* makeMailbox(options, alarmOptions, MailboxStorageFromDurableObjectState).pipe(
						Effect.provideContext(Context.add(services, RuntimeContext, runtimeContext)),
					)
				}),
			),
		)

	/**
	 * The Worker's routes: provider webhooks, forwarded to the mailbox object that owns each event.
	 * Needs `DeliveryMailboxes`, the application's mailbox namespace.
	 */
	const routes = Channels.routesLayer(options).pipe(Layer.provide(MailboxDeliveryAlchemyCloudflare))

	/**
	 * The delivery API, for remote workers finishing handed-off deliveries. Serve it beside `routes`,
	 * or leave it out. Each request goes to the mailbox object that owns the delivery.
	 */
	const deliveryApi = deliveryApiRoutes(options).pipe(Layer.provide(DeliveryControlAlchemyCloudflare))

	return { mailbox, routes, deliveryApi }
}

/**
 * A Worker's `fetch` from the bot's routes. Raises the router's path-parameter limit, because a
 * delivery ID is longer than the default 100 characters. Fails as the providers do when they cannot
 * be built, such as with a `ConfigError` for a missing secret, so Alchemy reports it at deploy.
 */
export const serve = <E, R>(routes: Layer.Layer<never, E, R | HttpRouter.HttpRouter>) =>
	HttpRouter.toHttpEffect(routes).pipe(Effect.provideService(HttpRouter.RouterConfig, Channels.routerConfig))

