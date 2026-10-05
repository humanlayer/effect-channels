/**
 * This file defines `Channels.make`: one call that joins providers, storage and event processing
 * into the webhook routes and the layer an application runs.
 *
 * The two halves it is made of are exported too, for hosts that run them in different places.
 * A Cloudflare Worker serves `routesLayer` while its Durable Object runs `processingLayer`.
 */
import { Context, Crypto, Effect, Exit, Layer, Predicate, Scope } from 'effect'
import { HttpEffect, HttpRouter } from 'effect/http'

import type { ChannelsProvider, ChannelsProviderRequirements } from './ChannelsProvider'
import type { ChannelsStorage } from './ChannelsStorage'
import { DeliveryControlLive, type DeliveryControlBackend } from './DeliveryControl'
import { deliveryApiRoutes } from './DeliveryHttpServer'
import { DELIVERY_ID_MAX_LENGTH } from './DeliveryReference'
import { MailboxDelivery } from './MailboxDelivery'
import { QueueDeliveryMode } from './MailboxPolicy'
import type { DeliveryMode } from './MailboxPolicy'
import { MailboxProcessingLive, ProviderEventDispatcherLive } from './MailboxProcessing'
import { ProviderOutputDispatcherLive } from './ProviderOutput'
import type { MailboxProcessingOptions } from './MailboxProcessing'
import type { MailboxSubscriptions } from './MailboxSubscriptions'
import { webhookRoutes } from './ProviderWebhooks'
import type { WebhookRoutesOptions } from './ProviderWebhooks'

/**
 * Router settings a bot's routes need. A delivery ID is longer than the router's default
 * 100-character limit on a path parameter, so an application that mounts `routes` on its own
 * router with the delivery API enabled must pass this, for example
 * `HttpRouter.serve(routes, { routerConfig: Channels.routerConfig })`. `start` sets it itself.
 */
export const routerConfig = { maxParamLength: DELIVERY_ID_MAX_LENGTH }

export type EventProcessingOptions = Pick<MailboxProcessingOptions, 'concurrency' | 'maxAttempts' | 'leaseMs'>

/**
 * What every host needs to know about a bot, whatever stores its mailboxes.
 *
 * @property namespace - tells this application's events apart from another's in shared storage
 * @property basePath - where the webhook routes and the delivery API are mounted inside a larger API
 */
export type Options<Requirements extends ReadonlyArray<ChannelsProviderRequirements>> = WebhookRoutesOptions & {
	readonly namespace: string
	readonly providers: {
		/** One type per provider, so providers whose callbacks need different services can share a list. */
		readonly [Index in keyof Requirements]: ChannelsProvider<Requirements[Index]>
	}
	readonly eventProcessing: EventProcessingOptions
}

/** The webhook routes of a bot. */
export const routesLayer = <const Requirements extends ReadonlyArray<ChannelsProviderRequirements>>(
	options: Options<Requirements>,
) => {
	const providers: ReadonlyArray<ChannelsProvider<Requirements[number]>> = options.providers
	return Layer.unwrap(
		Effect.gen(function* () {
			const webhookProviders = yield* Effect.forEach(providers, (provider) =>
				provider.webhookProvider({ namespace: options.namespace }),
			)
			const requestServices = yield* Effect.context<MailboxDelivery | Crypto.Crypto>()
			return webhookRoutes(webhookProviders, options).pipe(
				HttpRouter.provideRequest(Layer.succeedContext(requestServices)),
			)
		}),
	)
}

const deliveryModeForProviders =
	(providers: ReadonlyArray<{ readonly providerName: string; readonly deliveryMode: DeliveryMode }>) =>
	(providerName: string): DeliveryMode => {
		const provider = providers.find((candidate) => candidate.providerName === providerName)
		/** A mailbox for a provider this bot does not have runs at once and fails as "processor not found". */
		return Predicate.isUndefined(provider) ? QueueDeliveryMode.make({}) : provider.deliveryMode
	}

/** Mailbox processing for a bot: claims batches, runs each provider's callbacks, and sends saved output. */
export const processingLayer = <const Requirements extends ReadonlyArray<ChannelsProviderRequirements>>(
	options: Options<Requirements>,
	polling: MailboxProcessingOptions['polling'],
) => {
	const providers: ReadonlyArray<ChannelsProvider<Requirements[number]>> = options.providers
	return MailboxProcessingLive({
		...options.eventProcessing,
		polling,
		deliveryModeFor: deliveryModeForProviders(providers),
	}).pipe(
		Layer.provide(
			Layer.unwrap(
				Effect.gen(function* () {
					const eventProcessors = yield* Effect.forEach(providers, (provider) =>
						provider.eventProcessor({ namespace: options.namespace }),
					)
					const outputProcessors = yield* Effect.forEach(providers, (provider) =>
						Predicate.isUndefined(provider.outputProcessor)
							? Effect.succeed([])
							: Effect.map(provider.outputProcessor({ namespace: options.namespace }), (processor) => [processor]),
					)
					return Layer.merge(
						ProviderEventDispatcherLive(eventProcessors),
						ProviderOutputDispatcherLive(outputProcessors.flat()),
					)
				}),
			),
		),
	)
}

export type MakeOptions<
	Requirements extends ReadonlyArray<ChannelsProviderRequirements>,
	StorageError,
	StorageRequirements,
	Control extends DeliveryControlBackend = never,
> = Options<Requirements> & {
	readonly storage: ChannelsStorage<StorageError, StorageRequirements, Control>
}

/** A bot started outside an Effect program. */
export type Started = {
	readonly handle: (request: Request) => Promise<Response>
	readonly stop: () => Promise<void>
}

/**
 * Join providers, storage and event processing into one bot.
 *
 * - `routes` is mounted on an `HttpRouter`. It brings `layer` with it, so most applications mount
 *   `routes` and provide only what the storage needs, such as a `SqlClient`.
 * - `layer` is for a process that works through mailboxes but serves no HTTP.
 * - `start` is for applications that do not use Effect: it builds everything and returns a plain
 *   `Request` to `Response` function.
 *
 * Nothing runs until `routes` or `layer` is built. Building either starts polling when the storage
 * polls, and a program that uses both still gets one polling loop.
 */
export const make = <
	const Requirements extends ReadonlyArray<ChannelsProviderRequirements>,
	StorageError,
	StorageRequirements,
	Control extends DeliveryControlBackend = never,
>(
	options: MakeOptions<Requirements, StorageError, StorageRequirements, Control>,
) => {
	const layer = Layer.merge(processingLayer(options, options.storage.polling), options.storage.layer).pipe(
		Layer.provide(options.storage.layer),
	)

	const routes = routesLayer(options).pipe(Layer.provide(layer))

	/**
	 * The delivery API. Mount it beside `routes` to let remote workers finish handed-off deliveries.
	 * It needs a store that provides `DeliveryControlBackend`; with any other store it does not compile.
	 */
	const deliveryApi = deliveryApiRoutes(options).pipe(Layer.provide(DeliveryControlLive), Layer.provide(layer))

	/**
	 * Serve `routes`, and any `extraRoutes` such as `bot.deliveryApi`, outside an Effect program.
	 */
	const start = <ServicesError, ExtraError = never, ExtraRequirements = never>(
		services: Layer.Layer<
			| Requirements[number]['build']
			| Exclude<Requirements[number]['process'], MailboxSubscriptions>
			| StorageRequirements
			| Crypto.Crypto
			| Exclude<ExtraRequirements, HttpRouter.HttpRouter>,
			ServicesError
		>,
		extraRoutes: Layer.Layer<never, ExtraError, ExtraRequirements> = Layer.empty,
	): Promise<Started> =>
		Effect.runPromise(
			Effect.gen(function* () {
				const scope = yield* Scope.make()
				const memoMap = yield* Layer.makeMemoMap
				/** Building the routes builds `layer` too, so polling starts here and not on the first request. */
				const router = yield* Effect.gen(function* () {
					const servicesContext = yield* Layer.buildWithMemoMap(services, memoMap, scope)
					const routerContext = yield* Layer.buildWithMemoMap(
						Layer.provideMerge(
							Layer.merge(routes, extraRoutes),
							HttpRouter.layer.pipe(Layer.provide(Layer.succeed(HttpRouter.RouterConfig, routerConfig))),
						),
						memoMap,
						scope,
					).pipe(
						Effect.updateContext((ambient: Context.Context<never>) =>
							Context.merge(ambient, servicesContext),
						),
					)
					return Context.get(routerContext, HttpRouter.HttpRouter)
				}).pipe(Effect.onError(() => Scope.close(scope, Exit.void)))
				const handle = HttpEffect.toWebHandler(router.asHttpEffect())
				const context = yield* Effect.context<never>()
				return {
					handle: (request: Request) => handle(request),
					stop: () => Effect.runPromiseWith(context)(Scope.close(scope, Exit.void)),
				}
			}),
		)

	return { layer, routes, deliveryApi, start }
}
