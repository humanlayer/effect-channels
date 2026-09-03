import { NodeCrypto, NodeHttpClient } from '@effect/platform-node'
import {
	Channels,
	ChannelsGate,
	ChannelsObserver,
	ConversationSignals,
	Ingress,
	Organizations,
	ProviderRegistry,
	Subscriptions,
	type Message,
	type Thread,
	UserDirectory,
} from '@humanlayer/channels'
import { layerConfig as postgresLayerConfig } from '@humanlayer/channels-postgres'
import {
	Slack,
	SlackClient,
	SlackProvider,
	SlackRegistration,
	SlackRoutes,
	SlackTenantCredentials,
} from '@humanlayer/channels-slack'
import { Effect, Layer } from 'effect'
import { HttpClient, HttpRouter, HttpServerResponse } from 'effect/unstable/http'

export type SlackProviderConfig = { readonly provider: 'slack' }
export type PostgresConfig = {
	readonly adapter: 'postgres'
	readonly coordinator?: Parameters<typeof postgresLayerConfig>[0]
}

export const slack = (): SlackProviderConfig => ({ provider: 'slack' })
export const postgres = (coordinator?: PostgresConfig['coordinator']): PostgresConfig =>
	coordinator === undefined ? { adapter: 'postgres' } : { adapter: 'postgres', coordinator }

export type ChannelsAppOptions<HandlerError, HttpError = never> = {
	readonly providers: readonly [SlackProviderConfig]
	readonly storage: PostgresConfig
	readonly onNewMention?: (thread: Thread, message: Message) => Effect.Effect<void, HandlerError, Channels>
	readonly onSubscribedMessage?: (thread: Thread, message: Message) => Effect.Effect<void, HandlerError, Channels>
	readonly advanced?: {
		readonly httpClient?: Layer.Layer<HttpClient.HttpClient, HttpError>
	}
}

export const createChannelsApp = <HandlerError, HttpError = never>(
	options: ChannelsAppOptions<HandlerError, HttpError>,
) => {
	const storage = postgresLayerConfig(options.storage.coordinator)
	const subscriptions = Subscriptions.layer.pipe(Layer.provide(storage))
	const registry = ProviderRegistry.layer
	const userDirectory = UserDirectory.layer.pipe(Layer.provide(registry))
	const credentials = SlackTenantCredentials.layerFromConfig
	const httpClient = options.advanced?.httpClient ?? NodeHttpClient.layerFetch
	const slackClient = SlackClient.layer.pipe(Layer.provideMerge(credentials), Layer.provide(httpClient))
	const slackProvider = SlackProvider.layer.pipe(Layer.provideMerge(slackClient))
	const registration = SlackRegistration.layer.pipe(Layer.provideMerge(slackProvider), Layer.provide(registry))
	const services = Layer.mergeAll(
		Layer.merge(Channels.layer(), Ingress.layer).pipe(
			Layer.provideMerge(
				Layer.mergeAll(
					storage,
					subscriptions,
					registry,
					ConversationSignals.layerMemory,
					Organizations.layerDefault,
					ChannelsGate.layerAllowAll,
					ChannelsObserver.layerLogger,
					userDirectory,
				),
			),
		),
		Slack.layer.pipe(Layer.provideMerge(slackClient)),
		slackProvider,
		registration,
	)
	const worker = Layer.effectDiscard(
		Effect.gen(function* () {
			const channels = yield* Channels
			if (options.onNewMention !== undefined) {
				yield* channels.onNewMention(options.onNewMention)
			}
			if (options.onSubscribedMessage !== undefined) {
				yield* channels.onSubscribedMessage(options.onSubscribedMessage)
			}
			yield* channels.run.pipe(Effect.forkScoped)
		}),
	).pipe(Layer.provide(services))
	const requestServices = Layer.merge(NodeCrypto.layer, services)
	const providerRoutes = SlackRoutes.layer.pipe(
		HttpRouter.provideRequest(requestServices),
		Layer.provide(credentials),
	)
	const routes = Layer.merge(worker, providerRoutes)
	let fetchHandler: ((request: Request) => Promise<Response>) | undefined
	const handle = (request: Request) => {
		if (fetchHandler === undefined) {
			fetchHandler = HttpRouter.toWebHandler(routes, {
				disableLogger: true,
				middleware: (effect) =>
					effect.pipe(
						Effect.catchCause((cause) =>
							Effect.logError('channels Fetch handler failed', cause).pipe(
								Effect.as(HttpServerResponse.empty({ status: 500 })),
							),
						),
					),
			}).handler
		}
		return fetchHandler(request)
	}
	return { routes, handle }
}
