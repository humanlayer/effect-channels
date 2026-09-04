import { NodeCrypto, NodeHttpClient } from '@effect/platform-node'
import {
	Channels,
	ChannelsGate,
	ChannelsObserver,
	ConversationCoordinator,
	ConversationSignals,
	GateError,
	Ingress,
	Organizations,
	OrganizationStoreError,
	ProviderRegistry,
	Subscriptions,
	type Message,
	type Thread,
	UserDirectory,
	UserProfileCache,
	type ConversationCoordinatorOptions,
	type UserProfileCacheMemoryOptions,
} from '@humanlayer/channels'
import {
	layerConfig as postgresLayerConfig,
	layerFromClient as postgresLayerFromClient,
	type UserProfileCachePostgresOptions,
} from '@humanlayer/channels-postgres'
import {
	CredentialStoreError,
	Slack,
	SlackClient,
	SlackConnection,
	SlackConnectionLookupInput,
	SlackProvider,
	SlackRegistration,
	SlackRoutes,
	SlackTenantCredentials,
	SlackTenantCreds,
	SlackTeamId,
	type SlackConnectionLookupInput as SlackConnectionLookup,
} from '@humanlayer/channels-slack'
import { Cache, Config, ConfigProvider, Context, Effect, Layer, Match, Option, Schema } from 'effect'
import { HttpClient, HttpRouter, HttpServerResponse } from 'effect/unstable/http'
import { Persistence } from 'effect/unstable/persistence'
import { SqlClient } from 'effect/unstable/sql'

export type SlackProviderConfig<ConnectionError = never, ConnectionRequirements = never> = {
	readonly provider: 'slack'
	readonly loadConnection?: (
		input: SlackConnectionLookup,
	) => Effect.Effect<unknown, ConnectionError, ConnectionRequirements>
}

/** Selects Slack with optional application-owned workspace connection lookup. */
export const slack = <ConnectionError = never, ConnectionRequirements = never>(
	options: {
		readonly loadConnection?: (
			input: SlackConnectionLookup,
		) => Effect.Effect<unknown, ConnectionError, ConnectionRequirements>
	} = {},
): SlackProviderConfig<ConnectionError, ConnectionRequirements> => ({ provider: 'slack', ...options })

export type MemoryStorageOptions = {
	readonly coordinator?: ConversationCoordinatorOptions
	readonly userProfileCache?: UserProfileCacheMemoryOptions
}
export type PostgresStorageOptions = {
	readonly coordinator?: ConversationCoordinatorOptions
	readonly userProfileCache?: UserProfileCachePostgresOptions
	readonly pool?: 'internal' | 'shared'
}
type ChannelsStorageServices = ConversationCoordinator | Persistence.Persistence | UserProfileCache
const StorageTypeId = Symbol.for('@humanlayer/channels-app/Storage')

export type ChannelsStorageConfig<StorageError, Requirements = never> = {
	readonly [StorageTypeId]: Layer.Layer<ChannelsStorageServices, StorageError, Requirements>
}

type PostgresStorageError = Layer.Error<ReturnType<typeof postgresLayerConfig>>

function postgresStorage(
	options: PostgresStorageOptions & { readonly pool: 'shared' },
): ChannelsStorageConfig<PostgresStorageError, SqlClient.SqlClient>
function postgresStorage(options?: PostgresStorageOptions): ChannelsStorageConfig<PostgresStorageError>
function postgresStorage(
	options: PostgresStorageOptions = {},
): ChannelsStorageConfig<PostgresStorageError, SqlClient.SqlClient> | ChannelsStorageConfig<PostgresStorageError> {
	return options.pool === 'shared'
		? { [StorageTypeId]: postgresLayerFromClient(options.coordinator, options.userProfileCache) }
		: { [StorageTypeId]: postgresLayerConfig(options.coordinator, options.userProfileCache) }
}

/** Opaque storage constructors whose internal Layers are owned by `createChannelsApp`. */
export const ChannelsStorage = {
	/** Configures ephemeral in-process coordination, subscriptions, and profile caching. */
	memory: (options: MemoryStorageOptions = {}): ChannelsStorageConfig<never> => ({
		[StorageTypeId]: Layer.mergeAll(
			options.coordinator === undefined
				? ConversationCoordinator.layerMemory()
				: ConversationCoordinator.layerMemory(options.coordinator),
			Persistence.layerMemory,
			UserProfileCache.memory(options.userProfileCache),
		),
	}),
	/** Configures `DATABASE_URL`-backed coordination, subscriptions, migrations, and profile caching. */
	postgres: postgresStorage,
}

/** Compatibility alias for `ChannelsStorage.postgres()`. */
export const postgres = (coordinator?: ConversationCoordinatorOptions) =>
	ChannelsStorage.postgres(coordinator === undefined ? {} : { coordinator })

/** @internal */
export const makeConnectionServices = <ConnectionError, ConnectionRequirements>(
	provider: SlackProviderConfig<ConnectionError, ConnectionRequirements>,
) => {
	if (provider.loadConnection === undefined) {
		return Layer.mergeAll(
			Organizations.layerDefault,
			ChannelsGate.layerAllowAll,
			SlackTenantCredentials.layerFromConfig,
		)
	}
	const loadConnection = provider.loadConnection
	return Layer.effectContext(
		Effect.gen(function* () {
			const callbackContext = yield* Effect.context<ConnectionRequirements>()
			const cache = yield* Cache.make({
				capacity: 1_000,
				timeToLive: '1 minute',
				lookup: (workspaceId: SlackTeamId) =>
					loadConnection(SlackConnectionLookupInput.make({ workspaceId })).pipe(
						Effect.provide(callbackContext),
						Effect.flatMap((output) =>
							Schema.decodeUnknownEffect(Schema.UndefinedOr(SlackConnection))(output),
						),
						Effect.map(Option.fromUndefinedOr),
						Effect.mapError(() => workspaceId),
					),
			})
			const lookup = (workspaceId: SlackTeamId) => Cache.get(cache, workspaceId)
			const organizations = Organizations.of({
				resolve: (input) =>
					Match.value(input.source).pipe(
						Match.when('slack', () =>
							lookup(SlackTeamId.make(input.tenant)).pipe(
								Effect.map(Option.map((connection) => connection.organizationId)),
								Effect.mapError(() =>
									OrganizationStoreError.make({
										source: input.source,
										tenant: input.tenant,
										message: 'Slack connection lookup failed',
									}),
								),
							),
						),
						Match.when('github', () => Effect.succeed(Option.none())),
						Match.when('linear', () => Effect.succeed(Option.none())),
						Match.when('discord', () => Effect.succeed(Option.none())),
						Match.exhaustive,
					),
			})
			const gate = ChannelsGate.of({
				allowed: (input) =>
					Match.value(input.source).pipe(
						Match.when('slack', () =>
							lookup(SlackTeamId.make(input.tenant)).pipe(
								Effect.map(
									Option.match({
										onNone: () => false,
										onSome: (connection) =>
											connection.enabled && connection.organizationId === input.orgId,
									}),
								),
								Effect.mapError(() =>
									GateError.make({
										orgId: input.orgId,
										source: input.source,
										message: 'Slack connection lookup failed',
									}),
								),
							),
						),
						Match.orElse(() => Effect.succeed(false)),
					),
			})
			const credentials = SlackTenantCredentials.of({
				load: (input) =>
					lookup(input.teamId).pipe(
						Effect.map(
							Option.map((connection) =>
								SlackTenantCreds.make({
									botToken: connection.credentials.botToken,
									botUserId: connection.credentials.botUserId,
									botId: connection.credentials.botId,
								}),
							),
						),
						Effect.mapError(() => CredentialStoreError.make({ operation: 'load', teamId: input.teamId })),
					),
				save: (input) => Effect.fail(CredentialStoreError.make({ operation: 'save', teamId: input.teamId })),
			})
			return Context.make(Organizations, organizations).pipe(
				Context.add(ChannelsGate, gate),
				Context.add(SlackTenantCredentials, credentials),
			)
		}),
	)
}

export type ChannelsAppOptions<
	HandlerError,
	HttpError,
	StorageError,
	StorageRequirements,
	ConnectionError,
	ConnectionRequirements,
> = {
	readonly providers: readonly [SlackProviderConfig<ConnectionError, ConnectionRequirements>]
	readonly storage: ChannelsStorageConfig<StorageError, StorageRequirements>
	readonly handlers?: {
		readonly onNewMention?: (thread: Thread, message: Message) => Effect.Effect<void, HandlerError, Channels>
		readonly onSubscribedMessage?: (thread: Thread, message: Message) => Effect.Effect<void, HandlerError, Channels>
	}
	readonly onNewMention?: (thread: Thread, message: Message) => Effect.Effect<void, HandlerError, Channels>
	readonly onSubscribedMessage?: (thread: Thread, message: Message) => Effect.Effect<void, HandlerError, Channels>
	readonly advanced?: {
		readonly httpClient?: Layer.Layer<HttpClient.HttpClient, HttpError>
		readonly slackApiOrigin?: URL
		readonly configProvider?: ConfigProvider.ConfigProvider
	}
}

/** Creates one fully composed Channels application from providers, storage, and Effect handlers. */
export const createChannelsApp = <
	HandlerError = never,
	HttpError = never,
	StorageError = never,
	StorageRequirements = never,
	ConnectionError = never,
	ConnectionRequirements = never,
>(
	options: ChannelsAppOptions<
		HandlerError,
		HttpError,
		StorageError,
		StorageRequirements,
		ConnectionError,
		ConnectionRequirements
	>,
) => {
	const storage = options.storage[StorageTypeId]
	const subscriptions = Subscriptions.layer.pipe(Layer.provide(storage))
	const registry = ProviderRegistry.layer
	const userDirectory = UserDirectory.make().pipe(Layer.provide(Layer.merge(registry, storage)))
	const connectionServices = makeConnectionServices(options.providers[0])
	const httpClient = options.advanced?.httpClient ?? NodeHttpClient.layerFetch
	const slackClient = SlackClient.layerWith({ apiOrigin: options.advanced?.slackApiOrigin }).pipe(
		Layer.provideMerge(connectionServices),
		Layer.provide(httpClient),
	)
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
					connectionServices,
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
			const onNewMention = options.handlers?.onNewMention ?? options.onNewMention
			const onSubscribedMessage = options.handlers?.onSubscribedMessage ?? options.onSubscribedMessage
			if (onNewMention !== undefined) {
				yield* channels.onNewMention(onNewMention)
			}
			if (onSubscribedMessage !== undefined) {
				yield* channels.onSubscribedMessage(onSubscribedMessage)
			}
			yield* channels.run.pipe(Effect.forkScoped)
		}),
	).pipe(Layer.provide(services))
	const requestServices = Layer.merge(NodeCrypto.layer, services)
	const providerRoutes = SlackRoutes.layer.pipe(
		HttpRouter.provideRequest(requestServices),
		Layer.provide(connectionServices),
	)
	const unconfiguredRoutes = Layer.merge(worker, providerRoutes)
	const routes =
		options.advanced?.configProvider === undefined
			? unconfiguredRoutes
			: unconfiguredRoutes.pipe(Layer.provide(ConfigProvider.layer(options.advanced.configProvider)))
	// SAFETY: Fetch handlers are only executable for self-contained app configurations; Effect apps with requirements mount routes and provide them at the outer runtime edge.
	const fetchRoutes = routes as Layer.Layer<
		never,
		HttpError | StorageError | Config.ConfigError,
		HttpRouter.HttpRouter
	>
	let fetchHandler: ((request: Request) => Promise<Response>) | undefined
	let disposeHandler: (() => Promise<void>) | undefined
	let closed = false
	const handle = (request: Request) => {
		if (closed) {
			return Promise.reject(new Error('Channels application is closed'))
		}
		if (fetchHandler === undefined) {
			const webHandler = HttpRouter.toWebHandler(fetchRoutes, {
				disableLogger: true,
				middleware: (effect) =>
					effect.pipe(
						Effect.catchCause((cause) =>
							Effect.logError('channels Fetch handler failed', cause).pipe(
								Effect.as(HttpServerResponse.empty({ status: 500 })),
							),
						),
					),
			})
			fetchHandler = webHandler.handler
			disposeHandler = webHandler.dispose
		}
		return fetchHandler(request)
	}
	const close = async () => {
		if (closed) {
			return
		}
		closed = true
		const dispose = disposeHandler
		if (dispose !== undefined) {
			await dispose()
		}
	}
	return { routes, handle, close }
}
