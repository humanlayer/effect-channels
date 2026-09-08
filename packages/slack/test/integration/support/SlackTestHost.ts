import { NodeCrypto, NodeHttpClient } from '@effect/platform-node'
import {
	DeliveryPolicy,
	type HandlerContext,
	type MailboxReadiness,
	type MailboxStore,
	type RunnerOptions,
} from '@humanlayer/channels-delivery'
import * as DeliveryMemory from '@humanlayer/channels-delivery/memory'
import {
	Slack,
	SlackClient,
	SlackIngress,
	SlackRoutes,
	SlackSubscriptions,
	SlackTenantCredentials,
	type ConversationStoppedEvent,
	type Emoji,
	type Message,
	type MessageEvent,
	type MessageDeletedEvent,
	type MessageUpdatedEvent,
	type ReactionEvent,
	type Thread,
	type SlackConnectionLookupInput as SlackConnectionLookup,
	type SlackHandlerRegistration,
	type SlackSubscriptionsMemoryOptions,
} from '@humanlayer/channels-slack'
import { ConfigProvider, Effect, Layer } from 'effect'
import { HttpClient, HttpRouter, HttpServerResponse } from 'effect/unstable/http'

import { testConnectionStoreLayer } from '../../support.js'

export type SlackProviderConfig<E = never, R = never> = {
	readonly provider: 'slack'
	readonly loadConnection?: (input: SlackConnectionLookup) => Effect.Effect<unknown, E, R>
}

export const slack = <E = never, R = never>(
	options: {
		readonly loadConnection?: (input: SlackConnectionLookup) => Effect.Effect<unknown, E, R>
	} = {},
): SlackProviderConfig<E, R> => ({ provider: 'slack', ...options })

export type ChannelsStorageServices = MailboxStore | MailboxReadiness | SlackSubscriptions
export const StorageTypeId = Symbol.for('test/SlackStorage')
export type ChannelsStorageConfig<E, R = never> = {
	readonly [StorageTypeId]: Layer.Layer<ChannelsStorageServices, E, R>
}
export type MemoryStorageOptions = {
	readonly maxMailboxes?: number
	readonly subscriptions?: SlackSubscriptionsMemoryOptions
}

export const ChannelsStorage = {
	memory: (options: MemoryStorageOptions = {}) => ({
		[StorageTypeId]: Layer.merge(
			DeliveryMemory.layer({ maxMailboxes: options.maxMailboxes ?? 10_000 }),
			SlackSubscriptions.layerMemory(options.subscriptions),
		),
	}),
}

export const defaultDeliveryPolicy = DeliveryPolicy.make({
	mode: 'queue',
	maxPayloadBytes: 256_000,
	maxEnvelopes: 1_000,
	maxOutcomes: 10_000,
	retentionMs: 86_400_000,
	maxAttempts: 5,
	retryBaseMs: 100,
	retryMaxMs: 30_000,
	leaseMs: 30_000,
	heartbeatMs: 5_000,
	conflictRetries: 10,
})
export const defaultRunnerOptions: RunnerOptions = { scanLimit: 100, concurrency: 8, pollMs: 25 }

export const makeConnectionServices = <E, R>(provider: SlackProviderConfig<E, R>) =>
	provider.loadConnection === undefined
		? SlackTenantCredentials.layerFromConfig
		: SlackTenantCredentials.layerWithLookup({ loadConnection: provider.loadConnection })

type HandlerServices = Slack | SlackSubscriptions
type MessageHandler<E> = (
	thread: Thread,
	message: Message,
	context: HandlerContext<MessageEvent>,
) => Effect.Effect<void, E, HandlerServices>
type EventHandler<A, E> = (event: A, context: HandlerContext<A>) => Effect.Effect<void, E, HandlerServices>
export type ReactionHandlerRegistration<E> = {
	readonly id?: string
	readonly emojis: ReadonlyArray<Emoji>
	readonly handler: EventHandler<ReactionEvent, E>
}
type AppHandlers<E> = {
	readonly onNewMention?: MessageHandler<E>
	readonly onSubscribedMessage?: MessageHandler<E>
	readonly onDirectMessage?: MessageHandler<E>
	readonly onMessageUpdated?: EventHandler<MessageUpdatedEvent, E>
	readonly onMessageDeleted?: EventHandler<MessageDeletedEvent, E>
	readonly onReaction?: ReadonlyArray<ReactionHandlerRegistration<E>>
	readonly onAnyReaction?: EventHandler<ReactionEvent, E>
	readonly onConversationStopped?: EventHandler<ConversationStoppedEvent, E>
}
export type ChannelsAppOptions<
	HandlerError,
	HttpError,
	StorageError,
	StorageRequirements,
	ConnectionError,
	ConnectionRequirements,
> = AppHandlers<HandlerError> & {
	readonly providers: readonly [SlackProviderConfig<ConnectionError, ConnectionRequirements>]
	readonly storage: ChannelsStorageConfig<StorageError, StorageRequirements>
	readonly namespace?: string
	readonly policy?: DeliveryPolicy
	readonly runner?: RunnerOptions
	readonly handlers?: AppHandlers<HandlerError>
	readonly advanced?: {
		readonly httpClient?: Layer.Layer<HttpClient.HttpClient, HttpError>
		readonly slackApiOrigin?: URL
		readonly configProvider?: ConfigProvider.ConfigProvider
	}
}

const eventRegistration = <A, E>(
	id: string,
	handler: EventHandler<A, E> | undefined,
): ReadonlyArray<SlackHandlerRegistration<A, E, HandlerServices>> => (handler === undefined ? [] : [{ id, handler }])
const messageRegistration = <E>(id: string, handler: MessageHandler<E> | undefined) =>
	eventRegistration<MessageEvent, E>(
		id,
		handler === undefined ? undefined : (event, context) => handler(event.thread, event.message, context),
	)

export const makeSlackTestHost = <
	HandlerError = never,
	HttpError = never,
	StorageError = never,
	ConnectionError = never,
>(
	options: ChannelsAppOptions<HandlerError, HttpError, StorageError, never, ConnectionError, never>,
) => {
	const storage = options.storage[StorageTypeId]
	const connectionServices = makeConnectionServices(options.providers[0])
	const slackClient = SlackClient.layerWith({ apiOrigin: options.advanced?.slackApiOrigin }).pipe(
		Layer.provideMerge(connectionServices),
		Layer.provide(options.advanced?.httpClient ?? NodeHttpClient.layerFetch),
	)
	const native = Slack.layer.pipe(Layer.provideMerge(slackClient), Layer.provide(testConnectionStoreLayer))
	const handlers = { ...options, ...options.handlers }
	const ingress = SlackIngress.layer({
		namespace: options.namespace ?? 'slack-app',
		policy: options.policy ?? defaultDeliveryPolicy,
		handlers: {
			onNewMention: messageRegistration('new-mention', handlers.onNewMention),
			onSubscribedMessage: messageRegistration('subscribed-message', handlers.onSubscribedMessage),
			onDirectMessage: messageRegistration('direct-message', handlers.onDirectMessage),
			onMessageUpdated: eventRegistration('message-updated', handlers.onMessageUpdated),
			onMessageDeleted: eventRegistration('message-deleted', handlers.onMessageDeleted),
			onConversationStopped: eventRegistration('conversation-stopped', handlers.onConversationStopped),
			onReaction: [
				...eventRegistration('any-reaction', handlers.onAnyReaction),
				...(handlers.onReaction ?? []).map((registration, index) => ({
					id: registration.id ?? `reaction-${index}`,
					handler: (event: ReactionEvent, context: HandlerContext<ReactionEvent>) =>
						registration.emojis.some((emoji) => emoji.name === event.emoji.name)
							? registration.handler(event, context)
							: Effect.void,
				})),
			],
		},
	}).pipe(Layer.provideMerge(Layer.merge(storage, native)))
	const config = options.advanced?.configProvider
	const services = config === undefined ? ingress : ingress.pipe(Layer.provide(ConfigProvider.layer(config)))
	const run = Effect.flatMap(SlackIngress, (ingress) => ingress.run(options.runner ?? defaultRunnerOptions)).pipe(
		Effect.tapError(Effect.logError),
		Effect.withSpan('app.slack.run'),
	)
	const worker = Layer.effectDiscard(run.pipe(Effect.forkScoped)).pipe(Layer.provide(services))
	const providerRoutes = SlackRoutes.layer.pipe(
		HttpRouter.provideRequest(Layer.merge(NodeCrypto.layer, services)),
		Layer.provide(services),
	)
	const routes =
		config === undefined ? providerRoutes : providerRoutes.pipe(Layer.provide(ConfigProvider.layer(config)))
	const fetchRoutes = Layer.merge(routes, worker)
	let webHandler:
		| { readonly handler: (request: Request) => Promise<Response>; readonly dispose: () => Promise<void> }
		| undefined
	let closing: Promise<void> | undefined
	const handle = (request: Request) => {
		if (closing !== undefined) return Promise.reject(new Error('Slack application is closed'))
		webHandler ??= HttpRouter.toWebHandler(fetchRoutes, {
			disableLogger: true,
			middleware: (effect) =>
				effect.pipe(
					Effect.catchCause((cause) =>
						Effect.logError('Slack Fetch handler failed', cause).pipe(
							Effect.as(HttpServerResponse.empty({ status: 500 })),
						),
					),
				),
		})
		return webHandler.handler(request)
	}
	const close = () => (closing ??= webHandler?.dispose() ?? Promise.resolve())
	return { routes, worker, services, run, handle, close }
}
