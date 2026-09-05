import { Context, Effect, Layer, Match, Option, Schema, Stream } from 'effect'

import { ChannelsGate } from './ChannelsGate.ts'
import { ChannelsObserver } from './ChannelsObserver.ts'
import { loadConversationContext } from './Context.ts'
import { ConversationCoordinator } from './ConversationCoordinator.ts'
import { ConversationSignals } from './ConversationSignals.ts'
import type { Emoji } from './Emoji.ts'
import {
	ChannelsRunError,
	ChannelGone,
	ContextLoadFailed,
	DeleteFailed,
	EditFailed,
	FileReadFailed,
	HistoryFailed,
	type MetadataFailed,
	PostFailed,
	ReactionFailed,
	RetryabilityMetadata,
	retryabilityOf,
	type ObserverError,
	SubscriptionStoreError,
	SubjectFailed,
	TenantDisabled,
	ThreadGone,
	UnknownProvider,
	UnknownTenant,
	UnsupportedContextScope,
	type UserLookupFailed,
} from './Errors.ts'
import type {
	ActionEvent,
	AssignedEvent,
	CommandEvent,
	ConversationStoppedEvent,
	InboundEvent,
	MessageDeletedEvent,
	MessageEvent,
	MessageUpdatedEvent,
	ReactionEvent,
	SubscriptionTransition,
} from './Events.ts'
import { MessageEvent as MessageEventSchema } from './Events.ts'
import {
	channelMessagePage,
	channelMessageStream as historyChannelMessageStream,
	channelThreadPage,
	channelThreadStream as historyChannelThreadStream,
	threadMessagePage,
	threadMessageStream as historyThreadMessageStream,
} from './History.ts'
import { unimplemented } from './internal/unimplemented.ts'
import type { Message } from './Message.ts'
import type {
	ChannelInfoInput,
	ChannelPostInput,
	ChannelThreadsInput,
	ContainerMessagesInput,
	ConversationContext,
	DeleteInput,
	DeliveryStrategy,
	DownloadAttachmentInput,
	EditInput,
	EphemeralResult,
	GetUserInput,
	InfoInput,
	LoadContextInput,
	MessagePage,
	MessagesInput,
	OpenDMInput,
	PostEphemeralInput,
	PostInput,
	ReactInput,
	StartChannelTypingInput,
	StartThreadTypingInput,
	StreamInput,
	SubjectInput,
	SubscriptionInput,
	ThreadPage,
	ThreadSummary,
} from './Operations.ts'
import {
	ConversationContext as ConversationContextSchema,
	MessagePage as MessagePageSchema,
	QueueDelivery,
	ThreadPage as ThreadPageSchema,
	ThreadSummary as ThreadSummarySchema,
} from './Operations.ts'
import { Organizations } from './Organizations.ts'
import { ProviderRegistry, providerNameFromThreadId } from './ProviderRegistry.ts'
import type {
	ChannelInfo,
	FileData,
	MessageSubject,
	OrgId,
	ProviderName,
	TenantId,
	ThreadInfo,
	UserProfile,
} from './Schema.ts'
import { TenantId as TenantIdSchema, ThreadId as ThreadIdSchema } from './Schema.ts'
import type { SentMessage } from './SentMessage.ts'
import type { StreamChunk } from './StreamChunk.ts'
import { Subscriptions } from './Subscriptions.ts'
import type { Thread } from './Thread.ts'
import { UserDirectory } from './UserDirectory.ts'

type RegisteredMessageHandler = (thread: Thread, message: Message) => Effect.Effect<void, ChannelsRunError>
type RegisteredEventHandler<A> = (event: A) => Effect.Effect<void, ChannelsRunError>

type EgressAddress = { readonly provider: ProviderName; readonly tenant: TenantId }

const addressFromThreadId = (threadId: string): Effect.Effect<EgressAddress, UnknownProvider> =>
	Effect.try({
		try: () => {
			const provider = providerNameFromThreadId(threadId)
			const parts = threadId.split(':')
			const version = parts.at(1)
			const encodedTenant = parts.at(2)
			if (provider === undefined || version !== 'v1' || encodedTenant === undefined) {
				throw new Error('invalid canonical thread id')
			}
			return { provider, tenant: TenantIdSchema.make(decodeURIComponent(encodedTenant)) }
		},
		catch: () => UnknownProvider.make({ provider: threadId, retryability: 'non_retryable' }),
	})

const observerBestEffort = (effect: Effect.Effect<void, ObserverError>) =>
	effect.pipe(
		Effect.catchCause((cause) => Effect.logError('channels observer failed', cause)),
		Effect.asVoid,
	)

/**
 * Provides provider-neutral channel event handlers, messaging, history, and subscriptions.
 *
 * @category services
 * @since 0.0.0
 */
export class Channels extends Context.Service<
	Channels,
	{
		readonly onNewMention: <E, R>(
			handler: (thread: Thread, message: Message) => Effect.Effect<void, E, R>,
		) => Effect.Effect<void, never, R>
		readonly onSubscribedMessage: <E, R>(
			handler: (thread: Thread, message: Message) => Effect.Effect<void, E, R>,
		) => Effect.Effect<void, never, R>
		readonly onNewMessage: <E, R>(
			pattern: RegExp,
			handler: (thread: Thread, message: Message) => Effect.Effect<void, E, R>,
		) => Effect.Effect<void, never, R>
		readonly onDirectMessage: <E, R>(
			handler: (thread: Thread, message: Message) => Effect.Effect<void, E, R>,
		) => Effect.Effect<void, never, R>
		readonly onMessageUpdated: <E, R>(
			handler: (event: MessageUpdatedEvent) => Effect.Effect<void, E, R>,
		) => Effect.Effect<void, never, R>
		readonly onMessageDeleted: <E, R>(
			handler: (event: MessageDeletedEvent) => Effect.Effect<void, E, R>,
		) => Effect.Effect<void, never, R>
		readonly onConversationStopped: <E, R>(
			handler: (event: ConversationStoppedEvent) => Effect.Effect<void, E, R>,
		) => Effect.Effect<void, never, R>
		readonly onAssigned: <E, R>(
			handler: (event: AssignedEvent) => Effect.Effect<void, E, R>,
		) => Effect.Effect<void, never, R>
		readonly onAction: <E, R>(
			handler: (event: ActionEvent) => Effect.Effect<void, E, R>,
		) => Effect.Effect<void, never, R>
		readonly onReaction: <E, R>(
			emoji: ReadonlyArray<Emoji>,
			handler: (event: ReactionEvent) => Effect.Effect<void, E, R>,
		) => Effect.Effect<void, never, R>
		readonly onAnyReaction: <E, R>(
			handler: (event: ReactionEvent) => Effect.Effect<void, E, R>,
		) => Effect.Effect<void, never, R>
		readonly onCommand: <E, R>(
			handler: (event: CommandEvent) => Effect.Effect<void, E, R>,
		) => Effect.Effect<void, never, R>
		readonly post: (
			input: PostInput,
		) => Effect.Effect<SentMessage, UnknownProvider | UnknownTenant | TenantDisabled | PostFailed>
		readonly postToChannel: (
			input: ChannelPostInput,
		) => Effect.Effect<SentMessage, UnknownProvider | UnknownTenant | TenantDisabled | PostFailed>
		readonly edit: (
			input: EditInput,
		) => Effect.Effect<SentMessage, UnknownProvider | UnknownTenant | TenantDisabled | EditFailed>
		readonly delete: (
			input: DeleteInput,
		) => Effect.Effect<void, UnknownProvider | UnknownTenant | TenantDisabled | DeleteFailed>
		readonly stream: <E, R>(
			input: StreamInput,
			chunks: Stream.Stream<StreamChunk, E, R>,
		) => Effect.Effect<SentMessage, UnknownProvider | UnknownTenant | TenantDisabled | PostFailed, R>
		readonly startThreadTyping: (input: StartThreadTypingInput) => Effect.Effect<void>
		readonly startChannelTyping: (input: StartChannelTypingInput) => Effect.Effect<void>
		readonly addReaction: (
			input: ReactInput,
		) => Effect.Effect<void, UnknownProvider | UnknownTenant | ReactionFailed>
		readonly removeReaction: (
			input: ReactInput,
		) => Effect.Effect<void, UnknownProvider | UnknownTenant | ReactionFailed>
		/**
		 * Returns one provider-backed page of messages from a thread.
		 */
		readonly messages: (input: MessagesInput) => Effect.Effect<MessagePage, UnknownProvider | HistoryFailed>
		/**
		 * Lazily reads every page of messages from a thread in the requested direction.
		 */
		readonly messageStream: (input: MessagesInput) => Stream.Stream<Message, UnknownProvider | HistoryFailed>
		/**
		 * Returns one page of messages from the channel or other container that holds a thread.
		 */
		readonly containerMessages: (
			input: ContainerMessagesInput,
		) => Effect.Effect<MessagePage, UnknownProvider | HistoryFailed | UnsupportedContextScope>
		/**
		 * Lazily reads every page of messages from the channel or other container that holds a thread.
		 */
		readonly containerMessageStream: (
			input: ContainerMessagesInput,
		) => Stream.Stream<Message, UnknownProvider | HistoryFailed | UnsupportedContextScope>
		/**
		 * Returns one page of threads from a channel.
		 */
		readonly channelThreads: (
			input: ChannelThreadsInput,
		) => Effect.Effect<ThreadPage, UnknownProvider | HistoryFailed | UnsupportedContextScope>
		/**
		 * Lazily reads every page of threads from a channel.
		 */
		readonly channelThreadStream: (
			input: ChannelThreadsInput,
		) => Stream.Stream<ThreadSummary, UnknownProvider | HistoryFailed | UnsupportedContextScope>
		/**
		 * Loads bounded thread history and, when requested, preceding channel history for a message event.
		 */
		readonly context: (
			input: LoadContextInput,
		) => Effect.Effect<ConversationContext, UnknownProvider | ContextLoadFailed | UnsupportedContextScope>
		readonly info: (input: InfoInput) => Effect.Effect<ThreadInfo, UnknownProvider | ThreadGone | MetadataFailed>
		readonly channelInfo: (
			input: ChannelInfoInput,
		) => Effect.Effect<ChannelInfo, UnknownProvider | ChannelGone | MetadataFailed>
		readonly getUser: (
			input: GetUserInput,
		) => Effect.Effect<UserProfile, UnknownProvider | UnknownTenant | UserLookupFailed>
		readonly subject: (
			input: SubjectInput,
		) => Effect.Effect<Option.Option<MessageSubject>, UnknownProvider | SubjectFailed>
		readonly downloadAttachment: (
			input: DownloadAttachmentInput,
		) => Effect.Effect<FileData, UnknownProvider | UnknownTenant | FileReadFailed>
		readonly openDM: (input: OpenDMInput) => Effect.Effect<Thread, UnknownProvider | UnknownTenant | PostFailed>
		readonly postEphemeral: (
			input: PostEphemeralInput,
		) => Effect.Effect<EphemeralResult, UnknownProvider | UnknownTenant | PostFailed>
		readonly subscribe: (input: SubscriptionInput) => Effect.Effect<SubscriptionTransition, SubscriptionStoreError>
		readonly isSubscribed: (input: SubscriptionInput) => Effect.Effect<boolean, SubscriptionStoreError>
		readonly unsubscribe: (input: SubscriptionInput) => Effect.Effect<void, SubscriptionStoreError>
		readonly run: Effect.Effect<never, ChannelsRunError>
	}
>()('channels/Channels') {
	static layer(options: { readonly delivery: DeliveryStrategy } = { delivery: QueueDelivery.make({}) }) {
		return Layer.effect(
			Channels,
			Effect.gen(function* () {
				yield* Match.value(options.delivery).pipe(
					Match.tagsExhaustive({
						QueueDelivery: () => Effect.void,
						DebounceDelivery: () => unimplemented('Channels.layer.delivery.debounce'),
						ConcurrentDelivery: () => unimplemented('Channels.layer.delivery.concurrent'),
						InterruptDelivery: () => unimplemented('Channels.layer.delivery.interrupt'),
					}),
				)
				const coordinator = yield* ConversationCoordinator
				yield* ConversationSignals
				const registry = yield* ProviderRegistry
				const organizations = yield* Organizations
				const gate = yield* ChannelsGate
				const observer = yield* ChannelsObserver
				const subscriptions = yield* Subscriptions
				const userDirectory = yield* UserDirectory
				const mentionHandlers: Array<RegisteredMessageHandler> = []
				const subscribedHandlers: Array<RegisteredMessageHandler> = []
				const updatedHandlers: Array<RegisteredEventHandler<MessageUpdatedEvent>> = []
				const deletedHandlers: Array<RegisteredEventHandler<MessageDeletedEvent>> = []
				const reactionHandlers: Array<{
					readonly emoji?: ReadonlyArray<Emoji>
					readonly handler: RegisteredEventHandler<ReactionEvent>
				}> = []
				const stoppedHandlers: Array<RegisteredEventHandler<ConversationStoppedEvent>> = []

				const registerMessageHandler =
					(handlers: Array<RegisteredMessageHandler>, operation: string) =>
					<E, R>(
						handler: (thread: Thread, message: Message) => Effect.Effect<void, E, R>,
					): Effect.Effect<void, never, R> =>
						Effect.map(Effect.context<R>(), (context) => {
							handlers.push((thread, message) =>
								handler(thread, message).pipe(
									Effect.provide(context),
									Effect.mapError((error) =>
										ChannelsRunError.make({
											operation,
											message: 'registered handler failed',
											retryability: Schema.is(RetryabilityMetadata)(error)
												? retryabilityOf(error)
												: 'retryable',
										}),
									),
								),
							)
						})

				const registerEventHandler =
					<A>(handlers: Array<RegisteredEventHandler<A>>, operation: string) =>
					<E, R>(handler: (event: A) => Effect.Effect<void, E, R>): Effect.Effect<void, never, R> =>
						Effect.map(Effect.context<R>(), (context) => {
							handlers.push((event) =>
								handler(event).pipe(
									Effect.provide(context),
									Effect.mapError((error) =>
										ChannelsRunError.make({
											operation,
											message: 'registered handler failed',
											retryability: Schema.is(RetryabilityMetadata)(error)
												? retryabilityOf(error)
												: 'retryable',
										}),
									),
								),
							)
						})

				const dispatchMessage = (event: MessageEvent) =>
					Effect.gen(function* () {
						const { message, thread } = yield* userDirectory.hydrateDelivery(event.thread, event.message)
						yield* Effect.annotateCurrentSpan({ thread_id: thread.ref.id })
						yield* Match.value(event.delivery).pipe(
							Match.tagsExhaustive({
								NewMentionDelivery: () =>
									Effect.forEach(mentionHandlers, (handler) => handler(thread, message), {
										discard: true,
									}),
								SubscribedMessageDelivery: () =>
									Effect.forEach(subscribedHandlers, (handler) => handler(thread, message), {
										discard: true,
									}),
								DirectMessageDelivery: () => unimplemented('Channels.dispatch.directMessage'),
								PatternMessageDelivery: () => unimplemented('Channels.dispatch.patternMessage'),
							}),
						)
					})

				const dispatchDirect = (event: InboundEvent) =>
					Match.value(event).pipe(
						Match.tagsExhaustive({
							MessageEvent: dispatchMessage,
							MessageUpdatedEvent: (updated) =>
								Effect.forEach(updatedHandlers, (handler) => handler(updated), { discard: true }),
							MessageDeletedEvent: (deleted) =>
								Effect.forEach(deletedHandlers, (handler) => handler(deleted), { discard: true }),
							ConversationStoppedEvent: (stopped) =>
								Effect.forEach(stoppedHandlers, (handler) => handler(stopped), { discard: true }),
							AssignedEvent: () => unimplemented('Channels.dispatch.assigned'),
							ActionEvent: () => unimplemented('Channels.dispatch.action'),
							ReactionEvent: (reaction) =>
								Effect.forEach(
									reactionHandlers,
									(registered) =>
										registered.emoji === undefined ||
										registered.emoji.some(
											(emoji) =>
												emoji.name === reaction.emoji.name &&
												emoji.unicode === reaction.emoji.unicode,
										)
											? registered.handler(reaction)
											: Effect.void,
									{ discard: true },
								),
							CommandEvent: () => unimplemented('Channels.dispatch.command'),
						}),
						Effect.withSpan('channels.delivery', {
							attributes: {
								provider: event.provider,
								org_id: event.orgId,
								tenant: event.tenant,
								idempotency_key: event.idempotencyKey,
							},
						}),
					)

				const dispatch = dispatchDirect

				const authorizeEgress = <E>(input: {
					readonly address: EgressAddress
					readonly onGateLookupFailed: () => E
				}): Effect.Effect<OrgId, UnknownTenant | TenantDisabled | E> =>
					Effect.gen(function* () {
						const organization = yield* organizations
							.resolve({ source: input.address.provider, tenant: input.address.tenant })
							.pipe(
								Effect.tapError((error) => Effect.logError('organization lookup failed', error)),
								Effect.mapError(() =>
									UnknownTenant.make({ ...input.address, retryability: 'non_retryable' }),
								),
							)
						if (Option.isNone(organization)) {
							return yield* UnknownTenant.make({ ...input.address, retryability: 'non_retryable' })
						}
						yield* Effect.annotateCurrentSpan({ org_id: organization.value })
						const allowed = yield* gate
							.allowed({
								orgId: organization.value,
								source: input.address.provider,
								tenant: input.address.tenant,
							})
							.pipe(
								Effect.tapError((error) => Effect.logError('channels gate failed', error)),
								Effect.mapError(input.onGateLookupFailed),
							)
						if (!allowed) {
							return yield* TenantDisabled.make({
								orgId: organization.value,
								provider: input.address.provider,
								tenant: input.address.tenant,
								retryability: 'non_retryable',
							})
						}
						return organization.value
					})

				const post = Effect.fn('channels.post')(function* (input: PostInput) {
					const address = yield* addressFromThreadId(input.threadId)
					yield* Effect.annotateCurrentSpan({
						operation: 'post',
						provider: address.provider,
						tenant: address.tenant,
						thread_id: input.threadId,
					})
					const provider = yield* registry.byThreadId({ threadId: input.threadId })
					const orgId = yield* authorizeEgress({
						address,
						onGateLookupFailed: () =>
							PostFailed.make({
								provider: address.provider,
								threadId: input.threadId,
								message: 'gate lookup failed',
							}),
					})
					const reportOutbound = (outcome: {
						readonly ok: boolean
						readonly degraded: ReadonlyArray<string>
					}) =>
						observerBestEffort(
							observer.outboundSent({
								orgId,
								provider: address.provider,
								tenant: address.tenant,
								threadId: input.threadId,
								operation: 'post',
								ok: outcome.ok,
								degraded: outcome.degraded,
							}),
						)
					const sent = yield* provider
						.post(input)
						.pipe(Effect.tapError(() => reportOutbound({ ok: false, degraded: [] })))
					yield* reportOutbound({ ok: true, degraded: sent.ref.degraded })
					return sent
				})

				const postToChannel = Effect.fn('channels.post')(function* (input: ChannelPostInput) {
					const address: EgressAddress = {
						provider: input.channel.provider,
						tenant: input.channel.tenant,
					}
					const fallbackThreadId = ThreadIdSchema.make(input.channel.id)
					yield* Effect.annotateCurrentSpan({
						operation: 'post_to_channel',
						provider: address.provider,
						tenant: address.tenant,
					})
					const provider = yield* registry.byChannel({ channel: input.channel })
					const orgId = yield* authorizeEgress({
						address,
						onGateLookupFailed: () =>
							PostFailed.make({
								provider: address.provider,
								threadId: fallbackThreadId,
								message: 'gate lookup failed',
							}),
					})
					const reportOutbound = (outcome: {
						readonly ok: boolean
						readonly degraded: ReadonlyArray<string>
						readonly threadId: typeof fallbackThreadId
					}) =>
						observerBestEffort(
							observer.outboundSent({
								orgId,
								provider: address.provider,
								tenant: address.tenant,
								threadId: outcome.threadId,
								operation: 'post_to_channel',
								ok: outcome.ok,
								degraded: outcome.degraded,
							}),
						)
					const sent = yield* provider
						.postToChannel(input)
						.pipe(
							Effect.tapError(() =>
								reportOutbound({ ok: false, degraded: [], threadId: fallbackThreadId }),
							),
						)
					yield* reportOutbound({ ok: true, degraded: sent.ref.degraded, threadId: sent.ref.threadId })
					return sent
				})

				const stream = Effect.fn('channels.stream')(function* <E, R>(
					input: StreamInput,
					chunks: Stream.Stream<StreamChunk, E, R>,
				) {
					const address = yield* addressFromThreadId(input.threadId)
					yield* Effect.annotateCurrentSpan({
						operation: 'stream',
						provider: address.provider,
						tenant: address.tenant,
						thread_id: input.threadId,
					})
					const provider = yield* registry.byThreadId({ threadId: input.threadId })
					const orgId = yield* authorizeEgress({
						address,
						onGateLookupFailed: () =>
							PostFailed.make({
								provider: address.provider,
								threadId: input.threadId,
								message: 'gate lookup failed',
							}),
					})
					const report = (ok: boolean, degraded: ReadonlyArray<string>) =>
						observerBestEffort(
							observer.outboundSent({
								orgId,
								provider: address.provider,
								tenant: address.tenant,
								threadId: input.threadId,
								operation: 'stream',
								ok,
								degraded,
							}),
						)
					const sent = yield* provider.stream(input, chunks).pipe(Effect.tapError(() => report(false, [])))
					yield* report(true, sent.ref.degraded)
					return sent
				})

				const typingAllowed = (address: EgressAddress) =>
					organizations.resolve({ source: address.provider, tenant: address.tenant }).pipe(
						Effect.flatMap(
							Option.match({
								onNone: () => Effect.succeed(false),
								onSome: (orgId) =>
									gate.allowed({ orgId, source: address.provider, tenant: address.tenant }),
							}),
						),
						Effect.tapError((error) => Effect.logWarning('channels typing authorization failed', error)),
						Effect.orElseSucceed(() => false),
					)

				const startThreadTyping = Effect.fn('channels.typing.start')(function* (input: StartThreadTypingInput) {
					yield* Effect.gen(function* () {
						const address = yield* addressFromThreadId(input.threadId)
						yield* Effect.annotateCurrentSpan({
							operation: 'start_thread_typing',
							provider: address.provider,
							tenant: address.tenant,
							thread_id: input.threadId,
						})
						const provider = yield* registry.byThreadId({ threadId: input.threadId })
						if (!provider.capabilities.typing.thread) {
							yield* Effect.logDebug('provider does not support thread typing; skipping')
							return
						}
						const allowed = yield* typingAllowed(address)
						if (!allowed) {
							return
						}
						yield* provider.startThreadTyping(input)
					}).pipe(
						Effect.tapError((error) => Effect.logWarning('channels thread typing skipped', error)),
						Effect.ignore,
					)
				})

				const startChannelTyping = Effect.fn('channels.typing.start')(function* (
					input: StartChannelTypingInput,
				) {
					yield* Effect.gen(function* () {
						const address: EgressAddress = {
							provider: input.channel.provider,
							tenant: input.channel.tenant,
						}
						yield* Effect.annotateCurrentSpan({
							operation: 'start_channel_typing',
							provider: address.provider,
							tenant: address.tenant,
						})
						const provider = yield* registry.byChannel({ channel: input.channel })
						if (!provider.capabilities.typing.channel) {
							yield* Effect.logDebug('provider does not support channel typing; skipping')
							return
						}
						const allowed = yield* typingAllowed(address)
						if (!allowed) {
							return
						}
						yield* provider.startChannelTyping(input)
					}).pipe(
						Effect.tapError((error) => Effect.logWarning('channels channel typing skipped', error)),
						Effect.ignore,
					)
				})

				const messages = (input: MessagesInput) =>
					registry.byThreadId({ threadId: input.threadId }).pipe(
						Effect.flatMap((provider) => threadMessagePage(provider, input)),
						Effect.flatMap((page) =>
							Effect.map(userDirectory.hydrateMessages(page.messages), (hydrated) =>
								page.nextCursor === undefined
									? MessagePageSchema.make({ messages: hydrated })
									: MessagePageSchema.make({ messages: hydrated, nextCursor: page.nextCursor }),
							),
						),
					)

				const messageStream = (input: MessagesInput) =>
					Stream.unwrap(
						Effect.map(registry.byThreadId({ threadId: input.threadId }), (provider) =>
							historyThreadMessageStream(provider, input).pipe(
								Stream.mapEffect(userDirectory.hydrateMessage),
							),
						),
					)

				const containerMessages = (input: ContainerMessagesInput) =>
					registry.byChannel({ channel: input.channel }).pipe(
						Effect.flatMap((provider) => channelMessagePage(provider, input)),
						Effect.flatMap((page) =>
							Effect.map(userDirectory.hydrateMessages(page.messages), (hydrated) =>
								page.nextCursor === undefined
									? MessagePageSchema.make({ messages: hydrated })
									: MessagePageSchema.make({ messages: hydrated, nextCursor: page.nextCursor }),
							),
						),
					)

				const containerMessageStream = (input: ContainerMessagesInput) =>
					Stream.unwrap(
						Effect.map(registry.byChannel({ channel: input.channel }), (provider) =>
							historyChannelMessageStream(provider, input).pipe(
								Stream.mapEffect(userDirectory.hydrateMessage),
							),
						),
					)

				const channelThreads = (input: ChannelThreadsInput) =>
					registry.byChannel({ channel: input.channel }).pipe(
						Effect.flatMap((provider) => channelThreadPage(provider, input)),
						Effect.flatMap((page) =>
							Effect.map(
								Effect.forEach(page.threads, (summary) =>
									Effect.map(userDirectory.hydrateMessage(summary.rootMessage), (rootMessage) =>
										ThreadSummarySchema.make({ ...summary, rootMessage }),
									),
								),
								(threads) =>
									page.nextCursor === undefined
										? ThreadPageSchema.make({ threads })
										: ThreadPageSchema.make({ threads, nextCursor: page.nextCursor }),
							),
						),
					)

				const channelThreadStream = (input: ChannelThreadsInput) =>
					Stream.unwrap(
						Effect.map(registry.byChannel({ channel: input.channel }), (provider) =>
							historyChannelThreadStream(provider, input).pipe(
								Stream.mapEffect((summary) =>
									Effect.map(userDirectory.hydrateMessage(summary.rootMessage), (rootMessage) =>
										ThreadSummarySchema.make({ ...summary, rootMessage }),
									),
								),
							),
						),
					)

				const context = Effect.fn('channels.context')(function* (input: LoadContextInput) {
					yield* Effect.annotateCurrentSpan({
						operation: 'context',
						provider: input.event.provider,
						org_id: input.event.orgId,
						tenant: input.event.tenant,
						thread_id: input.event.thread.ref.id,
					})
					const provider = yield* registry.byName({ provider: input.event.provider })
					const loaded = yield* loadConversationContext(provider, input)
					const hydratedEvent = yield* userDirectory.hydrateDelivery(
						loaded.event.thread,
						loaded.event.message,
					)
					const threadMessages = yield* userDirectory.hydrateMessages(loaded.threadMessages)
					const containerMessages = yield* userDirectory.hydrateMessages(loaded.containerMessages)
					const event = MessageEventSchema.make({
						...loaded.event,
						thread: hydratedEvent.thread,
						message: hydratedEvent.message,
					})
					return ConversationContextSchema.make({ event, threadMessages, containerMessages })
				})

				const info = Effect.fn('channels.info')(function* (input: InfoInput) {
					yield* Effect.annotateCurrentSpan({ operation: 'info', thread_id: input.threadId })
					const provider = yield* registry.byThreadId({ threadId: input.threadId })
					return yield* provider.info(input)
				})

				const channelInfo = Effect.fn('channels.channel_info')(function* (input: ChannelInfoInput) {
					yield* Effect.annotateCurrentSpan({
						operation: 'channel_info',
						provider: input.channel.provider,
						tenant: input.channel.tenant,
					})
					const provider = yield* registry.byChannel({ channel: input.channel })
					return yield* provider.channelInfo(input)
				})

				const getUser = Effect.fn('channels.get_user')(function* (input: GetUserInput) {
					yield* Effect.annotateCurrentSpan({
						operation: 'get_user',
						provider: input.provider,
						tenant: input.tenant,
					})
					const provider = yield* registry.byName({ provider: input.provider })
					return yield* provider.getUser(input)
				})

				const downloadAttachment = Effect.fn('channels.download_attachment')(function* (
					input: DownloadAttachmentInput,
				) {
					const address: EgressAddress = {
						provider: input.attachment.provider,
						tenant: input.attachment.tenant,
					}
					yield* Effect.annotateCurrentSpan({
						operation: 'download_attachment',
						provider: address.provider,
						tenant: address.tenant,
						attachment_id: input.attachment.id,
					})
					const provider = yield* registry.byName({ provider: address.provider })
					if (!provider.capabilities.files.read) {
						return yield* FileReadFailed.make({
							provider: address.provider,
							message: 'provider does not support attachment downloads',
						})
					}
					yield* authorizeEgress({
						address,
						onGateLookupFailed: () =>
							FileReadFailed.make({ provider: address.provider, message: 'gate lookup failed' }),
					}).pipe(
						Effect.catchTag('TenantDisabled', () =>
							Effect.fail(
								FileReadFailed.make({ provider: address.provider, message: 'tenant disabled' }),
							),
						),
					)
					return yield* provider.downloadAttachment(input)
				})

				const edit = Effect.fn('channels.edit')(function* (input: EditInput) {
					const address = yield* addressFromThreadId(input.threadId)
					yield* Effect.annotateCurrentSpan({
						operation: 'edit',
						provider: address.provider,
						tenant: address.tenant,
						thread_id: input.threadId,
					})
					const provider = yield* registry.byThreadId({ threadId: input.threadId })
					const orgId = yield* authorizeEgress({
						address,
						onGateLookupFailed: () =>
							EditFailed.make({
								provider: address.provider,
								threadId: input.threadId,
								message: 'gate lookup failed',
							}),
					})
					const report = (ok: boolean) =>
						observerBestEffort(
							observer.outboundSent({
								orgId,
								provider: address.provider,
								tenant: address.tenant,
								threadId: input.threadId,
								operation: 'edit',
								ok,
								degraded: [],
							}),
						)
					const sent = yield* provider.edit(input).pipe(Effect.tapError(() => report(false)))
					yield* report(true)
					return sent
				})

				const deleteMessage = Effect.fn('channels.delete')(function* (input: DeleteInput) {
					const address = yield* addressFromThreadId(input.threadId)
					yield* Effect.annotateCurrentSpan({
						operation: 'delete',
						provider: address.provider,
						tenant: address.tenant,
						thread_id: input.threadId,
					})
					const provider = yield* registry.byThreadId({ threadId: input.threadId })
					const orgId = yield* authorizeEgress({
						address,
						onGateLookupFailed: () =>
							DeleteFailed.make({
								provider: address.provider,
								threadId: input.threadId,
								message: 'gate lookup failed',
							}),
					})
					const report = (ok: boolean) =>
						observerBestEffort(
							observer.outboundSent({
								orgId,
								provider: address.provider,
								tenant: address.tenant,
								threadId: input.threadId,
								operation: 'delete',
								ok,
								degraded: [],
							}),
						)
					yield* provider.delete(input).pipe(Effect.tapError(() => report(false)))
					yield* report(true)
				})

				const react = (method: 'add' | 'remove', input: ReactInput) =>
					Effect.gen(function* () {
						const address = yield* addressFromThreadId(input.threadId)
						yield* Effect.annotateCurrentSpan({
							operation: `${method}_reaction`,
							provider: address.provider,
							tenant: address.tenant,
							thread_id: input.threadId,
						})
						const provider = yield* registry.byThreadId({ threadId: input.threadId })
						const orgId = yield* authorizeEgress({
							address,
							onGateLookupFailed: () =>
								ReactionFailed.make({
									provider: address.provider,
									threadId: input.threadId,
									message: 'gate lookup failed',
								}),
						}).pipe(
							Effect.catchTag('TenantDisabled', () =>
								Effect.fail(
									ReactionFailed.make({
										provider: address.provider,
										threadId: input.threadId,
										message: 'tenant disabled',
									}),
								),
							),
						)
						const report = (ok: boolean) =>
							observerBestEffort(
								observer.outboundSent({
									orgId,
									provider: address.provider,
									tenant: address.tenant,
									threadId: input.threadId,
									operation: `${method}_reaction`,
									ok,
									degraded: [],
								}),
							)
						yield* (method === 'add' ? provider.addReaction(input) : provider.removeReaction(input)).pipe(
							Effect.tapError(() => report(false)),
						)
						yield* report(true)
					})

				return Channels.of({
					onNewMention: registerMessageHandler(mentionHandlers, 'Channels.onNewMention'),
					onSubscribedMessage: registerMessageHandler(subscribedHandlers, 'Channels.onSubscribedMessage'),
					onNewMessage: () => unimplemented('Channels.onNewMessage'),
					onDirectMessage: () => unimplemented('Channels.onDirectMessage'),
					onMessageUpdated: registerEventHandler(updatedHandlers, 'Channels.onMessageUpdated'),
					onMessageDeleted: registerEventHandler(deletedHandlers, 'Channels.onMessageDeleted'),
					onConversationStopped: registerEventHandler(stoppedHandlers, 'Channels.onConversationStopped'),
					onAssigned: () => unimplemented('Channels.onAssigned'),
					onAction: () => unimplemented('Channels.onAction'),
					onReaction: (emoji, handler) =>
						Effect.map(Effect.context(), (context) => {
							reactionHandlers.push({
								emoji,
								handler: (event) =>
									handler(event).pipe(
										Effect.provide(context),
										Effect.mapError((error) =>
											ChannelsRunError.make({
												operation: 'Channels.onReaction',
												message: 'registered handler failed',
												retryability: Schema.is(RetryabilityMetadata)(error)
													? retryabilityOf(error)
													: 'retryable',
											}),
										),
									),
							})
						}),
					onAnyReaction: (handler) =>
						Effect.map(Effect.context(), (context) => {
							reactionHandlers.push({
								handler: (event) =>
									handler(event).pipe(
										Effect.provide(context),
										Effect.mapError((error) =>
											ChannelsRunError.make({
												operation: 'Channels.onAnyReaction',
												message: 'registered handler failed',
												retryability: Schema.is(RetryabilityMetadata)(error)
													? retryabilityOf(error)
													: 'retryable',
											}),
										),
									),
							})
						}),
					onCommand: () => unimplemented('Channels.onCommand'),
					post,
					postToChannel,
					edit,
					delete: deleteMessage,
					stream,
					startThreadTyping,
					startChannelTyping,
					addReaction: (input) => react('add', input),
					removeReaction: (input) => react('remove', input),
					messages,
					messageStream,
					containerMessages,
					containerMessageStream,
					channelThreads,
					channelThreadStream,
					context,
					info,
					channelInfo,
					getUser,
					subject: () => unimplemented('Channels.subject'),
					downloadAttachment,
					openDM: () => unimplemented('Channels.openDM'),
					postEphemeral: () => unimplemented('Channels.postEphemeral'),
					subscribe: subscriptions.subscribe,
					isSubscribed: subscriptions.isSubscribed,
					unsubscribe: subscriptions.unsubscribe,
					run: coordinator.run(dispatch).pipe(
						Effect.catchTags({
							ConversationLeaseLost: (error) =>
								Effect.logError('channels worker lost its conversation lease', error).pipe(
									Effect.andThen(
										Effect.fail(
											ChannelsRunError.make({
												operation: 'ConversationCoordinator.run',
												message: 'conversation lease lost',
											}),
										),
									),
								),
							ConversationCoordinatorUnavailable: (error) =>
								Effect.logError('channels conversation coordinator unavailable', error).pipe(
									Effect.andThen(
										Effect.fail(
											ChannelsRunError.make({
												operation: 'ConversationCoordinator.run',
												message: 'conversation coordinator unavailable',
											}),
										),
									),
								),
						}),
					),
				})
			}),
		)
	}
}
