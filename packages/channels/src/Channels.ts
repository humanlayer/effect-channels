import { Context, Effect, Layer, Match, Option, Stream } from 'effect'

import { ChannelsGate } from './ChannelsGate.ts'
import { ChannelsObserver } from './ChannelsObserver.ts'
import { ConversationCoordinator } from './ConversationCoordinator.ts'
import { ConversationSignals } from './ConversationSignals.ts'
import {
	ChannelsRunError,
	ContextLoadFailed,
	DeleteFailed,
	EditFailed,
	FileReadFailed,
	HistoryFailed,
	PostFailed,
	ReactionFailed,
	type ObserverError,
	SubjectFailed,
	TenantDisabled,
	ThreadGone,
	UnknownProvider,
	UnknownTenant,
	UnsupportedContextScope,
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
import { QueueDelivery } from './Operations.ts'
import { Organizations } from './Organizations.ts'
import { ProviderRegistry, providerNameFromThreadId } from './ProviderRegistry.ts'
import type { ChannelInfo, FileData, MessageSubject, ThreadInfo, UserProfile } from './Schema.ts'
import { TenantId as TenantIdSchema } from './Schema.ts'
import type { SentMessage } from './SentMessage.ts'
import type { StreamChunk } from './StreamChunk.ts'
import { Subscriptions } from './Subscriptions.ts'
import type { Thread } from './Thread.ts'

type RegisteredMessageHandler = (thread: Thread, message: Message) => Effect.Effect<void, ChannelsRunError>

const addressFromThreadId = (threadId: string) =>
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
		catch: () => UnknownProvider.make({ provider: threadId }),
	})

const observerBestEffort = (effect: Effect.Effect<void, ObserverError>) =>
	effect.pipe(
		Effect.catchCause((cause) => Effect.logError('channels observer failed', cause)),
		Effect.asVoid,
	)

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
			emoji: ReadonlyArray<import('./Emoji.ts').Emoji>,
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
		readonly stream: (
			input: StreamInput,
			chunks: Stream.Stream<StreamChunk>,
		) => Effect.Effect<SentMessage, UnknownProvider | UnknownTenant | TenantDisabled | PostFailed>
		readonly startThreadTyping: (input: StartThreadTypingInput) => Effect.Effect<void>
		readonly startChannelTyping: (input: StartChannelTypingInput) => Effect.Effect<void>
		readonly addReaction: (
			input: ReactInput,
		) => Effect.Effect<void, UnknownProvider | UnknownTenant | ReactionFailed>
		readonly removeReaction: (
			input: ReactInput,
		) => Effect.Effect<void, UnknownProvider | UnknownTenant | ReactionFailed>
		readonly messages: (input: MessagesInput) => Effect.Effect<MessagePage, UnknownProvider | HistoryFailed>
		readonly messageStream: (input: MessagesInput) => Stream.Stream<Message, UnknownProvider | HistoryFailed>
		readonly containerMessages: (
			input: ContainerMessagesInput,
		) => Effect.Effect<MessagePage, UnknownProvider | HistoryFailed | UnsupportedContextScope>
		readonly containerMessageStream: (
			input: ContainerMessagesInput,
		) => Stream.Stream<Message, UnknownProvider | HistoryFailed | UnsupportedContextScope>
		readonly channelThreads: (
			input: ChannelThreadsInput,
		) => Effect.Effect<ThreadPage, UnknownProvider | HistoryFailed | UnsupportedContextScope>
		readonly channelThreadStream: (
			input: ChannelThreadsInput,
		) => Stream.Stream<ThreadSummary, UnknownProvider | HistoryFailed | UnsupportedContextScope>
		readonly context: (
			input: LoadContextInput,
		) => Effect.Effect<ConversationContext, UnknownProvider | ContextLoadFailed | UnsupportedContextScope>
		readonly info: (input: InfoInput) => Effect.Effect<ThreadInfo, UnknownProvider | ThreadGone>
		readonly channelInfo: (
			input: ChannelInfoInput,
		) => Effect.Effect<ChannelInfo, UnknownProvider | import('./Errors.ts').ChannelGone>
		readonly getUser: (input: GetUserInput) => Effect.Effect<UserProfile, UnknownProvider | UnknownTenant>
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
		readonly subscribe: (
			input: SubscriptionInput,
		) => Effect.Effect<SubscriptionTransition, import('./Errors.ts').SubscriptionStoreError>
		readonly isSubscribed: (
			input: SubscriptionInput,
		) => Effect.Effect<boolean, import('./Errors.ts').SubscriptionStoreError>
		readonly unsubscribe: (
			input: SubscriptionInput,
		) => Effect.Effect<void, import('./Errors.ts').SubscriptionStoreError>
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
				const mentionHandlers: Array<RegisteredMessageHandler> = []

				const onNewMention = <E, R>(
					handler: (thread: Thread, message: Message) => Effect.Effect<void, E, R>,
				): Effect.Effect<void, never, R> =>
					Effect.map(Effect.context<R>(), (context) => {
						mentionHandlers.push((thread, message) =>
							handler(thread, message).pipe(
								Effect.provide(context),
								Effect.mapError(() =>
									ChannelsRunError.make({
										operation: 'Channels.onNewMention',
										message: 'registered handler failed',
									}),
								),
							),
						)
					})

				const dispatchMessage = (event: MessageEvent) =>
					Match.value(event.delivery).pipe(
						Match.tagsExhaustive({
							NewMentionDelivery: () =>
								Effect.forEach(mentionHandlers, (handler) => handler(event.thread, event.message), {
									discard: true,
								}),
							SubscribedMessageDelivery: () => unimplemented('Channels.dispatch.subscribedMessage'),
							DirectMessageDelivery: () => unimplemented('Channels.dispatch.directMessage'),
							PatternMessageDelivery: () => unimplemented('Channels.dispatch.patternMessage'),
						}),
					)

				const dispatch = (event: InboundEvent) =>
					Match.value(event).pipe(
						Match.tagsExhaustive({
							MessageEvent: dispatchMessage,
							MessageUpdatedEvent: () => unimplemented('Channels.dispatch.messageUpdated'),
							MessageDeletedEvent: () => unimplemented('Channels.dispatch.messageDeleted'),
							ConversationStoppedEvent: () => unimplemented('Channels.dispatch.conversationStopped'),
							AssignedEvent: () => unimplemented('Channels.dispatch.assigned'),
							ActionEvent: () => unimplemented('Channels.dispatch.action'),
							ReactionEvent: () => unimplemented('Channels.dispatch.reaction'),
							CommandEvent: () => unimplemented('Channels.dispatch.command'),
						}),
						Effect.withSpan('channels.delivery', {
							attributes: {
								provider: event.provider,
								tenant: event.tenant,
								idempotency_key: event.idempotencyKey,
							},
						}),
					)

				const post = Effect.fn('channels.post')(function* (input: PostInput) {
					const address = yield* addressFromThreadId(input.threadId)
					const provider = yield* registry.byThreadId({ threadId: input.threadId })
					const organization = yield* organizations
						.resolve({ source: address.provider, tenant: address.tenant })
						.pipe(Effect.mapError(() => UnknownTenant.make(address)))
					if (Option.isNone(organization)) {
						return yield* UnknownTenant.make(address)
					}
					const allowed = yield* gate
						.allowed({ orgId: organization.value, source: address.provider, tenant: address.tenant })
						.pipe(
							Effect.mapError(() =>
								PostFailed.make({
									provider: address.provider,
									threadId: input.threadId,
									message: 'gate lookup failed',
								}),
							),
						)
					if (!allowed) {
						return yield* TenantDisabled.make({
							orgId: organization.value,
							provider: address.provider,
							tenant: address.tenant,
						})
					}
					const sent = yield* provider.post(input)
					yield* observerBestEffort(
						observer.outboundSent({
							orgId: organization.value,
							provider: address.provider,
							tenant: address.tenant,
							threadId: input.threadId,
							operation: 'post',
							ok: true,
							degraded: sent.ref.degraded,
						}),
					)
					return sent
				})

				return Channels.of({
					onNewMention,
					onSubscribedMessage: () => unimplemented('Channels.onSubscribedMessage'),
					onNewMessage: () => unimplemented('Channels.onNewMessage'),
					onDirectMessage: () => unimplemented('Channels.onDirectMessage'),
					onMessageUpdated: () => unimplemented('Channels.onMessageUpdated'),
					onMessageDeleted: () => unimplemented('Channels.onMessageDeleted'),
					onConversationStopped: () => unimplemented('Channels.onConversationStopped'),
					onAssigned: () => unimplemented('Channels.onAssigned'),
					onAction: () => unimplemented('Channels.onAction'),
					onReaction: () => unimplemented('Channels.onReaction'),
					onAnyReaction: () => unimplemented('Channels.onAnyReaction'),
					onCommand: () => unimplemented('Channels.onCommand'),
					post,
					postToChannel: () => unimplemented('Channels.postToChannel'),
					edit: () => unimplemented('Channels.edit'),
					delete: () => unimplemented('Channels.delete'),
					stream: () => unimplemented('Channels.stream'),
					startThreadTyping: () => unimplemented('Channels.startThreadTyping'),
					startChannelTyping: () => unimplemented('Channels.startChannelTyping'),
					addReaction: () => unimplemented('Channels.addReaction'),
					removeReaction: () => unimplemented('Channels.removeReaction'),
					messages: () => unimplemented('Channels.messages'),
					messageStream: () => Stream.fromEffect(unimplemented('Channels.messageStream')),
					containerMessages: () => unimplemented('Channels.containerMessages'),
					containerMessageStream: () => Stream.fromEffect(unimplemented('Channels.containerMessageStream')),
					channelThreads: () => unimplemented('Channels.channelThreads'),
					channelThreadStream: () => Stream.fromEffect(unimplemented('Channels.channelThreadStream')),
					context: () => unimplemented('Channels.context'),
					info: () => unimplemented('Channels.info'),
					channelInfo: () => unimplemented('Channels.channelInfo'),
					getUser: () => unimplemented('Channels.getUser'),
					subject: () => unimplemented('Channels.subject'),
					downloadAttachment: () => unimplemented('Channels.downloadAttachment'),
					openDM: () => unimplemented('Channels.openDM'),
					postEphemeral: () => unimplemented('Channels.postEphemeral'),
					subscribe: subscriptions.subscribe,
					isSubscribed: subscriptions.isSubscribed,
					unsubscribe: subscriptions.unsubscribe,
					run: coordinator.run(dispatch).pipe(
						Effect.catchTags({
							ConversationLeaseLost: () =>
								Effect.fail(
									ChannelsRunError.make({
										operation: 'ConversationCoordinator.run',
										message: 'conversation lease lost',
									}),
								),
							ConversationCoordinatorUnavailable: () =>
								Effect.fail(
									ChannelsRunError.make({
										operation: 'ConversationCoordinator.run',
										message: 'conversation coordinator unavailable',
									}),
								),
						}),
					),
				})
			}),
		)
	}
}
