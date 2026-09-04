import { DateTime, Effect, Layer, Predicate, Stream } from 'effect'
import { Persistence } from 'effect/unstable/persistence'

import {
	ChannelId,
	ChannelProvider,
	Channels,
	ChannelsGate,
	ChannelsObserver,
	ConversationCoordinator,
	ConversationSignals,
	IdempotencyKey,
	Ingress,
	Message,
	MessageEvent,
	MessageRef,
	NewMentionDelivery,
	NormalizedMessage,
	OrgId,
	Organizations,
	ProviderName,
	ProviderRegistry,
	Subscriptions,
	TenantId,
	Thread,
	ThreadId,
	UserId,
	UserDirectory,
	UserProfileCache,
	unimplemented,
	type Author,
	type Capabilities,
	type ChannelProviderFields,
	type ThreadRef,
} from '../src/index.ts'

const persistence = Persistence.layerMemory
const subscriptions = Subscriptions.layer.pipe(Layer.provide(persistence))
const registry = ProviderRegistry.layer
const userDirectory = UserDirectory.make().pipe(Layer.provide(Layer.merge(registry, UserProfileCache.layerMemory)))

export const CoreDependencies = Layer.mergeAll(
	ConversationCoordinator.layerMemory(),
	ConversationSignals.layerMemory,
	registry,
	userDirectory,
	Organizations.layerDefault,
	ChannelsGate.layerAllowAll,
	ChannelsObserver.layerLogger,
	subscriptions,
)

export const ChannelsLayer = Channels.layer().pipe(Layer.provide(CoreDependencies))

export const ChannelsWithIngressLayer = Layer.merge(Channels.layer(), Ingress.layer).pipe(
	Layer.provideMerge(CoreDependencies),
)

export const testChannelRef = {
	id: ChannelId.make('slack:v1:T_TEST:C_TEST'),
	provider: ProviderName.make('slack'),
	tenant: TenantId.make('T_TEST'),
	isDm: false,
}

export const testThreadRef = {
	id: ThreadId.make('slack:v1:T_TEST:C_TEST:100.1'),
	channel: testChannelRef,
	isNew: true,
}

export const testAuthor = {
	userId: UserId.make('U_TEST'),
	userName: 'tester',
	fullName: 'Test User',
	isBot: false,
	isMe: false,
}

export const testMessage = Message.make({
	ref: MessageRef.make('100.1'),
	threadRef: testThreadRef,
	text: 'hello',
	markdown: 'hello',
	author: testAuthor,
	metadata: { sentAt: DateTime.makeUnsafe('2026-08-29T12:00:00Z') },
	attachments: [],
	raw: { type: 'app_mention' },
})

export const testThread = Thread.make({
	ref: testThreadRef,
	currentMessage: testMessage,
	recentMessages: [testMessage],
})

export const makeTestMessageEvent = (idempotencyKey: string) =>
	MessageEvent.make({
		orgId: OrgId.make('org_test'),
		provider: 'slack',
		tenant: TenantId.make('T_TEST'),
		idempotencyKey: IdempotencyKey.make(idempotencyKey),
		thread: testThread,
		message: testMessage,
		delivery: NewMentionDelivery.make({ location: 'channel_root' }),
		raw: { type: 'app_mention' },
	})

export const testMessageEvent = makeTestMessageEvent(`evt_${'0'.repeat(32)}`)

export const makeTestAuthor = (input: {
	readonly userId: string
	readonly isBot?: boolean | 'unknown'
	readonly isMe?: boolean
}): Author => ({
	userId: UserId.make(input.userId),
	userName: input.userId,
	fullName: input.userId,
	isBot: input.isBot ?? false,
	isMe: input.isMe ?? false,
})

export const testIdempotencyKey = (seed: string) =>
	IdempotencyKey.make(
		`evt_${seed
			.replace(/[^a-f0-9]/g, '')
			.padEnd(32, '0')
			.slice(0, 32)}`,
	)

export const testThreadRefFor = (rootTs: string, isNew: boolean): ThreadRef => ({
	id: ThreadId.make(`slack:v1:T_TEST:C_TEST:${rootTs}`),
	channel: testChannelRef,
	isNew,
})

export const makeTestMessage = (input: {
	readonly messageTs: string
	readonly threadRef?: ThreadRef
	readonly author?: Author
	readonly text?: string
}) => {
	const text = input.text ?? `message ${input.messageTs}`
	return Message.make({
		ref: MessageRef.make(input.messageTs),
		threadRef: input.threadRef ?? testThreadRef,
		text,
		markdown: text,
		author: input.author ?? testAuthor,
		metadata: { sentAt: DateTime.makeUnsafe({ epochMilliseconds: Math.round(Number(input.messageTs) * 1000) }) },
		attachments: [],
		raw: { ts: input.messageTs },
	})
}

export const makeTestNormalizedMessage = (input: {
	readonly messageTs: string
	readonly rootTs?: string
	readonly mentioned?: boolean
	readonly author?: Author
	readonly text?: string
}): NormalizedMessage => {
	const threadRef = testThreadRefFor(input.rootTs ?? input.messageTs, input.rootTs === undefined)
	const message = makeTestMessage({
		messageTs: input.messageTs,
		threadRef,
		author: input.author ?? testAuthor,
		text: input.text ?? `message ${input.messageTs}`,
	})
	return NormalizedMessage.make({
		provider: 'slack',
		tenant: TenantId.make('T_TEST'),
		idempotencyKey: testIdempotencyKey(input.messageTs),
		thread: Thread.make({ ref: threadRef, currentMessage: message, recentMessages: [message] }),
		message,
		mentioned: input.mentioned ?? false,
		raw: { ts: input.messageTs },
	})
}

export const noCapabilities: Capabilities = {
	threadPost: false,
	channelPost: false,
	edit: false,
	delete: false,
	streaming: 'unsupported',
	typing: { thread: false, channel: false },
	history: { thread: false, channelMessages: false, channelThreads: false },
	reactions: { add: false, remove: false, events: false },
	files: { read: false, upload: false },
	actions: false,
	threadInfo: false,
	channelInfo: false,
	createThread: false,
	directMessages: { ingress: false, open: false },
	ephemeral: { native: false, dmFallback: false },
	subject: false,
}

export const expectTaggedFailure =
	<K extends string>(tag: K) =>
	<A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<E & { readonly _tag: K }, A, R> =>
		Effect.flatMap(Effect.flip(effect), (error) =>
			Predicate.isTagged(tag)(error) ? Effect.succeed(error) : Effect.die(new Error(`expected a ${tag} failure`)),
		)

export const makeFakeProvider = (overrides: Partial<ChannelProviderFields>) =>
	new ChannelProvider({
		name: 'slack',
		capabilities: noCapabilities,
		post: () => unimplemented('test.provider.post'),
		postToChannel: () => unimplemented('test.provider.postToChannel'),
		edit: () => unimplemented('test.provider.edit'),
		delete: () => unimplemented('test.provider.delete'),
		stream: () => unimplemented('test.provider.stream'),
		startThreadTyping: () => unimplemented('test.provider.startThreadTyping'),
		startChannelTyping: () => unimplemented('test.provider.startChannelTyping'),
		addReaction: () => unimplemented('test.provider.addReaction'),
		removeReaction: () => unimplemented('test.provider.removeReaction'),
		messages: () => unimplemented('test.provider.messages'),
		messageStream: () => Stream.fromEffect(unimplemented('test.provider.messageStream')),
		containerMessages: () => unimplemented('test.provider.containerMessages'),
		containerMessageStream: () => Stream.fromEffect(unimplemented('test.provider.containerMessageStream')),
		channelThreads: () => unimplemented('test.provider.channelThreads'),
		channelThreadStream: () => Stream.fromEffect(unimplemented('test.provider.channelThreadStream')),
		info: () => unimplemented('test.provider.info'),
		channelInfo: () => unimplemented('test.provider.channelInfo'),
		getUser: () => unimplemented('test.provider.getUser'),
		subject: () => unimplemented('test.provider.subject'),
		downloadAttachment: () => unimplemented('test.provider.downloadAttachment'),
		openDM: () => unimplemented('test.provider.openDM'),
		postEphemeral: () => unimplemented('test.provider.postEphemeral'),
		...overrides,
	})
