import { DateTime, Layer } from 'effect'
import { Persistence } from 'effect/unstable/persistence'

import {
	ChannelId,
	Channels,
	ChannelsGate,
	ChannelsObserver,
	ConversationCoordinator,
	ConversationSignals,
	IdempotencyKey,
	Message,
	MessageEvent,
	MessageRef,
	NewMentionDelivery,
	OrgId,
	Organizations,
	ProviderName,
	ProviderRegistry,
	Subscriptions,
	TenantId,
	Thread,
	ThreadId,
	UserId,
} from '../src/index.ts'

const persistence = Persistence.layerMemory
const subscriptions = Subscriptions.layer.pipe(Layer.provide(persistence))

export const CoreDependencies = Layer.mergeAll(
	ConversationCoordinator.layerMemory(),
	ConversationSignals.layerMemory,
	ProviderRegistry.layer,
	Organizations.layerDefault,
	ChannelsGate.layerAllowAll,
	ChannelsObserver.layerLogger,
	subscriptions,
)

export const ChannelsLayer = Channels.layer().pipe(Layer.provide(CoreDependencies))

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
