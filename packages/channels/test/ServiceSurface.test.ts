import { assert, it } from '@effect/vitest'
import { Cause, Effect, Exit, Layer, Stream } from 'effect'

import {
	ChannelId,
	Channels,
	ConcurrentDelivery,
	ConversationCoordinator,
	ConversationSignals,
	DebounceDelivery,
	Emoji,
	EphemeralNoFallback,
	InterruptDelivery,
	MarkdownContent,
	MessageRef,
	TenantId,
	ThreadContext,
	ThreadId,
	UserId,
	unimplemented,
} from '../src/index.ts'
import { ChannelsLayer, CoreDependencies, testAuthor, testMessage, testMessageEvent, testThread } from './support.ts'

const threadId = ThreadId.make('slack:v1:T_TEST:C_TEST:100.1')
const channel = {
	id: ChannelId.make('slack:v1:T_TEST:C_TEST'),
	provider: 'slack' as const,
	tenant: TenantId.make('T_TEST'),
	isDm: false,
}
const attachment = {
	provider: 'slack' as const,
	tenant: TenantId.make('T_TEST'),
	id: 'F_TEST',
	kind: 'file',
	providerLocator: { id: 'F_TEST' },
}
const content = MarkdownContent.make({ markdown: 'hello' })
const messageRef = MessageRef.make('100.2')

const expectDefect = <A, E, R>(operation: string, effect: Effect.Effect<A, E, R>) =>
	Effect.gen(function* () {
		const exit = yield* Effect.exit(effect)
		assert.strictEqual(Exit.isFailure(exit), true)
		if (Exit.isFailure(exit)) {
			assert.ok(
				Cause.pretty(exit.cause).includes(`${operation} is intentionally unimplemented`),
				`expected defect naming ${operation}, got: ${Cause.pretty(exit.cause)}`,
			)
		}
	})

it.effect('names every Phase 1 core placeholder', () =>
	Effect.gen(function* () {
		const channels = yield* Channels
		yield* expectDefect(
			'Channels.onSubscribedMessage',
			channels.onSubscribedMessage(() => Effect.void),
		)
		yield* expectDefect(
			'Channels.onNewMessage',
			channels.onNewMessage(/^hello/, () => Effect.void),
		)
		yield* expectDefect(
			'Channels.onDirectMessage',
			channels.onDirectMessage(() => Effect.void),
		)
		yield* expectDefect(
			'Channels.onMessageUpdated',
			channels.onMessageUpdated(() => Effect.void),
		)
		yield* expectDefect(
			'Channels.onMessageDeleted',
			channels.onMessageDeleted(() => Effect.void),
		)
		yield* expectDefect(
			'Channels.onConversationStopped',
			channels.onConversationStopped(() => Effect.void),
		)
		yield* expectDefect(
			'Channels.onAssigned',
			channels.onAssigned(() => Effect.void),
		)
		yield* expectDefect(
			'Channels.onAction',
			channels.onAction(() => Effect.void),
		)
		yield* expectDefect(
			'Channels.onReaction',
			channels.onReaction([Emoji.ThumbsUp], () => Effect.void),
		)
		yield* expectDefect(
			'Channels.onAnyReaction',
			channels.onAnyReaction(() => Effect.void),
		)
		yield* expectDefect(
			'Channels.onCommand',
			channels.onCommand(() => Effect.void),
		)
		yield* expectDefect('Channels.postToChannel', channels.postToChannel({ channel, content }))
		yield* expectDefect('Channels.edit', channels.edit({ threadId, messageRef, content }))
		yield* expectDefect('Channels.delete', channels.delete({ threadId, messageRef }))
		yield* expectDefect('Channels.stream', channels.stream({ threadId }, Stream.empty))
		yield* expectDefect('Channels.startThreadTyping', channels.startThreadTyping({ threadId }))
		yield* expectDefect('Channels.startChannelTyping', channels.startChannelTyping({ channel }))
		yield* expectDefect(
			'Channels.addReaction',
			channels.addReaction({ threadId, messageRef, emoji: Emoji.ThumbsUp }),
		)
		yield* expectDefect(
			'Channels.removeReaction',
			channels.removeReaction({ threadId, messageRef, emoji: Emoji.ThumbsUp }),
		)
		yield* expectDefect('Channels.messages', channels.messages({ threadId }))
		yield* expectDefect('Channels.messageStream', Stream.runDrain(channels.messageStream({ threadId })))
		yield* expectDefect('Channels.containerMessages', channels.containerMessages({ channel }))
		yield* expectDefect(
			'Channels.containerMessageStream',
			Stream.runDrain(channels.containerMessageStream({ channel })),
		)
		yield* expectDefect('Channels.channelThreads', channels.channelThreads({ channel }))
		yield* expectDefect('Channels.channelThreadStream', Stream.runDrain(channels.channelThreadStream({ channel })))
		yield* expectDefect(
			'Channels.context',
			channels.context({ event: testMessageEvent, policy: ThreadContext.make({ threadLimit: 10 }) }),
		)
		yield* expectDefect('Channels.info', channels.info({ threadId }))
		yield* expectDefect('Channels.channelInfo', channels.channelInfo({ channel }))
		yield* expectDefect(
			'Channels.getUser',
			channels.getUser({ provider: 'slack', tenant: TenantId.make('T_TEST'), userId: UserId.make('U_TEST') }),
		)
		yield* expectDefect('Channels.subject', channels.subject({ message: testMessage }))
		yield* expectDefect('Channels.downloadAttachment', channels.downloadAttachment({ attachment }))
		yield* expectDefect(
			'Channels.openDM',
			channels.openDM({ provider: 'slack', tenant: TenantId.make('T_TEST'), user: testAuthor }),
		)
		yield* expectDefect(
			'Channels.postEphemeral',
			channels.postEphemeral({ threadId, user: testAuthor, content, fallback: EphemeralNoFallback.make({}) }),
		)
		yield* expectDefect('Thread.getParticipants', testThread.getParticipants())
	}).pipe(Effect.provide(ChannelsLayer)),
)

it.effect('names every coordinator and signal placeholder', () =>
	Effect.gen(function* () {
		const coordinator = yield* ConversationCoordinator
		yield* expectDefect(
			'ConversationCoordinator.requestCancellation',
			coordinator.requestCancellation({ threadId, reason: 'application' }),
		)
		yield* expectDefect(
			'ConversationCoordinator.layerPostgres',
			Effect.scoped(Layer.build(ConversationCoordinator.layerPostgres())),
		)
		yield* expectDefect(
			'ConversationSignals.layerDistributed',
			Effect.scoped(Layer.build(ConversationSignals.layerDistributed)),
		)
	}).pipe(Effect.provide(CoreDependencies)),
)

it.effect('dies during layer construction for unimplemented delivery strategies', () =>
	Effect.gen(function* () {
		const buildWith = (delivery: Parameters<typeof Channels.layer>[0]) =>
			Effect.scoped(Layer.build(Channels.layer(delivery).pipe(Layer.provide(CoreDependencies))))
		yield* expectDefect(
			'Channels.layer.delivery.debounce',
			buildWith({ delivery: DebounceDelivery.make({ windowMs: 100 }) }),
		)
		yield* expectDefect(
			'Channels.layer.delivery.concurrent',
			buildWith({ delivery: ConcurrentDelivery.make({ concurrency: 2 }) }),
		)
		yield* expectDefect('Channels.layer.delivery.interrupt', buildWith({ delivery: InterruptDelivery.make({}) }))
	}),
)

it.effect('includes the exact operation name in shared defects', () =>
	expectDefect('Example.operation', unimplemented('Example.operation')),
)
