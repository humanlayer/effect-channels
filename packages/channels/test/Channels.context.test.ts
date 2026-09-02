import { assert, it } from '@effect/vitest'
import { Effect, Ref } from 'effect'

import {
	Channels,
	ContainerAndThreadContext,
	HistoryFailed,
	ProviderRegistry,
	ThreadContext,
	type Capabilities,
	type ChannelProvider,
	type ContainerMessagesInput,
	type MessagesInput,
} from '../src/index.ts'
import {
	ChannelsWithIngressLayer,
	expectTaggedFailure,
	makeFakeProvider,
	makeTestAuthor,
	makeTestMessage,
	noCapabilities,
	testChannelRef,
	testMessageEvent,
	testThreadRef,
} from './support.ts'

const human = makeTestAuthor({ userId: 'U_HUMAN' })
const me = makeTestAuthor({ userId: 'U_BOT', isBot: true, isMe: true })
const otherBot = makeTestAuthor({ userId: 'B_OTHER', isBot: true })

const threadOldestFirst = [
	makeTestMessage({ messageTs: '100.1', author: human }),
	makeTestMessage({ messageTs: '100.2', author: me }),
	makeTestMessage({ messageTs: '100.3', author: otherBot }),
	makeTestMessage({ messageTs: '100.4', author: human }),
]

const containerNewestFirst = [
	makeTestMessage({ messageTs: '99.3', author: otherBot }),
	makeTestMessage({ messageTs: '99.2', author: me }),
	makeTestMessage({ messageTs: '99.1', author: human }),
]

const historyCapabilities = (history: Capabilities['history']): Capabilities => ({ ...noCapabilities, history })

const withProvider = <A, E, R>(provider: ChannelProvider, program: Effect.Effect<A, E, R>) =>
	Effect.gen(function* () {
		const registry = yield* ProviderRegistry
		yield* registry.register(provider)
		return yield* program
	}).pipe(Effect.provide(ChannelsWithIngressLayer))

const refs = (messages: ReadonlyArray<{ readonly ref: string }>) => messages.map((message) => message.ref)

it.effect('loads thread context oldest-first and keeps every author, including bots and the agent itself', () =>
	Effect.gen(function* () {
		const threadCalls = yield* Ref.make<ReadonlyArray<MessagesInput>>([])
		const containerCalls = yield* Ref.make<ReadonlyArray<ContainerMessagesInput>>([])
		const provider = makeFakeProvider({
			capabilities: historyCapabilities({ thread: true, channelMessages: true, channelThreads: false }),
			messages: (input) =>
				Ref.update(threadCalls, (calls) => [...calls, input]).pipe(Effect.as({ messages: threadOldestFirst })),
			containerMessages: (input) =>
				Ref.update(containerCalls, (calls) => [...calls, input]).pipe(
					Effect.as({ messages: containerNewestFirst }),
				),
		})
		yield* withProvider(
			provider,
			Effect.gen(function* () {
				const channels = yield* Channels
				const context = yield* channels.context({
					event: testMessageEvent,
					policy: ThreadContext.make({ threadLimit: 10 }),
				})
				assert.deepStrictEqual(context.event, testMessageEvent)
				assert.deepStrictEqual(refs(context.threadMessages), ['100.1', '100.2', '100.3', '100.4'])
				assert.deepStrictEqual(
					context.threadMessages.map((message) => message.author.userId),
					['U_HUMAN', 'U_BOT', 'B_OTHER', 'U_HUMAN'],
				)
				assert.deepStrictEqual(context.containerMessages, [])
				assert.deepStrictEqual(yield* Ref.get(threadCalls), [
					{ threadId: testThreadRef.id, options: { direction: 'forward' } },
				])
				assert.deepStrictEqual(yield* Ref.get(containerCalls), [])
			}),
		)
	}),
)

it.effect('loads container context before the current message and flips it to chronological order', () =>
	Effect.gen(function* () {
		const containerCalls = yield* Ref.make<ReadonlyArray<ContainerMessagesInput>>([])
		const provider = makeFakeProvider({
			capabilities: historyCapabilities({ thread: true, channelMessages: true, channelThreads: false }),
			messages: () => Effect.succeed({ messages: threadOldestFirst }),
			containerMessages: (input) =>
				Ref.update(containerCalls, (calls) => [...calls, input]).pipe(
					Effect.as({ messages: containerNewestFirst }),
				),
		})
		yield* withProvider(
			provider,
			Effect.gen(function* () {
				const channels = yield* Channels
				const context = yield* channels.context({
					event: testMessageEvent,
					policy: ContainerAndThreadContext.make({ threadLimit: 2, containerLimit: 2 }),
				})
				assert.deepStrictEqual(refs(context.threadMessages), ['100.1', '100.2'])
				assert.deepStrictEqual(refs(context.containerMessages), ['99.2', '99.3'])
				assert.deepStrictEqual(
					context.containerMessages.map((message) => message.author.userId),
					['U_BOT', 'B_OTHER'],
				)
				assert.deepStrictEqual(yield* Ref.get(containerCalls), [
					{
						channel: testChannelRef,
						before: testMessageEvent.message.ref,
						options: { direction: 'backward' },
					},
				])
			}),
		)
	}),
)

it.effect('fails with UnsupportedContextScope when the provider lacks the requested scope', () =>
	Effect.gen(function* () {
		const threadOnly = makeFakeProvider({
			capabilities: historyCapabilities({ thread: true, channelMessages: false, channelThreads: false }),
			messages: () => Effect.succeed({ messages: threadOldestFirst }),
		})
		yield* withProvider(
			threadOnly,
			Effect.gen(function* () {
				const channels = yield* Channels
				const error = yield* expectTaggedFailure('UnsupportedContextScope')(
					channels.context({
						event: testMessageEvent,
						policy: ContainerAndThreadContext.make({ threadLimit: 5, containerLimit: 5 }),
					}),
				)
				assert.strictEqual(error.scope, 'channel_messages')
				assert.strictEqual(error.provider, 'slack')
			}),
		)
	}),
)

it.effect('fails with UnsupportedContextScope for thread context when thread history is unsupported', () =>
	Effect.gen(function* () {
		const provider = makeFakeProvider({ capabilities: noCapabilities })
		yield* withProvider(
			provider,
			Effect.gen(function* () {
				const channels = yield* Channels
				const error = yield* expectTaggedFailure('UnsupportedContextScope')(
					channels.context({ event: testMessageEvent, policy: ThreadContext.make({ threadLimit: 5 }) }),
				)
				assert.strictEqual(error.scope, 'thread')
			}),
		)
	}),
)

it.effect('maps provider history failures to ContextLoadFailed', () =>
	Effect.gen(function* () {
		const provider = makeFakeProvider({
			capabilities: historyCapabilities({ thread: true, channelMessages: true, channelThreads: false }),
			messages: () => Effect.fail(HistoryFailed.make({ provider: 'slack', message: 'replies failed' })),
		})
		yield* withProvider(
			provider,
			Effect.gen(function* () {
				const channels = yield* Channels
				const error = yield* expectTaggedFailure('ContextLoadFailed')(
					channels.context({ event: testMessageEvent, policy: ThreadContext.make({ threadLimit: 5 }) }),
				)
				assert.strictEqual(error.provider, 'slack')
			}),
		)
	}),
)
