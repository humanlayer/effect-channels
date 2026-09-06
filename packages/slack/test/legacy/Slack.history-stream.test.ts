import { assert, it } from '@effect/vitest'
import { Effect, Layer, Ref, Stream } from 'effect'

import {
	Channel,
	Slack,
	SlackApiError,
	Thread,
	type MessageHistoryOptions,
	type MessagePage,
	type ThreadPage,
	type ThreadSummary,
} from '../../src/index.ts'
import type { SlackClient } from '../../src/SlackClient.ts'
import { stubSlackClientLayer } from '../support.ts'
import { makeTestMessage, testChannelRef, testThreadRef, testThreadRefFor } from './support.ts'

const message = (messageTs: string) => makeTestMessage({ messageTs })

const summary = (rootTs: string): ThreadSummary => ({
	thread: testThreadRefFor(rootTs, false),
	rootMessage: makeTestMessage({ messageTs: rootTs, threadRef: testThreadRefFor(rootTs, false) }),
	replyCount: 2,
})

const pageKey = (options: MessageHistoryOptions | undefined) =>
	`${options?.direction ?? 'backward'}:${options?.cursor ?? 'start'}`

const threadPages = new Map<string, MessagePage>([
	['backward:start', { messages: [message('100.5'), message('100.4')], nextCursor: 'c1' }],
	['backward:c1', { messages: [message('100.3'), message('100.2')], nextCursor: 'c2' }],
	['backward:c2', { messages: [message('100.1')] }],
	['forward:start', { messages: [message('100.1'), message('100.2')], nextCursor: 'f1' }],
	['forward:f1', { messages: [message('100.3'), message('100.4')], nextCursor: 'f2' }],
	['forward:f2', { messages: [message('100.5')] }],
])

const containerPages = new Map<string, MessagePage>([
	['backward:start', { messages: [message('300.1'), message('200.1')], nextCursor: 'h1' }],
	['backward:h1', { messages: [message('100.1')] }],
])

const threadListPages = new Map<string, ThreadPage>([
	['backward:start', { threads: [summary('300.1'), summary('200.1')], nextCursor: 't1' }],
	['backward:t1', { threads: [summary('100.1')] }],
])

const scriptedPage = <P>(pages: ReadonlyMap<string, P>, options: MessageHistoryOptions | undefined) => {
	const page = pages.get(pageKey(options))
	return page === undefined
		? Effect.fail(SlackApiError.make({ operation: 'history', code: `no scripted page for ${pageKey(options)}` }))
		: Effect.succeed(page)
}

type RecordedCalls = Ref.Ref<ReadonlyArray<MessageHistoryOptions | undefined>>

const makeCalls = Effect.all({
	thread: Ref.make<ReadonlyArray<MessageHistoryOptions | undefined>>([]),
	channel: Ref.make<ReadonlyArray<MessageHistoryOptions | undefined>>([]),
})

const makeHistoryProvider = (calls: { readonly thread: RecordedCalls; readonly channel: RecordedCalls }) =>
	stubSlackClientLayer({
		getUser: () => Effect.fail(SlackApiError.make({ operation: 'users.info', code: 'user_not_found' })),
		replies: (input) =>
			Ref.update(calls.thread, (all) => [...all, historyOptions(input)]).pipe(
				Effect.andThen(scriptedPage(threadPages, historyOptions(input))),
			),
		history: (input) =>
			Ref.update(calls.channel, (all) => [...all, historyOptions(input)]).pipe(
				Effect.andThen(scriptedPage(containerPages, historyOptions(input))),
			),
		listThreads: (input) =>
			Ref.update(calls.channel, (all) => [...all, historyOptions(input)]).pipe(
				Effect.andThen(scriptedPage(threadListPages, historyOptions(input))),
			),
	})

const withProvider = <A, E, R>(client: Layer.Layer<SlackClient>, program: Effect.Effect<A, E, R>) =>
	program.pipe(Effect.provide(Slack.layer.pipe(Layer.provide(client))))

const refs = (messages: ReadonlyArray<{ readonly ref: string }>) => messages.map((entry) => entry.ref)

const thread = Thread.fromRef(testThreadRef)
const channel = Channel.fromRef(testChannelRef)

it.effect('does no provider I/O until a history stream runs, and one page call per page method', () =>
	Effect.gen(function* () {
		const calls = yield* makeCalls
		yield* withProvider(
			makeHistoryProvider(calls),
			Effect.gen(function* () {
				const threadStream = thread.messages
				const allStream = thread.allMessages
				const channelStream = channel.messages
				const threadListStream = channel.threads
				assert.deepStrictEqual(yield* Ref.get(calls.thread), [])
				assert.deepStrictEqual(yield* Ref.get(calls.channel), [])

				const page = yield* thread.listMessages()
				assert.deepStrictEqual(refs(page.messages), ['100.5', '100.4'])
				assert.strictEqual(page.nextCursor, 'c1')
				assert.deepStrictEqual(yield* Ref.get(calls.thread), [undefined])

				const limited = yield* thread.listMessages({ limit: 2, cursor: 'c1' })
				assert.deepStrictEqual(refs(limited.messages), ['100.3', '100.2'])
				assert.deepStrictEqual(yield* Ref.get(calls.thread), [undefined, { limit: 2, cursor: 'c1' }])

				const channelPage = yield* channel.listMessages()
				assert.deepStrictEqual(refs(channelPage.messages), ['300.1', '200.1'])
				const threadList = yield* channel.listThreads()
				assert.deepStrictEqual(
					threadList.threads.map((entry) => entry.thread.id),
					['slack:v1:T_TEST:C_TEST:300.1', 'slack:v1:T_TEST:C_TEST:200.1'],
				)
				assert.deepStrictEqual(yield* Ref.get(calls.channel), [undefined, undefined])

				const first = yield* threadStream.pipe(Stream.take(1), Stream.runCollect)
				assert.deepStrictEqual(refs(first), ['100.5'])
				assert.deepStrictEqual(yield* Ref.get(calls.thread), [undefined, { limit: 2, cursor: 'c1' }, undefined])

				yield* Ref.set(calls.thread, [])
				const newestFirst = yield* Stream.runCollect(threadStream)
				assert.deepStrictEqual(refs(newestFirst), ['100.5', '100.4', '100.3', '100.2', '100.1'])
				assert.deepStrictEqual(yield* Ref.get(calls.thread), [undefined, { cursor: 'c1' }, { cursor: 'c2' }])

				yield* Ref.set(calls.thread, [])
				const oldestFirst = yield* Stream.runCollect(allStream)
				assert.deepStrictEqual(refs(oldestFirst), ['100.1', '100.2', '100.3', '100.4', '100.5'])
				assert.deepStrictEqual(yield* Ref.get(calls.thread), [
					{ direction: 'forward' },
					{ direction: 'forward', cursor: 'f1' },
					{ direction: 'forward', cursor: 'f2' },
				])

				yield* Ref.set(calls.channel, [])
				const channelMessages = yield* Stream.runCollect(channelStream)
				assert.deepStrictEqual(refs(channelMessages), ['300.1', '200.1', '100.1'])
				assert.deepStrictEqual(yield* Ref.get(calls.channel), [undefined, { cursor: 'h1' }])

				yield* Ref.set(calls.channel, [])
				const summaries = yield* Stream.runCollect(threadListStream)
				assert.deepStrictEqual(
					summaries.map((entry) => entry.thread.id),
					['slack:v1:T_TEST:C_TEST:300.1', 'slack:v1:T_TEST:C_TEST:200.1', 'slack:v1:T_TEST:C_TEST:100.1'],
				)
				assert.deepStrictEqual(yield* Ref.get(calls.channel), [undefined, { cursor: 't1' }])
			}),
		)
	}),
)

it.effect('resumes a stream from a caller-supplied cursor and keeps the caller limit on every page', () =>
	Effect.gen(function* () {
		const calls = yield* makeCalls
		yield* withProvider(
			makeHistoryProvider(calls),
			Effect.gen(function* () {
				const channels = yield* Slack
				const resumed = yield* Stream.runCollect(
					channels.messageStream({ threadId: testThreadRef.id, options: { limit: 2, cursor: 'c1' } }),
				)
				assert.deepStrictEqual(refs(resumed), ['100.3', '100.2', '100.1'])
				assert.deepStrictEqual(yield* Ref.get(calls.thread), [
					{ limit: 2, cursor: 'c1' },
					{ limit: 2, cursor: 'c2' },
				])
			}),
		)
	}),
)

const historyOptions = (input: MessageHistoryOptions): MessageHistoryOptions | undefined => {
	const options: { -readonly [K in keyof MessageHistoryOptions]: MessageHistoryOptions[K] } = {}
	if (input.limit !== undefined) options.limit = input.limit
	if (input.cursor !== undefined) options.cursor = input.cursor
	if (input.direction !== undefined) options.direction = input.direction
	return Object.keys(options).length === 0 ? undefined : options
}
