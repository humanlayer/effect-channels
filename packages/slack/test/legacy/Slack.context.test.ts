import { assert, it } from '@effect/vitest'
import { Effect, Queue, Stream } from 'effect'

import { Slack, SlackApiError, type SlackHistoryInput, type SlackRepliesInput } from '../../src/index'
import { testTeamId, testChannelId, testRootTs } from '../support'
import {
	expectTaggedFailure,
	makeTestAuthor,
	makeTestMessage,
	nativeSlackLayer,
	testChannelRef,
	testMessageEvent,
	testThreadRef,
} from './support'

const human = makeTestAuthor({ userId: 'U_HUMAN' })
const me = makeTestAuthor({ userId: 'U_BOT', isBot: true, isMe: true })
const otherBot = makeTestAuthor({ userId: 'B_OTHER', isBot: true })
const threadOldestFirst = [human, me, otherBot, human].map((author, index) =>
	makeTestMessage({ messageTs: `100.${index + 1}`, author }),
)
const containerNewestFirst = [otherBot, me, human].map((author, index) =>
	makeTestMessage({ messageTs: `99.${3 - index}`, author }),
)

it.effect('loads oldest-first native thread context without filtering humans, other bots, or self', () =>
	Effect.gen(function* () {
		const calls = yield* Queue.unbounded<SlackRepliesInput>()
		yield* Effect.gen(function* () {
			const slack = yield* Slack
			const messages = yield* slack
				.messageStream({ threadId: testThreadRef.id, options: { direction: 'forward' } })
				.pipe(Stream.take(10), Stream.runCollect)
			assert.deepStrictEqual(
				messages.map((message) => message.ref),
				['100.1', '100.2', '100.3', '100.4'],
			)
			assert.deepStrictEqual(
				messages.map((message) => message.author.userId),
				['U_HUMAN', 'U_BOT', 'B_OTHER', 'U_HUMAN'],
			)
			assert.deepStrictEqual(yield* Queue.takeAll(calls), [
				{ teamId: testTeamId, channelId: testChannelId, threadTs: testRootTs, direction: 'forward' },
			])
		}).pipe(
			Effect.provide(
				nativeSlackLayer({
					replies: (input) => Queue.offer(calls, input).pipe(Effect.as({ messages: threadOldestFirst })),
				}),
			),
		)
	}),
)

it.effect('composes bounded preceding-channel context in chronological order', () =>
	Effect.gen(function* () {
		const calls = yield* Queue.unbounded<SlackHistoryInput>()
		yield* Effect.gen(function* () {
			const slack = yield* Slack
			const thread = yield* slack
				.messageStream({ threadId: testThreadRef.id, options: { direction: 'forward' } })
				.pipe(Stream.take(2), Stream.runCollect)
			const preceding = yield* slack
				.containerMessageStream({
					channel: testChannelRef,
					before: testMessageEvent.message.ref,
					options: { direction: 'backward' },
				})
				.pipe(Stream.take(2), Stream.runCollect)
			const chronological = [...preceding].reverse()
			assert.deepStrictEqual(
				thread.map((message) => message.ref),
				['100.1', '100.2'],
			)
			assert.deepStrictEqual(
				chronological.map((message) => message.ref),
				['99.2', '99.3'],
			)
			assert.deepStrictEqual(
				chronological.map((message) => message.author.userId),
				['U_BOT', 'B_OTHER'],
			)
			assert.deepStrictEqual(yield* Queue.takeAll(calls), [
				{ teamId: testTeamId, channelId: testChannelId, before: testRootTs, direction: 'backward' },
			])
		}).pipe(
			Effect.provide(
				nativeSlackLayer({
					replies: () => Effect.succeed({ messages: threadOldestFirst }),
					history: (input) => Queue.offer(calls, input).pipe(Effect.as({ messages: containerNewestFirst })),
				}),
			),
		)
	}),
)

it.effect('retains actionable history failures rather than a facade ContextLoadFailed wrapper', () =>
	Effect.gen(function* () {
		const slack = yield* Slack
		const error = yield* expectTaggedFailure('HistoryFailed')(
			slack.messageStream({ threadId: testThreadRef.id }).pipe(Stream.runCollect),
		)
		assert.strictEqual(error.provider, 'slack')
		assert.strictEqual(error.retryability, 'non_retryable')
	}).pipe(
		Effect.provide(
			nativeSlackLayer({
				replies: () =>
					Effect.fail(SlackApiError.make({ operation: 'conversations.replies', code: 'missing_scope' })),
			}),
		),
	),
)
