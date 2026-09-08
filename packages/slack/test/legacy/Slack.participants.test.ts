import { assert, it } from '@effect/vitest'
import { Effect, Layer, Ref } from 'effect'

import { Slack, SlackApiError, Thread, type MessageHistoryOptions, type MessagePage } from '../../src/index.js'
import { stubSlackClientLayer } from '../support.js'
import { makeTestAuthor, makeTestMessage, testThreadRef } from './support.js'

const humanA = makeTestAuthor({ userId: 'U_A', isBot: 'unknown' })
const botB = makeTestAuthor({ userId: 'B_B', isBot: true })
const me = makeTestAuthor({ userId: 'U_BOT', isBot: true, isMe: true })
const humanC = makeTestAuthor({ userId: 'U_C' })
const humanD = makeTestAuthor({ userId: 'U_D' })

const pages = new Map<string, MessagePage>([
	[
		'start',
		{
			messages: [
				makeTestMessage({ messageTs: '100.1', author: humanA }),
				makeTestMessage({ messageTs: '100.2', author: botB }),
			],
			nextCursor: 'p1',
		},
	],
	[
		'p1',
		{
			messages: [
				makeTestMessage({ messageTs: '100.3', author: me }),
				makeTestMessage({ messageTs: '100.4', author: humanA }),
				makeTestMessage({ messageTs: '100.5', author: humanC }),
			],
		},
	],
])

it.effect('derives unique human participants from the complete thread history, seeded by the current author', () =>
	Effect.gen(function* () {
		const calls = yield* Ref.make<ReadonlyArray<MessageHistoryOptions | undefined>>([])
		const provider = stubSlackClientLayer({
			getUser: () => Effect.fail(SlackApiError.make({ operation: 'users.info', code: 'user_not_found' })),
			replies: (input) =>
				Ref.update(calls, (all) => [...all, historyOptions(input)]).pipe(
					Effect.map(() => pages.get(historyOptions(input)?.cursor ?? 'start') ?? { messages: [] }),
				),
		})
		const thread = Thread.make({
			ref: testThreadRef,
			currentMessage: makeTestMessage({ messageTs: '100.6', author: humanD }),
			recentMessages: [],
		})
		yield* Effect.gen(function* () {
			const participants = yield* thread.getParticipants()
			assert.deepStrictEqual(participants, [humanD, humanA, humanC])
			assert.deepStrictEqual(yield* Ref.get(calls), [
				{ direction: 'forward' },
				{ direction: 'forward', cursor: 'p1' },
			])
		}).pipe(Effect.provide(Slack.layer.pipe(Layer.provide(provider))))
	}),
)

it.effect('returns only the current author when the thread has no history yet', () =>
	Effect.gen(function* () {
		const provider = stubSlackClientLayer({
			replies: () => Effect.succeed({ messages: [] }),
		})
		const thread = Thread.make({
			ref: testThreadRef,
			currentMessage: makeTestMessage({ messageTs: '100.1', author: humanD }),
			recentMessages: [],
		})
		yield* Effect.gen(function* () {
			assert.deepStrictEqual(yield* thread.getParticipants(), [humanD])
		}).pipe(Effect.provide(Slack.layer.pipe(Layer.provide(provider))))
	}),
)

const historyOptions = (input: MessageHistoryOptions): MessageHistoryOptions | undefined => {
	const options: { -readonly [K in keyof MessageHistoryOptions]: MessageHistoryOptions[K] } = {}
	if (input.limit !== undefined) options.limit = input.limit
	if (input.cursor !== undefined) options.cursor = input.cursor
	if (input.direction !== undefined) options.direction = input.direction
	return Object.keys(options).length === 0 ? undefined : options
}
