import { assert, it } from '@effect/vitest'
import { Effect, Ref } from 'effect'

import { ProviderRegistry, Thread, type MessageHistoryOptions, type MessagePage } from '../src/index.ts'
import {
	ChannelsWithIngressLayer,
	makeFakeProvider,
	makeTestAuthor,
	makeTestMessage,
	noCapabilities,
	testThreadRef,
} from './support.ts'

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
		const provider = makeFakeProvider({
			capabilities: {
				...noCapabilities,
				history: { thread: true, channelMessages: false, channelThreads: false },
			},
			messages: (input) =>
				Ref.update(calls, (all) => [...all, input.options]).pipe(
					Effect.map(() => pages.get(input.options?.cursor ?? 'start') ?? { messages: [] }),
				),
		})
		const thread = Thread.make({
			ref: testThreadRef,
			currentMessage: makeTestMessage({ messageTs: '100.6', author: humanD }),
			recentMessages: [],
		})
		yield* Effect.gen(function* () {
			const registry = yield* ProviderRegistry
			yield* registry.register(provider)
			const participants = yield* thread.getParticipants()
			assert.deepStrictEqual(participants, [humanD, humanA, humanC])
			assert.deepStrictEqual(yield* Ref.get(calls), [
				{ direction: 'forward' },
				{ direction: 'forward', cursor: 'p1' },
			])
		}).pipe(Effect.provide(ChannelsWithIngressLayer))
	}),
)

it.effect('returns only the current author when the thread has no history yet', () =>
	Effect.gen(function* () {
		const provider = makeFakeProvider({
			capabilities: {
				...noCapabilities,
				history: { thread: true, channelMessages: false, channelThreads: false },
			},
			messages: () => Effect.succeed({ messages: [] }),
		})
		const thread = Thread.make({
			ref: testThreadRef,
			currentMessage: makeTestMessage({ messageTs: '100.1', author: humanD }),
			recentMessages: [],
		})
		yield* Effect.gen(function* () {
			const registry = yield* ProviderRegistry
			yield* registry.register(provider)
			assert.deepStrictEqual(yield* thread.getParticipants(), [humanD])
		}).pipe(Effect.provide(ChannelsWithIngressLayer))
	}),
)
