import { assert, it } from '@effect/vitest'
import { Effect, Layer, Queue } from 'effect'

import { MessageRef, ThreadId } from '../src/index.js'
import { SlackHistoryInput, SlackRepliesInput } from '../src/Schema.js'
import { Slack } from '../src/Slack.js'
import { SlackClient } from '../src/SlackClient.js'
import { expectTaggedFailure } from './nativeSupport.js'
import {
	makeSlackClientHarness,
	slackJsonResponse,
	testBotToken,
	testChannelId,
	testChannelRef,
	testRootThreadId,
	testRootTs,
	testTeamId,
	type RecordedSlackRequest,
} from './support.js'

const params = (request: RecordedSlackRequest) => Object.fromEntries(request.url.searchParams)

const refs = (messages: ReadonlyArray<{ readonly ref: string }>) => messages.map((message) => message.ref)

interface SlackMessageFixture {
	readonly type: string
	readonly user?: string
	readonly bot_id?: string
	readonly text: string
	readonly ts: string
	readonly thread_ts?: string
}

const reply = (
	ts: string,
	author: { readonly user?: string; readonly bot_id?: string },
	text: string,
): SlackMessageFixture => ({
	type: 'message',
	...author,
	text,
	ts,
	thread_ts: '100.1',
})

const threadOldestFirst: ReadonlyArray<SlackMessageFixture> = [
	reply('100.1', { user: 'U_HUMAN' }, 'root'),
	reply('100.2', { user: 'U_BOT' }, 'our answer'),
	reply('100.3', { bot_id: 'B_OTHER' }, 'another bot'),
	reply('100.4', { user: 'U_HUMAN' }, 'follow-up'),
]

const pageResponse = (
	messages: ReadonlyArray<SlackMessageFixture>,
	extra: { readonly has_more?: boolean; readonly next_cursor?: string } = {},
) =>
	slackJsonResponse(
		JSON.stringify({
			ok: true,
			messages,
			has_more: extra.has_more ?? false,
			response_metadata: extra.next_cursor === undefined ? undefined : { next_cursor: extra.next_cursor },
		}),
	)

const channelNewestFirst: ReadonlyArray<SlackMessageFixture> = [
	{ type: 'message', user: 'U_HUMAN', text: 'third', ts: '99.3', thread_ts: '99.1' },
	{ type: 'message', user: 'U_BOT', text: 'second', ts: '99.2' },
]

const replies = (input: SlackRepliesInput) => Effect.flatMap(SlackClient, (client) => client.replies(input))
const history = (input: SlackHistoryInput) => Effect.flatMap(SlackClient, (client) => client.history(input))

it.effect('fetches forward thread replies oldest-first with the caller cursor and marks bot identities', () =>
	Effect.gen(function* () {
		const harness = yield* makeSlackClientHarness(() => pageResponse(threadOldestFirst, { next_cursor: 'next' }))
		const page = yield* replies(
			SlackRepliesInput.make({
				teamId: testTeamId,
				channelId: testChannelId,
				threadTs: testRootTs,
				limit: 2,
				cursor: 'abc',
				direction: 'forward',
			}),
		).pipe(Effect.provide(harness.layer))
		const request = yield* Queue.take(harness.requests)
		assert.strictEqual(request.method, 'POST')
		assert.strictEqual(request.url.pathname, '/api/conversations.replies')
		assert.strictEqual(request.authorization, `Bearer ${testBotToken}`)
		assert.deepStrictEqual(params(request), { channel: 'C_TEST', ts: '100.1', limit: '2', cursor: 'abc' })
		assert.deepStrictEqual(refs(page.messages), ['100.1', '100.2', '100.3', '100.4'])
		assert.strictEqual(page.nextCursor, 'next')
		assert.deepStrictEqual(
			page.messages.map((message) => message.threadRef.id),
			[testRootThreadId, testRootThreadId, testRootThreadId, testRootThreadId],
		)
		assert.deepStrictEqual(
			page.messages.map((message) => [message.author.userId, message.author.isBot, message.author.isMe]),
			[
				['U_HUMAN', 'unknown', false],
				['U_BOT', true, true],
				['B_OTHER', true, false],
				['U_HUMAN', 'unknown', false],
			],
		)
		assert.strictEqual(yield* Queue.size(harness.requests), 0)
	}),
)

it.effect('treats an empty next_cursor as the end of forward replies', () =>
	Effect.gen(function* () {
		const harness = yield* makeSlackClientHarness(() => pageResponse(threadOldestFirst, { next_cursor: '' }))
		const page = yield* replies(
			SlackRepliesInput.make({
				teamId: testTeamId,
				channelId: testChannelId,
				threadTs: testRootTs,
				direction: 'forward',
			}),
		).pipe(Effect.provide(harness.layer))
		assert.strictEqual(page.nextCursor, undefined)
		assert.deepStrictEqual(params(yield* Queue.take(harness.requests)), {
			channel: 'C_TEST',
			ts: '100.1',
			limit: '100',
		})
	}),
)

it.effect('over-fetches backward replies, returns the newest tail newest-first, and resumes before the cursor', () =>
	Effect.gen(function* () {
		const harness = yield* makeSlackClientHarness((request) =>
			request.url.searchParams.get('latest') === '100.3'
				? pageResponse(threadOldestFirst.slice(0, 2))
				: pageResponse(threadOldestFirst),
		)
		const backward = SlackRepliesInput.make({
			teamId: testTeamId,
			channelId: testChannelId,
			threadTs: testRootTs,
			limit: 2,
		})
		const first = yield* replies(backward).pipe(Effect.provide(harness.layer))
		assert.deepStrictEqual(params(yield* Queue.take(harness.requests)), {
			channel: 'C_TEST',
			ts: '100.1',
			limit: '200',
		})
		assert.deepStrictEqual(refs(first.messages), ['100.4', '100.3'])
		assert.strictEqual(first.nextCursor, '100.3')

		const second = yield* replies({ ...backward, cursor: '100.3' }).pipe(Effect.provide(harness.layer))
		assert.deepStrictEqual(params(yield* Queue.take(harness.requests)), {
			channel: 'C_TEST',
			ts: '100.1',
			limit: '200',
			latest: '100.3',
			inclusive: 'false',
		})
		assert.deepStrictEqual(refs(second.messages), ['100.2', '100.1'])
		assert.strictEqual(second.nextCursor, undefined)
	}),
)

const longThreadOldestFirst: ReadonlyArray<SlackMessageFixture> = Array.from({ length: 7 }, (_, index) =>
	reply(`100.${index + 1}`, { user: 'U_HUMAN' }, `message ${index + 1}`),
)

const longThreadServer = (request: RecordedSlackRequest) => {
	const latest = request.url.searchParams.get('latest')
	const cursor = request.url.searchParams.get('cursor')
	const range =
		latest === null
			? longThreadOldestFirst
			: longThreadOldestFirst.filter((message) => Number(message.ts) < Number(latest))
	const start = cursor === null ? 0 : Number(cursor.slice('index:'.length))
	const page = range.slice(start, start + 3)
	const nextStart = start + 3
	const hasMore = nextStart < range.length
	return hasMore ? pageResponse(page, { has_more: true, next_cursor: `index:${nextStart}` }) : pageResponse(page)
}

it.effect('walks a long thread to its true end before serving the newest-first page', () =>
	Effect.gen(function* () {
		const harness = yield* makeSlackClientHarness(longThreadServer)
		const backward = SlackRepliesInput.make({
			teamId: testTeamId,
			channelId: testChannelId,
			threadTs: testRootTs,
			limit: 2,
		})
		const collected: Array<string> = []
		let cursor: string | undefined
		for (let page = 0; page < 6; page++) {
			const result = yield* replies(cursor === undefined ? backward : { ...backward, cursor }).pipe(
				Effect.provide(harness.layer),
			)
			assert.isAtMost(result.messages.length, 2)
			collected.push(...refs(result.messages))
			if (result.nextCursor === undefined) {
				break
			}
			cursor = result.nextCursor
		}
		assert.deepStrictEqual(collected, ['100.7', '100.6', '100.5', '100.4', '100.3', '100.2', '100.1'])
	}),
)

it.effect('reads channel history before a message newest-first and pages by the oldest ts', () =>
	Effect.gen(function* () {
		const harness = yield* makeSlackClientHarness(() => pageResponse(channelNewestFirst, { has_more: true }))
		const page = yield* history(
			SlackHistoryInput.make({ teamId: testTeamId, channelId: testChannelId, before: testRootTs, limit: 2 }),
		).pipe(Effect.provide(harness.layer))
		const request = yield* Queue.take(harness.requests)
		assert.strictEqual(request.url.pathname, '/api/conversations.history')
		assert.deepStrictEqual(params(request), { channel: 'C_TEST', limit: '2', latest: '100.1', inclusive: 'false' })
		assert.deepStrictEqual(refs(page.messages), ['99.3', '99.2'])
		assert.deepStrictEqual(
			page.messages.map((message) => message.threadRef.id),
			['slack:v1:T_TEST:C_TEST:99.1', 'slack:v1:T_TEST:C_TEST:99.2'],
		)
		assert.strictEqual(page.nextCursor, '99.2')

		const resumed = yield* history(
			SlackHistoryInput.make({
				teamId: testTeamId,
				channelId: testChannelId,
				before: testRootTs,
				cursor: '99.2',
			}),
		).pipe(Effect.provide(harness.layer))
		assert.deepStrictEqual(params(yield* Queue.take(harness.requests)), {
			channel: 'C_TEST',
			limit: '100',
			latest: '99.2',
			inclusive: 'false',
		})
		assert.strictEqual(resumed.nextCursor, '99.2')
	}),
)

it.effect('reads forward channel history between the cursor and the boundary oldest-first', () =>
	Effect.gen(function* () {
		const harness = yield* makeSlackClientHarness(() => pageResponse(channelNewestFirst, { has_more: true }))
		const page = yield* history(
			SlackHistoryInput.make({
				teamId: testTeamId,
				channelId: testChannelId,
				before: testRootTs,
				cursor: '98.9',
				direction: 'forward',
			}),
		).pipe(Effect.provide(harness.layer))
		assert.deepStrictEqual(params(yield* Queue.take(harness.requests)), {
			channel: 'C_TEST',
			limit: '100',
			oldest: '98.9',
			latest: '100.1',
			inclusive: 'false',
		})
		assert.deepStrictEqual(refs(page.messages), ['99.2', '99.3'])
		assert.strictEqual(page.nextCursor, '99.3')
	}),
)

it.effect('maps provider history calls onto replies and history and narrows failures to HistoryFailed', () =>
	Effect.gen(function* () {
		const harness = yield* makeSlackClientHarness((request) =>
			request.url.pathname === '/api/conversations.replies'
				? pageResponse(threadOldestFirst)
				: slackJsonResponse('{"ok":false,"error":"channel_not_found"}'),
		)
		const providerLayer = Slack.layer.pipe(Layer.provide(harness.layer))
		yield* Effect.gen(function* () {
			const provider = yield* Slack
			const threadPage = yield* provider.messages({
				threadId: ThreadId.make(testRootThreadId),
				options: { direction: 'forward', limit: 4 },
			})
			assert.deepStrictEqual(refs(threadPage.messages), ['100.1', '100.2', '100.3', '100.4'])
			assert.deepStrictEqual(params(yield* Queue.take(harness.requests)), {
				channel: 'C_TEST',
				ts: '100.1',
				limit: '4',
			})

			const profileRequests = yield* Queue.takeAll(harness.requests)
			assert.ok(profileRequests.length > 0)
			assert.ok(profileRequests.every((request) => request.url.pathname === '/api/users.info'))

			const error = yield* expectTaggedFailure('HistoryFailed')(
				provider.containerMessages({
					channel: testChannelRef,
					before: MessageRef.make('100.1'),
					options: { direction: 'backward', limit: 2 },
				}),
			)
			assert.strictEqual(error.provider, 'slack')
			assert.deepStrictEqual(params(yield* Queue.take(harness.requests)), {
				channel: 'C_TEST',
				limit: '2',
				latest: '100.1',
				inclusive: 'false',
			})
		}).pipe(Effect.provide(providerLayer))
	}),
)
