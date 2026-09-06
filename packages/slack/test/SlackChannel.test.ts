import { assert, it } from '@effect/vitest'
import { DateTime, Effect, Layer, Queue, Schema } from 'effect'

import { MarkdownContent, ThreadId, UserId } from '../src/index.ts'
import { SlackChannelId, SlackChannelInfoInput, SlackListThreadsInput } from '../src/Schema.ts'
import { Slack } from '../src/Slack.ts'
import { SlackClient } from '../src/SlackClient.ts'
import { slackChannelRef } from '../src/SlackThreadId.ts'
import { expectTaggedFailure } from './nativeSupport.ts'
import { testConnectionStoreLayer } from './support.ts'
import {
	makeSlackClientHarness,
	slackJsonResponse,
	testChannelId,
	testChannelRef,
	testRootThreadId,
	testTeamId,
	unknownTenantCredentialsLayer,
	type RecordedSlackRequest,
} from './support.ts'

const params = (request: RecordedSlackRequest) => Object.fromEntries(request.url.searchParams)

const PostBody = Schema.Struct({
	channel: Schema.String,
	thread_ts: Schema.optionalKey(Schema.String),
	text: Schema.String,
})

const channelHistoryNewestFirst = [
	{
		type: 'message',
		user: 'U_A',
		text: 'root three',
		ts: '300.1',
		thread_ts: '300.1',
		reply_count: 2,
		latest_reply: '300.3',
	},
	{ type: 'message', user: 'U_B', text: 'no replies here', ts: '250.1' },
	{
		type: 'message',
		user: 'U_C',
		text: 'root two',
		ts: '200.1',
		thread_ts: '200.1',
		reply_count: 1,
		latest_reply: '200.2',
	},
	{ type: 'message', user: 'U_D', text: 'root one', ts: '100.1', thread_ts: '100.1', reply_count: 5 },
]

const threadListResponse = () =>
	slackJsonResponse(
		JSON.stringify({
			ok: true,
			messages: channelHistoryNewestFirst,
			has_more: true,
			response_metadata: { next_cursor: 'more' },
		}),
	)

const channelInfoResponse = (channel: {
	readonly id: string
	readonly name?: string
	readonly is_im?: boolean
	readonly num_members?: number
}) => slackJsonResponse(JSON.stringify({ ok: true, channel }))

const providerLayerFor = <E>(harness: { readonly layer: Layer.Layer<SlackClient, E> }) =>
	Slack.layer.pipe(Layer.provide(testConnectionStoreLayer), Layer.provide(harness.layer))

it.effect('posts a channel root message without thread_ts and returns a new thread reference', () =>
	Effect.gen(function* () {
		const harness = yield* makeSlackClientHarness(() =>
			slackJsonResponse('{"ok":true,"channel":"C_TEST","ts":"500.1","message":{"user":"U_BOT"}}'),
		)
		const sent = yield* Effect.flatMap(Slack, (provider) =>
			provider.postToChannel({
				channel: testChannelRef,
				content: MarkdownContent.make({ markdown: 'hello channel' }),
			}),
		).pipe(Effect.provide(providerLayerFor(harness)))
		const request = yield* Queue.take(harness.requests)
		const body = yield* Schema.decodeEffect(Schema.fromJsonString(PostBody))(request.body)
		assert.strictEqual(request.method, 'POST')
		assert.strictEqual(request.url.pathname, '/api/chat.postMessage')
		assert.deepStrictEqual(body, { channel: 'C_TEST', text: 'hello channel' })
		assert.strictEqual(sent.ref.threadId, 'slack:v1:T_TEST:C_TEST:500.1')
		assert.strictEqual(sent.ref.messageRef, '500.1')
		assert.strictEqual(sent.ref.provider, 'slack')
		assert.deepStrictEqual(sent.ref.degraded, [])
		assert.strictEqual(sent.message.threadRef.isNew, true)
		assert.deepStrictEqual(sent.message.threadRef.channel, testChannelRef)
		assert.deepStrictEqual(sent.message.author, {
			userId: UserId.make('U_BOT'),
			userName: 'U_BOT',
			fullName: 'U_BOT',
			isBot: true,
			isMe: true,
		})
		assert.strictEqual(yield* Queue.size(harness.requests), 0)
	}),
)

it.effect('decodes conversations.info into ChannelInfo and flags DMs', () =>
	Effect.gen(function* () {
		const harness = yield* makeSlackClientHarness((request) =>
			request.url.searchParams.get('channel') === 'D_TEST'
				? channelInfoResponse({ id: 'D_TEST', is_im: true })
				: channelInfoResponse({ id: 'C_TEST', name: 'general', num_members: 12 }),
		)
		const info = yield* Effect.flatMap(SlackClient, (client) =>
			client.channelInfo(SlackChannelInfoInput.make({ teamId: testTeamId, channelId: testChannelId })),
		).pipe(Effect.provide(harness.layer))
		const request = yield* Queue.take(harness.requests)
		assert.strictEqual(request.method, 'POST')
		assert.strictEqual(request.url.pathname, '/api/conversations.info')
		assert.deepStrictEqual(params(request), { channel: 'C_TEST' })
		assert.deepStrictEqual(info, { channel: testChannelRef, name: 'general', memberCount: 12 })

		const dmChannelId = SlackChannelId.make('D_TEST')
		const dm = yield* Effect.flatMap(SlackClient, (client) =>
			client.channelInfo(SlackChannelInfoInput.make({ teamId: testTeamId, channelId: dmChannelId })),
		).pipe(Effect.provide(harness.layer))
		assert.deepStrictEqual(dm, { channel: slackChannelRef(testTeamId, dmChannelId) })
		assert.strictEqual(dm.channel.isDm, true)
	}),
)

it.effect('lists channel threads from conversations.history roots with replies, over-fetching by three', () =>
	Effect.gen(function* () {
		const harness = yield* makeSlackClientHarness(threadListResponse)
		const page = yield* Effect.flatMap(SlackClient, (client) =>
			client.listThreads(SlackListThreadsInput.make({ teamId: testTeamId, channelId: testChannelId, limit: 3 })),
		).pipe(Effect.provide(harness.layer))
		const request = yield* Queue.take(harness.requests)
		assert.strictEqual(request.url.pathname, '/api/conversations.history')
		assert.deepStrictEqual(params(request), { channel: 'C_TEST', limit: '9' })
		assert.deepStrictEqual(
			page.threads.map((summary) => summary.thread.id),
			['slack:v1:T_TEST:C_TEST:300.1', 'slack:v1:T_TEST:C_TEST:200.1', 'slack:v1:T_TEST:C_TEST:100.1'],
		)
		assert.deepStrictEqual(
			page.threads.map((summary) => summary.thread.isNew),
			[false, false, false],
		)
		assert.deepStrictEqual(
			page.threads.map((summary) => summary.rootMessage.ref),
			['300.1', '200.1', '100.1'],
		)
		assert.deepStrictEqual(
			page.threads.map((summary) => summary.replyCount),
			[2, 1, 5],
		)
		assert.deepStrictEqual(
			page.threads.map((summary) =>
				summary.lastActivityAt === undefined ? undefined : DateTime.toEpochMillis(summary.lastActivityAt),
			),
			[300_300, 200_200, undefined],
		)
		assert.strictEqual(page.nextCursor, '100.1')

		const limited = yield* Effect.flatMap(SlackClient, (client) =>
			client.listThreads(
				SlackListThreadsInput.make({ teamId: testTeamId, channelId: testChannelId, limit: 1, cursor: '100.1' }),
			),
		).pipe(Effect.provide(harness.layer))
		assert.deepStrictEqual(params(yield* Queue.take(harness.requests)), {
			channel: 'C_TEST',
			limit: '3',
			latest: '100.1',
			inclusive: 'false',
		})
		assert.deepStrictEqual(
			limited.threads.map((summary) => summary.thread.id),
			['slack:v1:T_TEST:C_TEST:300.1'],
		)
		assert.strictEqual(limited.nextCursor, '300.1')
	}),
)

it.effect('caps the thread listing over-fetch at 200 and defaults the page size to 50', () =>
	Effect.gen(function* () {
		const harness = yield* makeSlackClientHarness(threadListResponse)
		yield* Effect.flatMap(SlackClient, (client) =>
			client.listThreads(SlackListThreadsInput.make({ teamId: testTeamId, channelId: testChannelId })),
		).pipe(Effect.provide(harness.layer))
		assert.deepStrictEqual(params(yield* Queue.take(harness.requests)), { channel: 'C_TEST', limit: '150' })
		yield* Effect.flatMap(SlackClient, (client) =>
			client.listThreads(
				SlackListThreadsInput.make({ teamId: testTeamId, channelId: testChannelId, limit: 100 }),
			),
		).pipe(Effect.provide(harness.layer))
		assert.deepStrictEqual(params(yield* Queue.take(harness.requests)), { channel: 'C_TEST', limit: '200' })
	}),
)

it.effect('passes provider thread listing options through and maps failures to HistoryFailed', () =>
	Effect.gen(function* () {
		const harness = yield* makeSlackClientHarness((request) =>
			request.url.searchParams.get('latest') === 'broken'
				? slackJsonResponse('{"ok":false,"error":"invalid_ts_latest"}')
				: threadListResponse(),
		)
		yield* Effect.gen(function* () {
			const provider = yield* Slack
			const page = yield* provider.channelThreads({ channel: testChannelRef, options: { limit: 2, cursor: 'c' } })
			assert.deepStrictEqual(params(yield* Queue.take(harness.requests)), {
				channel: 'C_TEST',
				limit: '6',
				latest: 'c',
				inclusive: 'false',
			})
			assert.deepStrictEqual(
				page.threads.map((summary) => summary.thread.id),
				['slack:v1:T_TEST:C_TEST:300.1', 'slack:v1:T_TEST:C_TEST:200.1'],
			)
			assert.strictEqual(page.nextCursor, '200.1')

			const error = yield* expectTaggedFailure('HistoryFailed')(
				provider.channelThreads({ channel: testChannelRef, options: { cursor: 'broken' } }),
			)
			assert.strictEqual(error.provider, 'slack')
		}).pipe(Effect.provide(providerLayerFor(harness)))
	}),
)

const busyChannelNewestFirst = [
	{ type: 'message', user: 'U_A', text: 'root five', ts: '500.1', thread_ts: '500.1', reply_count: 1 },
	{ type: 'message', user: 'U_B', text: 'chatter', ts: '450.1' },
	{ type: 'message', user: 'U_C', text: 'root four', ts: '400.1', thread_ts: '400.1', reply_count: 2 },
	{ type: 'message', user: 'U_D', text: 'root three', ts: '300.1', thread_ts: '300.1', reply_count: 3 },
	{ type: 'message', user: 'U_E', text: 'root two', ts: '200.1', thread_ts: '200.1', reply_count: 1 },
	{ type: 'message', user: 'U_F', text: 'chatter', ts: '150.1' },
	{ type: 'message', user: 'U_G', text: 'root one', ts: '100.1', thread_ts: '100.1', reply_count: 4 },
]

const busyChannelWindow = (request: RecordedSlackRequest) => {
	const latest = request.url.searchParams.get('latest')
	const cursor = request.url.searchParams.get('cursor')
	const limit = Number(request.url.searchParams.get('limit') ?? '100')
	const source =
		latest !== null
			? busyChannelNewestFirst.filter((message) => Number(message.ts) < Number(latest))
			: cursor !== null
				? busyChannelNewestFirst.slice(6)
				: busyChannelNewestFirst
	const page = source.slice(0, limit)
	const hasMore = source.length > page.length
	return slackJsonResponse(
		JSON.stringify({
			ok: true,
			messages: page,
			has_more: hasMore,
			response_metadata: hasMore ? { next_cursor: 'opaque-w2' } : undefined,
		}),
	)
}

const collectAllThreads = (limit: number) =>
	Effect.gen(function* () {
		const client = yield* SlackClient
		const ids: Array<string> = []
		let cursor: string | undefined
		for (let page = 0; page < 6; page++) {
			const result = yield* client.listThreads(
				cursor === undefined
					? SlackListThreadsInput.make({ teamId: testTeamId, channelId: testChannelId, limit })
					: SlackListThreadsInput.make({ teamId: testTeamId, channelId: testChannelId, limit, cursor }),
			)
			assert.isAtMost(result.threads.length, limit)
			for (const summary of result.threads) {
				ids.push(summary.thread.id)
			}
			if (result.nextCursor === undefined) {
				return ids
			}
			cursor = result.nextCursor
		}
		return ids
	})

it.effect('returns every thread across pages when a window holds more roots than the page limit', () =>
	Effect.gen(function* () {
		const harness = yield* makeSlackClientHarness(busyChannelWindow)
		const ids = yield* collectAllThreads(2).pipe(Effect.provide(harness.layer))
		assert.deepStrictEqual(ids, [
			'slack:v1:T_TEST:C_TEST:500.1',
			'slack:v1:T_TEST:C_TEST:400.1',
			'slack:v1:T_TEST:C_TEST:300.1',
			'slack:v1:T_TEST:C_TEST:200.1',
			'slack:v1:T_TEST:C_TEST:100.1',
		])
	}),
)

it.effect('titles thread info with the channel name and maps lookup failures to ThreadGone and ChannelGone', () =>
	Effect.gen(function* () {
		const harness = yield* makeSlackClientHarness((request) =>
			request.url.searchParams.get('channel') === 'C_GONE'
				? slackJsonResponse('{"ok":false,"error":"channel_not_found"}')
				: channelInfoResponse({ id: 'C_TEST', name: 'general' }),
		)
		yield* Effect.gen(function* () {
			const provider = yield* Slack
			const threadId = ThreadId.make(testRootThreadId)
			const info = yield* provider.info({ threadId })
			assert.strictEqual(info.title, 'general')
			assert.strictEqual(info.thread.id, testRootThreadId)
			assert.strictEqual(info.thread.isNew, false)
			assert.deepStrictEqual(params(yield* Queue.take(harness.requests)), { channel: 'C_TEST' })

			const goneThreadId = ThreadId.make('slack:v1:T_TEST:C_GONE:100.1')
			const threadGone = yield* expectTaggedFailure('ThreadGone')(provider.info({ threadId: goneThreadId }))
			assert.strictEqual(threadGone.threadId, goneThreadId)

			const goneChannel = slackChannelRef(testTeamId, SlackChannelId.make('C_GONE'))
			const channelGone = yield* expectTaggedFailure('ChannelGone')(
				provider.channelInfo({ channel: goneChannel }),
			)
			assert.strictEqual(channelGone.channelId, goneChannel.id)
		}).pipe(Effect.provide(providerLayerFor(harness)))
	}),
)

it.effect('narrows non-gone metadata failures to MetadataFailed instead of lying with ThreadGone', () =>
	Effect.gen(function* () {
		const ratelimited = yield* makeSlackClientHarness(() => slackJsonResponse('{"ok":false,"error":"ratelimited"}'))
		yield* Effect.gen(function* () {
			const provider = yield* Slack
			const infoError = yield* expectTaggedFailure('MetadataFailed')(
				provider.info({ threadId: ThreadId.make(testRootThreadId) }),
			)
			assert.strictEqual(infoError.provider, 'slack')
			const channelError = yield* expectTaggedFailure('MetadataFailed')(
				provider.channelInfo({ channel: testChannelRef }),
			)
			assert.strictEqual(channelError.provider, 'slack')
		}).pipe(Effect.provide(providerLayerFor(ratelimited)))

		const unknownTenant = yield* makeSlackClientHarness(
			() => slackJsonResponse('{"ok":true}'),
			unknownTenantCredentialsLayer,
		)
		yield* Effect.gen(function* () {
			const provider = yield* Slack
			const error = yield* expectTaggedFailure('MetadataFailed')(
				provider.info({ threadId: ThreadId.make(testRootThreadId) }),
			)
			assert.strictEqual(error.message, 'unknown Slack workspace')
		}).pipe(Effect.provide(providerLayerFor(unknownTenant)))
	}),
)
