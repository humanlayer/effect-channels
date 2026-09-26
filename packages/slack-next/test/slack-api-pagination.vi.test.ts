import { describe, it } from '@effect/vitest'
import { Array as Arr, ConfigProvider, Effect, Layer, Match, Predicate, Queue, Schema } from 'effect'
import { HttpClient, HttpClientRequest, HttpClientResponse } from 'effect/unstable/http'

import { SlackApi } from '../src/SlackApi'
import { SlackApiLiveBase } from '../src/SlackApiLive'
import { SlackChannelId, SlackMessageTs, SlackTeamId } from '../src/SlackIdentity'
import {
	SlackChannelInfo,
	SlackChannelRef,
	slackFileFromMetadata,
	SlackMarkdownContent,
	SlackMessage,
	SlackMessageCount,
	SlackMessageRef,
	SlackParticipant,
	SlackSentMessage,
	SlackThreadRef,
	SlackUserId,
} from '../src/SlackModels'
import { SlackThread } from '../src/SlackThread'
import { slackFileMetadata, slackFileObject } from './slack-file-fixtures'

const teamId = SlackTeamId.make('T_PAGINATION')
const channelId = SlackChannelId.make('C_PAGINATION')
const threadTs = SlackMessageTs.make('1700000010.000000')
const threadRef = SlackThreadRef.make({ teamId, channelId, threadTs, isDm: false })
const channelRef = SlackChannelRef.make({ teamId, channelId, isDm: false })
const content = SlackMarkdownContent.make({ markdown: 'hello channel' })

const alice = SlackParticipant.make({
	userId: SlackUserId.make('U_ALICE'),
	userName: 'alice',
	fullName: 'Alice Example',
	isBot: false,
	isMe: false,
})
const bob = SlackParticipant.make({
	userId: SlackUserId.make('U_BOB'),
	userName: 'bob',
	fullName: 'Bob Example',
	isBot: false,
	isMe: false,
})
const bot = SlackParticipant.make({
	userId: SlackUserId.make('U_BOT'),
	userName: 'helper',
	fullName: 'Helper Bot',
	isBot: true,
	isMe: false,
})
const me = SlackParticipant.make({
	userId: SlackUserId.make('U_ME'),
	userName: 'agent',
	fullName: 'Agent',
	isBot: false,
	isMe: true,
})

const message = (timestamp: string, author = alice) => {
	const ref = SlackMessageRef.make({ teamId, channelId, messageTs: SlackMessageTs.make(timestamp) })
	return SlackMessage.make({ ref, thread: threadRef, author, content, files: [], metadata: {} })
}

const rootMessage = message(threadTs)
const sentMessage = message(threadTs, me)
const sent = SlackSentMessage.make({ ref: sentMessage.ref, message: sentMessage })
const channelInfo = SlackChannelInfo.make({ channel: channelRef, name: 'pagination', memberCount: 4 })

type SlackMessagePage = {
	readonly messages: ReadonlyArray<SlackMessage>
	readonly nextCursor?: string
}

type RecordedThreadPageRequest = {
	thread: SlackThreadRef
	cursor?: string
	limit?: number
}

type RecordedChannelPageRequest = {
	channel: SlackChannelRef
	before?: string
	cursor?: string
	limit?: number
}

const RequestBody = Schema.Struct({
	channel: Schema.optionalKey(Schema.String),
	ts: Schema.optionalKey(Schema.String),
	latest: Schema.optionalKey(Schema.String),
	limit: Schema.optionalKey(Schema.Union([Schema.Finite, Schema.FiniteFromString])),
	cursor: Schema.optionalKey(Schema.String),
	user: Schema.optionalKey(Schema.String),
	text: Schema.optionalKey(Schema.String),
})

const makeHarness = Effect.fn('test.makeSlackApiHarness')(function* () {
	const threadPages = yield* Queue.unbounded<SlackMessagePage>()
	const channelPages = yield* Queue.unbounded<SlackMessagePage>()
	const calls = yield* Queue.unbounded<unknown>()
	const participants = new Map<string, SlackParticipant>(
		[alice, bob, bot, me].map((participant) => [participant.userId, participant]),
	)
	const pageResponse = (request: HttpClientRequest.HttpClientRequest, page: SlackMessagePage) =>
		HttpClientResponse.fromWeb(
			request,
			Response.json({
				ok: true,
				messages: page.messages.map((item) => ({
					type: 'message',
					user: item.author.userId,
					text: Match.value(item.content).pipe(
						Match.discriminatorsExhaustive('_tag')({
							SlackMarkdownContent: (value) => value.markdown,
							SlackPlainTextContent: (value) => value.text,
						}),
					),
					ts: item.ref.messageTs,
					thread_ts: item.thread.threadTs,
					files: Arr.isReadonlyArrayEmpty(item.files) ? undefined : [slackFileObject],
				})),
				response_metadata: { next_cursor: page.nextCursor ?? '' },
			}),
		)
	const httpClient = HttpClient.make((request) =>
		Effect.gen(function* () {
			const webRequest = yield* HttpClientRequest.toWeb(request).pipe(Effect.orDie)
			const url = new URL(webRequest.url)
			const body = yield* (
				webRequest.method === 'GET'
					? Schema.decodeEffect(RequestBody)(Object.fromEntries(url.searchParams))
					: Schema.decodeEffect(Schema.fromJsonString(RequestBody))(
							yield* Effect.promise(() => webRequest.text()),
						)
			).pipe(Effect.orDie)
			const method = url.pathname.split('/').at(-1)
			if (method === 'users.info') {
				const participant = participants.get(body.user ?? '')
				if (participant === undefined) return yield* Effect.die(new Error(`Unknown user ${body.user}`))
				return HttpClientResponse.fromWeb(
					request,
					Response.json({
						ok: true,
						user: {
							id: participant.userId,
							name: participant.userName,
							real_name: participant.fullName,
							is_bot: participant.isBot,
						},
					}),
				)
			}
			if (method === 'conversations.replies') {
				const recordedRequest: RecordedThreadPageRequest = {
					thread: threadRef,
					limit: body.limit,
				}
				if (Predicate.isNotUndefined(body.cursor)) recordedRequest.cursor = body.cursor
				yield* Queue.offer(calls, {
					operation: 'listThreadMessagesPage',
					request: recordedRequest,
				})
				return pageResponse(request, yield* Queue.take(threadPages))
			}
			if (method === 'conversations.history') {
				const recordedRequest: RecordedChannelPageRequest = {
					channel: channelRef,
					before: body.latest,
					limit: body.limit,
				}
				if (Predicate.isNotUndefined(body.cursor)) recordedRequest.cursor = body.cursor
				yield* Queue.offer(calls, {
					operation: 'listChannelMessagesPage',
					request: recordedRequest,
				})
				return pageResponse(request, yield* Queue.take(channelPages))
			}
			if (method === 'chat.postMessage') {
				yield* Queue.offer(calls, { operation: 'postToChannel', request: { channel: channelRef, content } })
				return HttpClientResponse.fromWeb(
					request,
					Response.json({
						ok: true,
						channel: channelId,
						ts: rootMessage.ref.messageTs,
						message: { ts: rootMessage.ref.messageTs, user: me.userId, text: body.text },
					}),
				)
			}
			if (method === 'conversations.info') {
				yield* Queue.offer(calls, { operation: 'getChannelInfo', request: { channel: channelRef } })
				return HttpClientResponse.fromWeb(
					request,
					Response.json({ ok: true, channel: { id: channelId, name: 'pagination', num_members: 4 } }),
				)
			}
			return yield* Effect.die(new Error(`Unexpected Slack method ${method}`))
		}),
	)
	const dependencies = Layer.mergeAll(
		Layer.succeed(HttpClient.HttpClient, httpClient),
		ConfigProvider.layer(
			ConfigProvider.fromUnknown({ SLACK_BOT_TOKEN: 'xoxb-test', SLACK_BOT_USER_ID: me.userId }),
		),
	)
	const layer = SlackApiLiveBase.pipe(Layer.provide(dependencies))
	return { threadPages, channelPages, calls, layer }
})

describe('SlackApi paginated thread layer', () => {
	it.effect('resolves a reacted reply to its root thread timestamp', ({ expect }) =>
		Effect.gen(function* () {
			const harness = yield* makeHarness()
			const reply = message('1700000011.000000')
			yield* Queue.offer(harness.threadPages, { messages: [reply] })

			const resolved = yield* Effect.flatMap(SlackApi, (api) =>
				api.resolveReactionThread({ message: reply.ref }),
			).pipe(Effect.provide(harness.layer))

			expect(resolved).toBe(threadTs)
			expect(yield* Queue.take(harness.calls)).toEqual({
				operation: 'listThreadMessagesPage',
				request: { thread: threadRef, limit: 1 },
			})
		}),
	)

	it.effect('follows every thread cursor and returns messages oldest-first', ({ expect }) =>
		Effect.gen(function* () {
			const harness = yield* makeHarness()
			const oldest = message('1700000001.000000')
			const middle = message('1700000002.000000')
			const newest = message('1700000003.000000')
			yield* Queue.offer(harness.threadPages, { messages: [newest, middle], nextCursor: 'thread-page-2' })
			yield* Queue.offer(harness.threadPages, { messages: [oldest], nextCursor: '' })

			const messages = yield* Effect.flatMap(SlackApi, (api) =>
				api.listThreadMessages({ thread: threadRef }),
			).pipe(Effect.provide(harness.layer))
			expect(messages).toEqual([oldest, middle, newest])
			expect(yield* Queue.take(harness.calls)).toEqual({
				operation: 'listThreadMessagesPage',
				request: { thread: threadRef, limit: 15 },
			})
			expect(yield* Queue.take(harness.calls)).toEqual({
				operation: 'listThreadMessagesPage',
				request: { thread: threadRef, cursor: 'thread-page-2', limit: 15 },
			})
		}),
	)

	it.effect('keeps file metadata on API-loaded messages', ({ expect }) =>
		Effect.gen(function* () {
			const harness = yield* makeHarness()
			const plain = message('1700000001.000000')
			const base = message('1700000002.000000')
			const withFile = SlackMessage.make({
				ref: base.ref,
				thread: base.thread,
				author: base.author,
				content: base.content,
				files: [slackFileFromMetadata(teamId, slackFileMetadata)],
				metadata: base.metadata,
			})
			yield* Queue.offer(harness.threadPages, { messages: [withFile, plain] })

			const messages = yield* Effect.flatMap(SlackApi, (api) =>
				api.listThreadMessages({ thread: threadRef }),
			).pipe(Effect.provide(harness.layer))
			expect(messages).toEqual([plain, withFile])
			expect(messages[1]?.files[0]?.downloadUrl).toBe(slackFileMetadata.url_private_download)
		}),
	)

	it.effect('follows channel cursors as needed, caps the result, and returns chronological order', ({ expect }) =>
		Effect.gen(function* () {
			const harness = yield* makeHarness()
			const second = message('1700000002.000000')
			const third = message('1700000003.000000')
			const fourth = message('1700000004.000000')
			const fifth = message('1700000005.000000')
			yield* Queue.offer(harness.channelPages, { messages: [fifth, fourth], nextCursor: 'channel-page-2' })
			yield* Queue.offer(harness.channelPages, { messages: [third, second] })
			const count = SlackMessageCount.make(3)

			const messages = yield* Effect.flatMap(SlackApi, (api) =>
				api.listChannelMessagesBeforeThread({ thread: threadRef, count }),
			).pipe(Effect.provide(harness.layer))
			expect(messages).toEqual([third, fourth, fifth])
			expect(yield* Queue.take(harness.calls)).toEqual({
				operation: 'listChannelMessagesPage',
				request: { channel: channelRef, before: threadTs, limit: count },
			})
			expect(yield* Queue.take(harness.calls)).toEqual({
				operation: 'listChannelMessagesPage',
				request: { channel: channelRef, before: threadTs, cursor: 'channel-page-2', limit: count },
			})
		}),
	)

	it.effect('derives participants from complete history and preserves first-seen user order', ({ expect }) =>
		Effect.gen(function* () {
			const harness = yield* makeHarness()
			yield* Queue.offer(harness.threadPages, {
				messages: [message('1700000005.000000', me), message('1700000004.000000', bob)],
				nextCursor: 'participants-page-2',
			})
			yield* Queue.offer(harness.threadPages, {
				messages: [
					message('1700000003.000000', alice),
					message('1700000002.000000', bot),
					message('1700000001.000000', bob),
				],
			})

			const participants = yield* Effect.flatMap(SlackApi, (api) =>
				api.listParticipants({ thread: threadRef }),
			).pipe(Effect.provide(harness.layer))
			expect(participants).toEqual([bob, alice])
		}),
	)

	it.effect('backs SlackChannel behavior with the captured channel and thread anchor', ({ expect }) =>
		Effect.gen(function* () {
			const harness = yield* makeHarness()
			const prior = message('1700000009.000000')
			yield* Queue.offer(harness.channelPages, { messages: [prior] })
			const thread = SlackThread.make({ ref: threadRef, mailboxKey: 'mailbox:pagination' })

			expect(thread.channel.ref).toEqual(channelRef)
			expect(
				yield* thread
					.listChannelMessagesBeforeThread(SlackMessageCount.make(1))
					.pipe(Effect.provide(harness.layer)),
			).toEqual([prior])
			expect(yield* thread.channel.post(content).pipe(Effect.provide(harness.layer))).toEqual(sent)
			expect(yield* thread.channel.fetchInfo().pipe(Effect.provide(harness.layer))).toEqual(channelInfo)

			expect(yield* Queue.take(harness.calls)).toEqual({
				operation: 'listChannelMessagesPage',
				request: { channel: channelRef, before: threadTs, limit: 1 },
			})
			expect(yield* Queue.take(harness.calls)).toEqual({
				operation: 'postToChannel',
				request: { channel: channelRef, content },
			})
			expect(yield* Queue.take(harness.calls)).toEqual({
				operation: 'getChannelInfo',
				request: { channel: channelRef },
			})
		}),
	)
})
