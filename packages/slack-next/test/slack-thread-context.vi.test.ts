import { describe, it } from '@effect/vitest'
import {
	MailboxSubscriptionAlreadyExistsResult,
	MailboxSubscriptionCreatedResult,
	MailboxSubscriptionsMemory,
} from '@humanlayer/channels-delivery-next'
import { Effect, Layer, Option, Queue, Stream } from 'effect'

import { SlackApi } from '../src/SlackApi'
import { SlackChannelId, SlackMessageTs, SlackTeamId } from '../src/SlackIdentity'
import {
	SlackChannelInfo,
	SlackMarkdownContent,
	SlackMessage,
	SlackMessageCount,
	SlackMessageRef,
	SlackParticipant,
	SlackPlainTextContent,
	SlackReaction,
	SlackSentMessage,
	SlackThreadInfo,
	SlackThreadRef,
	SlackUserId,
} from '../src/SlackModels'
import { MarkdownTextChunk, PlanUpdateChunk, TaskUpdateChunk } from '../src/SlackStreamChunk'
import { SlackChannelHistoryUnavailable, SlackThread } from '../src/SlackThread'

const teamId = SlackTeamId.make('T_CONTEXT')
const channelId = SlackChannelId.make('C_CONTEXT')
const threadTs = SlackMessageTs.make('1700000000.000001')
const threadRef = SlackThreadRef.make({ teamId, channelId, threadTs, isDm: false })
const thread = SlackThread.make({ ref: threadRef, mailboxKey: 'mailbox:context' })
const count = SlackMessageCount.make(3)
const content = SlackMarkdownContent.make({ markdown: '**hello**' })
const participant = SlackParticipant.make({
	userId: SlackUserId.make('U_ALICE'),
	userName: 'alice',
	fullName: 'Alice Example',
	isBot: false,
	isMe: false,
})
const rootMessageRef = SlackMessageRef.make({ teamId, channelId, messageTs: threadTs })
const arbitraryMessageRef = SlackMessageRef.make({
	teamId,
	channelId,
	messageTs: SlackMessageTs.make('1700000001.000001'),
})
const message = SlackMessage.make({
	ref: rootMessageRef,
	thread: threadRef,
	author: participant,
	content: SlackPlainTextContent.make({ text: 'hello' }),
	files: [],
	metadata: { source: 'test', ordinal: 1 },
})
const sent = SlackSentMessage.make({ ref: rootMessageRef, message })
const threadInfo = SlackThreadInfo.make({ thread: threadRef, title: 'Context thread' })
const channelInfo = SlackChannelInfo.make({
	channel: { teamId, channelId, isDm: false },
	name: 'context',
	memberCount: 2,
})

const unexpected = (operation: string) => Effect.die(new Error(`Unexpected SlackApi.${operation} call`))

const makeApi = (
	calls: Queue.Queue<unknown>,
	overrides: Partial<typeof SlackApi.Service> = {},
): typeof SlackApi.Service => ({
	listParticipants: (request) =>
		Queue.offer(calls, { operation: 'listParticipants', request }).pipe(Effect.as([participant])),
	listThreadMessages: (request) =>
		Queue.offer(calls, { operation: 'listThreadMessages', request }).pipe(Effect.as([message])),
	listChannelMessagesBeforeThread: (request) =>
		Queue.offer(calls, { operation: 'listChannelMessagesBeforeThread', request }).pipe(Effect.as([message])),
	postToThread: (request) => Queue.offer(calls, { operation: 'postToThread', request }).pipe(Effect.as(sent)),
	postToChannel: (request) => Queue.offer(calls, { operation: 'postToChannel', request }).pipe(Effect.as(sent)),
	startTyping: (request) => Queue.offer(calls, { operation: 'startTyping', request }).pipe(Effect.asVoid),
	stream: (requestedThread, chunks) =>
		Stream.runCollect(chunks).pipe(
			Effect.flatMap((collected) =>
				Queue.offer(calls, { operation: 'stream', thread: requestedThread, chunks: Array.from(collected) }),
			),
			Effect.as(sent),
		),
	addReaction: (request) => Queue.offer(calls, { operation: 'addReaction', request }).pipe(Effect.asVoid),
	removeReaction: (request) => Queue.offer(calls, { operation: 'removeReaction', request }).pipe(Effect.asVoid),
	resolveParticipant: () => unexpected('resolveParticipant'),
	getMessage: () => unexpected('getMessage'),
	resolveReactionThread: () => unexpected('resolveReactionThread'),
	getThreadInfo: (request) => Queue.offer(calls, { operation: 'getThreadInfo', request }).pipe(Effect.as(threadInfo)),
	getChannelInfo: (request) =>
		Queue.offer(calls, { operation: 'getChannelInfo', request }).pipe(Effect.as(channelInfo)),
	uploadFileToChannel: () => unexpected('uploadFileToChannel'),
	uploadFileToThread: () => unexpected('uploadFileToThread'),
	downloadFile: () => unexpected('downloadFile'),
	downloadFileBytes: () => unexpected('downloadFileBytes'),
	...overrides,
})

describe('SlackThread context API', () => {
	it.effect('delegates only the actual operation arguments', ({ expect }) =>
		Effect.gen(function* () {
			const calls = yield* Queue.unbounded<unknown>()
			const layer = Layer.succeed(SlackApi, makeApi(calls))

			expect(yield* thread.listParticipants().pipe(Effect.provide(layer))).toEqual([participant])
			expect(yield* Queue.take(calls)).toEqual({ operation: 'listParticipants', request: { thread: threadRef } })

			expect(yield* thread.listMessages().pipe(Effect.provide(layer))).toEqual([message])
			expect(yield* Queue.take(calls)).toEqual({
				operation: 'listThreadMessages',
				request: { thread: threadRef },
			})

			expect(yield* thread.listChannelMessagesBeforeThread(count).pipe(Effect.provide(layer))).toEqual([message])
			expect(yield* Queue.take(calls)).toEqual({
				operation: 'listChannelMessagesBeforeThread',
				request: { thread: threadRef, count },
			})

			expect(yield* thread.post(content).pipe(Effect.provide(layer))).toEqual(sent)
			expect(yield* Queue.take(calls)).toEqual({
				operation: 'postToThread',
				request: { thread: threadRef, content },
			})

			expect(yield* thread.startTyping().pipe(Effect.provide(layer))).toBeUndefined()
			expect(yield* Queue.take(calls)).toEqual({ operation: 'startTyping', request: { thread: threadRef } })

			expect(yield* thread.fetchThreadInfo().pipe(Effect.provide(layer))).toEqual(threadInfo)
			expect(yield* Queue.take(calls)).toEqual({ operation: 'getThreadInfo', request: { thread: threadRef } })

			expect(yield* thread.fetchChannelInfo().pipe(Effect.provide(layer))).toEqual(channelInfo)
			expect(yield* Queue.take(calls)).toEqual({
				operation: 'getChannelInfo',
				request: { channel: thread.channel.ref },
			})
		}),
	)

	it.effect('rejects DM channel history before calling SlackApi', ({ expect }) =>
		Effect.gen(function* () {
			const calls = yield* Queue.unbounded<unknown>()
			const dmRef = SlackThreadRef.make({ ...threadRef, channelId: SlackChannelId.make('D_CONTEXT'), isDm: true })
			const dmThread = SlackThread.make({ ref: dmRef, mailboxKey: 'mailbox:dm-context' })
			const api = makeApi(calls, {
				listChannelMessagesBeforeThread: () => unexpected('listChannelMessagesBeforeThread'),
			})

			const error = yield* dmThread
				.listChannelMessagesBeforeThread(count)
				.pipe(Effect.provideService(SlackApi, api), Effect.flip)

			expect(error).toEqual(new SlackChannelHistoryUnavailable({ thread: dmRef }))
			expect(yield* Queue.poll(calls)).toEqual(Option.none())
		}),
	)

	it.effect('uses the message internal ref for reactions', ({ expect }) =>
		Effect.gen(function* () {
			const calls = yield* Queue.unbounded<unknown>()
			const layer = Layer.succeed(SlackApi, makeApi(calls))
			const reaction = SlackReaction.make('eyes')
			const arbitraryMessage = SlackMessage.make({
				ref: arbitraryMessageRef,
				thread: message.thread,
				author: message.author,
				content: message.content,
				files: message.files,
				metadata: message.metadata,
			})

			yield* arbitraryMessage.addReaction(reaction).pipe(Effect.provide(layer))
			yield* arbitraryMessage.removeReaction(reaction).pipe(Effect.provide(layer))

			expect(yield* Queue.take(calls)).toEqual({
				operation: 'addReaction',
				request: { message: arbitraryMessageRef, reaction },
			})
			expect(yield* Queue.take(calls)).toEqual({
				operation: 'removeReaction',
				request: { message: arbitraryMessageRef, reaction },
			})
		}),
	)

	it.effect('passes every Slack stream chunk variant through unchanged', ({ expect }) =>
		Effect.gen(function* () {
			const calls = yield* Queue.unbounded<unknown>()
			const chunks = [
				MarkdownTextChunk.make({ text: 'working' }),
				TaskUpdateChunk.make({ id: 'task-1', title: 'Inspect', status: 'in_progress', details: 'reading' }),
				PlanUpdateChunk.make({ title: 'Finish implementation' }),
			]

			expect(
				yield* thread
					.stream(Stream.fromIterable(chunks))
					.pipe(Effect.provide(Layer.succeed(SlackApi, makeApi(calls)))),
			).toEqual(sent)
			expect(yield* Queue.take(calls)).toEqual({
				operation: 'stream',
				thread: threadRef,
				chunks,
			})
		}),
	)

	it.effect('tracks deterministic subscription transitions', ({ expect }) =>
		Effect.gen(function* () {
			expect(yield* thread.isSubscribed()).toBe(false)
			expect(yield* thread.subscribe()).toEqual(MailboxSubscriptionCreatedResult.make({}))
			expect(yield* thread.subscribe()).toEqual(MailboxSubscriptionAlreadyExistsResult.make({}))
			expect(yield* thread.isSubscribed()).toBe(true)
			expect(yield* thread.unsubscribe()).toBeUndefined()
			expect(yield* thread.unsubscribe()).toBeUndefined()
			expect(yield* thread.isSubscribed()).toBe(false)
		}).pipe(Effect.provide(MailboxSubscriptionsMemory)),
	)
})
