import * as NodeCrypto from '@effect/platform-node/NodeCrypto'
import { describe, it } from '@effect/vitest'
import {
	Channels,
	ChannelsMemory,
	DeliveryActivity,
	MessageId,
	QueueDeliveryMode,
	makeDeliveryClient,
	type DeliveryContext,
} from '@humanlayer/channels-delivery'
import { Clock, Config, Deferred, Effect, Layer, Queue, Redacted, Ref, Schedule } from 'effect'
import { TestClock } from 'effect/testing'
import { FetchHttpClient, HttpRouter, HttpServerRequest } from 'effect/http'

import {
	SlackApi,
	SlackApiError,
	SlackAppMentionEvent,
	SlackBot,
	SlackChannelId,
	SlackMessage,
	SlackMessageRef,
	SlackMessageTs,
	SlackParticipant,
	SlackPlainTextContent,
	SlackThreadRef,
	SlackTeamId,
	SlackUserId,
	type SlackPostToThreadRequest,
} from '../src'
import { SlackAppMentionEnvelope } from '../src/SlackWebhookSchemas'
import { signedSlackInput, slackEmulatorSigningSecret } from './fixtures'

const mention = SlackAppMentionEnvelope.make({
	type: 'event_callback',
	team_id: SlackTeamId.make('T_BOT_TEST'),
	event_id: 'Ev-slack-bot-mention',
	event_time: 0,
	event: SlackAppMentionEvent.make({
		type: 'app_mention',
		user: SlackUserId.make('U_ALICE'),
		text: '<@U_BOT> hello',
		ts: SlackMessageTs.make('1700000001.000001'),
		channel: SlackChannelId.make('C_BOT_TEST'),
	}),
})

describe('SlackBot.make', () => {
	it.effect('carries a signed mention through Channels.make to onNewMention', ({ expect }) =>
		Effect.gen(function* () {
			const mentioned = yield* Deferred.make<{ readonly threadTs: string; readonly subscribed: boolean }>()
			const bot = Channels.make({
				namespace: 'slack-bot-test',
				basePath: '/api/channels',
				providers: [
					SlackBot.make({
						signingSecret: Config.succeed(Redacted.make(slackEmulatorSigningSecret)),
						deliveryMode: QueueDeliveryMode.make({}),
						slackApi: Layer.mock(SlackApi, {
							resolveParticipant: (request) =>
								Effect.succeed(
									SlackParticipant.make({
										userId: SlackUserId.make(request.userId ?? 'U_ALICE'),
										userName: 'alice',
										fullName: 'Alice Example',
										isBot: false,
										isMe: false,
									}),
								),
						}),
						handlers: {
							onNewMention: (event) =>
								Effect.gen(function* () {
									yield* event.thread.subscribe()
									const subscribed = yield* event.thread.isSubscribed()
									yield* Deferred.succeed(mentioned, {
										threadTs: event.thread.ref.threadTs,
										subscribed,
									})
								}),
						},
					}),
				],
				eventProcessing: { concurrency: 1, leaseMs: 30_000 },
				storage: ChannelsMemory.make({ polling: { intervalMs: 1_000 } }),
			})
			const fetch = yield* HttpRouter.toHttpEffect(bot.routes).pipe(Effect.provide(NodeCrypto.layer))
			const signed = yield* signedSlackInput(slackEmulatorSigningSecret, mention)

			const response = yield* fetch.pipe(
				Effect.provideService(
					HttpServerRequest.HttpServerRequest,
					HttpServerRequest.fromWeb(
						new Request('http://localhost/api/channels/integrations/slack/webhook', {
							method: 'POST',
							headers: signed.headers,
							body: new TextDecoder().decode(signed.body),
						}),
					),
				),
			)
			yield* TestClock.adjust(1_000)

			expect(response.status).toBe(200)
			expect(yield* Deferred.await(mentioned)).toEqual({ threadTs: '1700000001.000001', subscribed: true })
		}),
	)
})

const participant = SlackParticipant.make({
	userId: SlackUserId.make('U_ALICE'),
	userName: 'alice',
	fullName: 'Alice Example',
	isBot: false,
	isMe: false,
})

/** Slack's answer to a post: a message in the same thread, with the given timestamp. */
const postedMessage = (request: SlackPostToThreadRequest, messageTs: string) => {
	const ref = SlackMessageRef.make({
		teamId: request.thread.teamId,
		channelId: request.thread.channelId,
		messageTs: SlackMessageTs.make(messageTs),
	})
	return {
		ref,
		message: SlackMessage.make({
			ref,
			thread: SlackThreadRef.make(request.thread),
			author: participant,
			content: SlackPlainTextContent.make({ text: 'posted' }),
			files: [],
			metadata: {},
		}),
	}
}

/**
 * Start a bot over `slackApi` whose mention callback hands off, send it a signed mention, and return
 * the delivery the callback handed off with a generated delivery client for the bot.
 */
const handOffMention = (slackApi: Layer.Layer<SlackApi>) =>
	Effect.gen(function* () {
		const contexts = yield* Queue.unbounded<DeliveryContext>()
		const bot = Channels.make({
			namespace: 'slack-bot-test',
			basePath: '/api/channels',
			providers: [
				SlackBot.make({
					signingSecret: Config.succeed(Redacted.make(slackEmulatorSigningSecret)),
					deliveryMode: QueueDeliveryMode.make({}),
					slackApi,
					handlers: {
						onNewMention: (_event, delivery) =>
							Queue.offer(contexts, delivery).pipe(Effect.andThen(delivery.handoff())),
					},
				}),
			],
			eventProcessing: { concurrency: 1, leaseMs: 30_000 },
			storage: ChannelsMemory.make({ polling: { intervalMs: 10 } }),
		})
		const started = yield* Effect.promise(() => bot.start(NodeCrypto.layer, bot.deliveryApi))
		yield* Effect.addFinalizer(() => Effect.promise(started.stop))
		const now = yield* Clock.currentTimeMillis
		const signed = yield* signedSlackInput(slackEmulatorSigningSecret, mention, String(Math.floor(now / 1_000)))
		const response = yield* Effect.promise(() =>
			started.handle(
				new Request('http://localhost/api/channels/integrations/slack/webhook', {
					method: 'POST',
					headers: signed.headers,
					body: new TextDecoder().decode(signed.body),
				}),
			),
		)
		const delivery = yield* Queue.take(contexts)
		const client = yield* makeDeliveryClient({ baseUrl: 'http://localhost', basePath: '/api/channels' }).pipe(
			Effect.provide(
				FetchHttpClient.layer.pipe(
					Layer.provide(
						Layer.succeed(FetchHttpClient.Fetch, (input, init) => started.handle(new Request(input, init))),
					),
				),
			),
		)
		const target = { deliveryId: delivery.deliveryId, accessToken: delivery.accessToken }
		const awaitRetired = client
			.status(target)
			.pipe(Effect.repeat({ until: ({ stage }) => stage === 'Retired', schedule: Schedule.spaced('20 millis') }))
		return { response, contexts, client, target, awaitRetired }
	})

describe('SlackBot.make remote delivery', () => {
	it.live('posts a handed-off delivery\'s result to its thread, retrying Slack without running the callback again', ({ expect }) =>
		Effect.gen(function* () {
			const posts = yield* Queue.unbounded<SlackPostToThreadRequest>()
			const postFailuresLeft = yield* Ref.make(1)
			const { response, contexts, client, target, awaitRetired } = yield* handOffMention(
				Layer.mock(SlackApi, {
					resolveParticipant: () => Effect.succeed(participant),
					postToThread: (request) =>
						Effect.gen(function* () {
							yield* Queue.offer(posts, request)
							if (yield* Ref.modify(postFailuresLeft, (left) => [left > 0, Math.max(0, left - 1)])) {
								return yield* SlackApiError.make({ operation: 'post', message: 'Could not reach Slack' })
							}
							return postedMessage(request, '1700000009.000009')
						}),
				}),
			)
			expect(response.status).toBe(200)
			const receipt = yield* client.complete({ ...target, payload: { markdown: 'The fix is ready.' } })
			expect(receipt.status).toBe('accepted')

			const first = yield* Queue.take(posts)
			const second = yield* Queue.take(posts)
			for (const post of [first, second]) {
				expect(post.thread.threadTs).toBe('1700000001.000001')
				expect(post.content).toEqual({ _tag: 'SlackMarkdownContent', markdown: 'The fix is ready.' })
			}
			const retired = yield* awaitRetired
			expect(retired.output).toEqual([{ operationId: 'outcome', kind: 'PresentOutcome', state: 'Delivered', attempts: 2, hadAmbiguousAttempt: false }])
			expect(yield* Queue.size(contexts)).toBe(0)
		}),
	)

	it.live('posts, edits, and removes a progress message in the thread, then posts the result', ({ expect }) =>
		Effect.gen(function* () {
			const calls = yield* Queue.unbounded<string>()
			const posted = yield* Ref.make(0)
			const { client, target, awaitRetired } = yield* handOffMention(
				Layer.mock(SlackApi, {
					resolveParticipant: () => Effect.succeed(participant),
					postToThread: (request) =>
						Effect.gen(function* () {
							const count = yield* Ref.updateAndGet(posted, (n) => n + 1)
							yield* Queue.offer(calls, `post ${JSON.stringify(request.content)}`)
							return postedMessage(request, `1700000009.00000${count}`)
						}),
					updateMessage: ({ message, content }) =>
						Queue.offer(calls, `update ${message.messageTs} ${JSON.stringify(content)}`).pipe(Effect.asVoid),
					deleteMessage: ({ message }) => Queue.offer(calls, `delete ${message.messageTs}`).pipe(Effect.asVoid),
				}),
			)
			const messageId = MessageId.make('progress')
			yield* client.messages.create({ ...target, message: { messageId, markdown: 'Running tests…' } })
			yield* client.messages.update({ ...target, messageId, message: { markdown: 'Tests passed.' } })
			yield* client.messages.delete({ ...target, messageId })
			yield* client.complete({ ...target, payload: { markdown: 'Done.' } })
			const retired = yield* awaitRetired
			expect(retired.output.map(({ kind, state }) => `${kind}:${state}`)).toEqual([
				'CreateMessage:Delivered',
				'UpdateMessage:Delivered',
				'DeleteMessage:Delivered',
				'PresentOutcome:Delivered',
			])
			const seen = yield* Queue.takeAll(calls)
			expect(Array.from(seen)).toEqual([
				'post {"_tag":"SlackMarkdownContent","markdown":"Running tests…"}',
				'update 1700000009.000001 {"_tag":"SlackMarkdownContent","markdown":"Tests passed."}',
				'delete 1700000009.000001',
				'post {"_tag":"SlackMarkdownContent","markdown":"Done."}',
			])
		}),
	)

	it.live('shows Working as the thread status, then the result clears it', ({ expect }) =>
		Effect.gen(function* () {
			const calls = yield* Queue.unbounded<string>()
			const { client, target, awaitRetired } = yield* handOffMention(
				Layer.mock(SlackApi, {
					resolveParticipant: () => Effect.succeed(participant),
					setThreadStatus: ({ status }) => Queue.offer(calls, `status ${status}`).pipe(Effect.asVoid),
					clearThreadStatus: () => Queue.offer(calls, 'clear').pipe(Effect.asVoid),
				}),
			)
			yield* client.activity.set({ ...target, activity: DeliveryActivity.cases.Working.make({ message: 'Reading logs' }) })
			expect(yield* Queue.take(calls)).toBe('status Reading logs')
			yield* client.complete(target)
			const retired = yield* awaitRetired
			expect(retired.activity).toBeUndefined()
			expect(retired.output.map(({ kind, state }) => `${kind}:${state}`)).toEqual([
				'SetActivity:Delivered',
				'PresentOutcome:Delivered',
			])
			expect(yield* Queue.take(calls)).toBe('clear')
		}),
	)
})
