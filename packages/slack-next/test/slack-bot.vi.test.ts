import * as NodeCrypto from '@effect/platform-node/NodeCrypto'
import { describe, it } from '@effect/vitest'
import {
	Channels,
	ChannelsMemory,
	QueueDeliveryMode,
	makeDeliveryClient,
	type DeliveryContext,
} from '@humanlayer/channels-delivery-next'
import { Clock, Config, Deferred, Effect, Layer, Queue, Redacted, Ref, Schedule } from 'effect'
import { TestClock } from 'effect/testing'
import { FetchHttpClient, HttpRouter, HttpServerRequest } from 'effect/unstable/http'

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

describe('SlackBot.make remote delivery', () => {
	it.live('posts a handed-off delivery\'s result to its thread, retrying Slack without running the callback again', ({ expect }) =>
		Effect.gen(function* () {
			const contexts = yield* Queue.unbounded<DeliveryContext>()
			const posts = yield* Queue.unbounded<SlackPostToThreadRequest>()
			const postFailuresLeft = yield* Ref.make(1)
			const participant = SlackParticipant.make({
				userId: SlackUserId.make('U_ALICE'),
				userName: 'alice',
				fullName: 'Alice Example',
				isBot: false,
				isMe: false,
			})
			const bot = Channels.make({
				namespace: 'slack-bot-test',
				basePath: '/api/channels',
				providers: [
					SlackBot.make({
						signingSecret: Config.succeed(Redacted.make(slackEmulatorSigningSecret)),
						deliveryMode: QueueDeliveryMode.make({}),
						slackApi: Layer.mock(SlackApi, {
							resolveParticipant: () => Effect.succeed(participant),
							postToThread: (request) =>
								Effect.gen(function* () {
									yield* Queue.offer(posts, request)
									if (yield* Ref.modify(postFailuresLeft, (left) => [left > 0, Math.max(0, left - 1)])) {
										return yield* SlackApiError.make({ operation: 'post', message: 'Could not reach Slack' })
									}
									const ref = SlackMessageRef.make({
										teamId: request.thread.teamId,
										channelId: request.thread.channelId,
										messageTs: SlackMessageTs.make('1700000009.000009'),
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
								}),
						}),
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
			expect(response.status).toBe(200)
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
			const receipt = yield* client.complete({ ...target, payload: { markdown: 'The fix is ready.' } })
			expect(receipt.status).toBe('accepted')

			const first = yield* Queue.take(posts)
			const second = yield* Queue.take(posts)
			for (const post of [first, second]) {
				expect(post.thread.threadTs).toBe('1700000001.000001')
				expect(post.content).toEqual({ _tag: 'SlackMarkdownContent', markdown: 'The fix is ready.' })
			}
			const retired = yield* client
				.status(target)
				.pipe(Effect.repeat({ until: ({ stage }) => stage === 'Retired', schedule: Schedule.spaced('20 millis') }))
			expect(retired.output).toEqual([{ operationId: 'outcome', kind: 'PresentOutcome', state: 'Delivered', attempts: 2 }])
			expect(yield* Queue.size(contexts)).toBe(0)
		}),
	)
})
