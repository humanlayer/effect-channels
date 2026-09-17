import { NodeCrypto } from '@effect/platform-node'
import { assert, it } from '@effect/vitest'
import { Effect, Fiber, Layer, Option, Queue, Schema } from 'effect'

import { SlackApiError } from '../src/Errors'
import {
	IdempotencyKey,
	SlackIngress,
	SlackSubscriptions,
	IngressAccepted,
	IngressDropped,
	Message,
	MessageRef,
	MessageEvent,
	NormalizedMessage,
	ProviderName,
	TenantId,
	Thread,
	ThreadId,
	UserId,
} from '../src/index'
import { SlackChannelId, SlackEventCallback, type SlackBotIdentity } from '../src/Schema'
import { Slack } from '../src/Slack'
import { SlackClient } from '../src/SlackClient'
import { normalizeSlackMessage } from '../src/SlackNormalize'
import { expectTaggedFailure, nativeIngressLayer, nativeMailbox, nativeRunner } from './nativeSupport'
import { testConnectionStoreLayer } from './support'
import { makeSlackClientHarness, makeStubSlackClient, slackJsonResponse } from './support'

const identity: SlackBotIdentity = { botUserId: 'U_BOT', botId: 'B_BOT' }

const normalize = (channelType: 'im' | 'mpim', channel: string) =>
	Effect.gen(function* () {
		const callback = yield* Schema.decodeEffect(SlackEventCallback)({
			type: 'event_callback',
			team_id: 'T_TEST',
			event_id: `Ev_${channelType}`,
			event_time: 1_788_000_000,
			event: {
				type: 'message',
				user: 'U_HUMAN',
				text: `hello ${channelType}`,
				ts: '100.1',
				channel,
				channel_type: channelType,
			},
		})
		return Option.getOrThrow(yield* normalizeSlackMessage({ callback, identity }))
	}).pipe(Effect.provide(NodeCrypto.layer))

it.effect('normalizes IM and MPIM messages as direct-message threads', () =>
	Effect.gen(function* () {
		const im = yield* normalize('im', 'D_TEST')
		const mpim = yield* normalize('mpim', 'G_MPIM')
		assert.strictEqual(im.thread.ref.id, 'slack:v1:T_TEST:im:D_TEST:100.1')
		assert.strictEqual(im.directMessageThread?.id, 'slack:v1:T_TEST:im:D_TEST')
		assert.strictEqual(mpim.thread.ref.id, 'slack:v1:T_TEST:mpim:G_MPIM:100.1')
		assert.strictEqual(mpim.thread.ref.channel.isDm, true)
	}),
)

it.effect('opens a proactive DM as a conversation-scoped Thread', () =>
	Effect.gen(function* () {
		const provider = yield* Slack
		const thread = yield* provider.openDM({
			provider: ProviderName.make('slack'),
			tenant: TenantId.make('T_TEST'),
			user: {
				userId: UserId.make('U_HUMAN'),
				userName: 'human',
				fullName: 'Human',
				isBot: false,
				isMe: false,
			},
		})
		assert.strictEqual(thread.ref.id, 'slack:v1:T_TEST:im:D_OPENED')
		assert.strictEqual(thread.isDM, true)
	}).pipe(
		Effect.provide(
			Slack.layer.pipe(
				Layer.provide(testConnectionStoreLayer),
				Layer.provide(
					Layer.succeed(
						SlackClient,
						makeStubSlackClient({ openDM: () => Effect.succeed(SlackChannelId.make('D_OPENED')) }),
					),
				),
			),
		),
	),
)

it.effect('reports openDM failures without fabricating a ThreadId', () =>
	Effect.gen(function* () {
		const provider = yield* Slack
		const error = yield* expectTaggedFailure('DirectMessageOpenFailed')(
			provider.openDM({
				provider: ProviderName.make('slack'),
				tenant: TenantId.make('T_TEST'),
				user: {
					userId: UserId.make('U_HUMAN'),
					userName: 'human',
					fullName: 'Human',
					isBot: false,
					isMe: false,
				},
			}),
		)
		assert.strictEqual(error.tenant, 'T_TEST')
		assert.strictEqual(error.userId, 'U_HUMAN')
		assert.strictEqual('threadId' in error, false)
	}).pipe(
		Effect.provide(
			Slack.layer.pipe(
				Layer.provide(testConnectionStoreLayer),
				Layer.provide(
					Layer.succeed(
						SlackClient,
						makeStubSlackClient({
							openDM: () =>
								Effect.fail(SlackApiError.make({ operation: 'conversations.open', code: 'failed' })),
						}),
					),
				),
			),
		),
	),
)

it.effect('preserves IM and MPIM identities in provider-backed history', () =>
	Effect.gen(function* () {
		const harness = yield* makeSlackClientHarness(() =>
			slackJsonResponse(
				JSON.stringify({
					ok: true,
					messages: [{ user: 'U_HUMAN', text: 'history', ts: '100.1' }],
					has_more: false,
				}),
			),
		)
		const program = Effect.gen(function* () {
			const provider = yield* Slack
			const im = yield* provider.messages({ threadId: ThreadId.make('slack:v1:T_TEST:im:D_TEST') })
			const mpim = yield* provider.messages({ threadId: ThreadId.make('slack:v1:T_TEST:mpim:G_TEST:100.1') })
			assert.strictEqual(im.messages[0]?.threadRef.id, 'slack:v1:T_TEST:im:D_TEST')
			assert.strictEqual(im.messages[0]?.threadRef.channel.isDm, true)
			assert.strictEqual(mpim.messages[0]?.threadRef.id, 'slack:v1:T_TEST:mpim:G_TEST:100.1')
			assert.strictEqual(mpim.messages[0]?.threadRef.channel.isDm, true)
		})
		yield* program.pipe(
			Effect.provide(Slack.layer.pipe(Layer.provide(testConnectionStoreLayer), Layer.provide(harness.layer))),
		)
	}),
)

it.effect('routes a DM exactly once before subscription and mention routes and suppresses own replies', () =>
	Effect.gen(function* () {
		const direct = yield* Queue.unbounded<string>()
		const mentions = yield* Queue.unbounded<string>()
		const subscribed = yield* Queue.unbounded<string>()
		const layer = nativeIngressLayer({
			onDirectMessage: [
				{ id: 'direct', handler: (event) => Queue.offer(direct, event.thread.ref.id).pipe(Effect.asVoid) },
			],
			onNewMention: [
				{ id: 'mention', handler: (event) => Queue.offer(mentions, event.thread.ref.id).pipe(Effect.asVoid) },
			],
			onSubscribedMessage: [
				{
					id: 'subscribed',
					handler: (event) => Queue.offer(subscribed, event.thread.ref.id).pipe(Effect.asVoid),
				},
			],
		})
		yield* Effect.gen(function* () {
			const ingress = yield* SlackIngress
			const subscriptions = yield* SlackSubscriptions
			const normalized = yield* normalize('im', 'D_TEST')
			const conversationThreadId = normalized.directMessageThread?.id
			if (conversationThreadId === undefined) return yield* Effect.die('expected a proactive DM bridge identity')
			yield* subscriptions.subscribe({ threadId: conversationThreadId })
			assert.deepStrictEqual(
				yield* ingress.acceptMessage(NormalizedMessage.make({ ...normalized, mentioned: true })),
				IngressAccepted.make({ idempotencyKey: normalized.idempotencyKey }),
			)
			const worker = yield* Effect.forkChild(ingress.run(nativeRunner))
			assert.strictEqual(yield* Queue.take(direct), conversationThreadId)
			assert.strictEqual(yield* Queue.size(mentions), 0)
			assert.strictEqual(yield* Queue.size(subscribed), 0)

			const ownAuthor = {
				...normalized.message.author,
				userId: UserId.make('U_BOT'),
				isMe: true,
				isBot: true as const,
			}
			const ownMessage = Message.make({
				ref: MessageRef.make('100.2'),
				threadRef: normalized.message.threadRef,
				text: normalized.message.text,
				markdown: normalized.message.markdown,
				author: ownAuthor,
				metadata: normalized.message.metadata,
				attachments: normalized.message.attachments,
				raw: normalized.message.raw,
			})
			const own = NormalizedMessage.make({
				...normalized,
				idempotencyKey: IdempotencyKey.make(`evt_${'1'.repeat(32)}`),
				message: ownMessage,
				thread: Thread.make({
					ref: normalized.thread.ref,
					currentMessage: ownMessage,
					recentMessages: [ownMessage],
				}),
			})
			assert.deepStrictEqual(yield* ingress.acceptMessage(own), IngressDropped.make({ reason: 'bot' }))
			yield* Fiber.interrupt(worker)
		}).pipe(Effect.provide(layer))
	}),
)

it.effect('pins a replayed DM to its original route when the proactive subscription changes', () =>
	Effect.gen(function* () {
		const calls = yield* Queue.unbounded<MessageEvent>()
		const layer = nativeIngressLayer({
			onDirectMessage: [{ id: 'direct', handler: (event) => Queue.offer(calls, event).pipe(Effect.asVoid) }],
		})
		yield* Effect.gen(function* () {
			const ingress = yield* SlackIngress
			const subscriptions = yield* SlackSubscriptions
			const first = yield* normalize('im', 'D_TEST')
			const proactive = first.directMessageThread
			if (proactive === undefined) return yield* Effect.die('expected proactive DM identity')
			yield* ingress.acceptMessage(first)
			yield* subscriptions.subscribe({ threadId: proactive.id })
			yield* ingress.acceptMessage(first)
			assert.strictEqual((yield* nativeMailbox('direct', first))?.state.pending.length, 1)
			const next = NormalizedMessage.make({
				...first,
				idempotencyKey: IdempotencyKey.make(`evt_${'2'.repeat(32)}`),
			})
			yield* ingress.acceptMessage(next)
			yield* subscriptions.unsubscribe({ threadId: proactive.id })
			yield* ingress.acceptMessage(next)
			const bridged = NormalizedMessage.make({ ...next, thread: Thread.fromRef(proactive) })
			assert.strictEqual((yield* nativeMailbox('direct', bridged))?.state.pending.length, 1)
			const worker = yield* ingress.run(nativeRunner).pipe(Effect.forkChild)
			const delivered = [yield* Queue.take(calls), yield* Queue.take(calls)]
			for (const event of delivered) {
				const expected = event.idempotencyKey === first.idempotencyKey ? first.thread.ref.id : proactive.id
				assert.strictEqual(event.thread.ref.id, expected)
				assert.strictEqual(event.message.threadRef.id, expected)
				assert.strictEqual(event.thread.currentMessage?.threadRef.id, expected)
				assert.deepStrictEqual(
					event.thread.recentMessages.map((message) => message.threadRef.id),
					[expected],
				)
				assert.strictEqual(event.delivery._tag, 'DirectMessageDelivery')
			}
			assert.strictEqual(yield* Queue.size(calls), 0)
			yield* Fiber.interrupt(worker)
		}).pipe(Effect.provide(layer))
	}),
)
