import { NodeCrypto } from '@effect/platform-node'
import { assert, it } from '@effect/vitest'
import { Ingress, IngressAccepted, type NormalizedMessage } from '@humanlayer/channels'
import { Context, Effect, Option, Queue, Schema } from 'effect'
import { HttpRouter } from 'effect/unstable/http'

import { SlackEventCallback, type SlackBotIdentity } from '../src/Schema.ts'
import { normalizeSlackMessage } from '../src/SlackNormalize.ts'
import { appMentionCallback, makeTestIngress, signedSlackRequest, testRouteLayer } from './support.ts'

type EncodedCallback = typeof SlackEventCallback.Encoded

const identity: SlackBotIdentity = { botUserId: 'U_BOT', botId: 'B_OURS' }
const rootThreadId = 'slack:v1:T_TEST:C_TEST:100.1'

const callbackWith = (eventId: string, event: EncodedCallback['event']): EncodedCallback => ({
	type: 'event_callback',
	team_id: 'T_TEST',
	event_id: eventId,
	event_time: 1_788_000_000,
	event,
})

const humanReply = callbackWith('Ev_REPLY', {
	type: 'message',
	user: 'U_HUMAN',
	text: 'a reply without a mention',
	ts: '100.2',
	thread_ts: '100.1',
	channel: 'C_TEST',
	channel_type: 'channel',
})

const otherBotReply = callbackWith('Ev_BOT', {
	type: 'message',
	subtype: 'bot_message',
	bot_id: 'B_OTHER',
	text: 'from another bot',
	ts: '100.3',
	thread_ts: '100.1',
	channel: 'C_TEST',
})

const ownEchoByBotId = callbackWith('Ev_ECHO_BOT', {
	type: 'message',
	bot_id: 'B_OURS',
	text: 'our own reply',
	ts: '100.4',
	thread_ts: '100.1',
	channel: 'C_TEST',
})

const ownEchoByUser = callbackWith('Ev_ECHO_USER', {
	type: 'message',
	user: 'U_BOT',
	text: 'our own reply',
	ts: '100.5',
	thread_ts: '100.1',
	channel: 'C_TEST',
})

const channelJoin = callbackWith('Ev_JOIN', {
	type: 'message',
	subtype: 'channel_join',
	user: 'U_HUMAN',
	text: 'has joined the channel',
	ts: '100.7',
	channel: 'C_TEST',
})

const mentionInThread = callbackWith('Ev_MENTION_REPLY', {
	type: 'app_mention',
	user: 'U_HUMAN',
	text: '<@U_BOT> are you there?',
	ts: '100.8',
	thread_ts: '100.1',
	channel: 'C_TEST',
})

const messageTwinOfMention = callbackWith('Ev_TWIN', {
	type: 'message',
	user: 'U_HUMAN',
	text: '<@U_BOT> hello from Slack',
	ts: '100.1',
	channel: 'C_TEST',
})

const normalize = (callback: EncodedCallback) =>
	Effect.gen(function* () {
		const decoded = yield* Schema.decodeEffect(SlackEventCallback)(callback)
		return yield* normalizeSlackMessage({ callback: decoded, identity })
	})

const normalizeSome = (callback: EncodedCallback) =>
	Effect.map(normalize(callback), (result) => Option.getOrThrow(result))

it.effect('keeps replies in the root thread and leaves mention detection to the event type', () =>
	Effect.gen(function* () {
		const reply = yield* normalizeSome(humanReply)
		assert.strictEqual(reply.thread.ref.id, rootThreadId)
		assert.strictEqual(reply.thread.ref.isNew, false)
		assert.strictEqual(reply.mentioned, false)
		assert.strictEqual(reply.message.ref, '100.2')
		assert.strictEqual(reply.message.author.userId, 'U_HUMAN')
		assert.strictEqual(reply.message.author.isBot, 'unknown')
		assert.strictEqual(reply.message.author.isMe, false)

		const mention = yield* normalizeSome(mentionInThread)
		assert.strictEqual(mention.thread.ref.id, rootThreadId)
		assert.strictEqual(mention.thread.ref.isNew, false)
		assert.strictEqual(mention.mentioned, true)
		assert.strictEqual(mention.message.text, 'are you there?')
	}).pipe(Effect.provide(NodeCrypto.layer)),
)

it.effect('marks other bots as bots and our own echoes as isMe by bot id or bot user id', () =>
	Effect.gen(function* () {
		const otherBot = yield* normalizeSome(otherBotReply)
		assert.strictEqual(otherBot.message.author.isBot, true)
		assert.strictEqual(otherBot.message.author.isMe, false)
		assert.strictEqual(otherBot.message.author.userId, 'B_OTHER')

		const echoByBotId = yield* normalizeSome(ownEchoByBotId)
		assert.strictEqual(echoByBotId.message.author.isMe, true)
		assert.strictEqual(echoByBotId.message.author.isBot, true)

		const echoByUser = yield* normalizeSome(ownEchoByUser)
		assert.strictEqual(echoByUser.message.author.isMe, true)
		assert.strictEqual(echoByUser.message.author.isBot, true)
	}).pipe(Effect.provide(NodeCrypto.layer)),
)

it.effect('drops ineligible message subtypes before ingress', () =>
	Effect.gen(function* () {
		assert.strictEqual(Option.isNone(yield* normalize(channelJoin)), true)
	}).pipe(Effect.provide(NodeCrypto.layer)),
)

it.effect('derives the same idempotency key for the app_mention and message twins of one Slack message', () =>
	Effect.gen(function* () {
		const mention = yield* normalizeSome(appMentionCallback)
		const twin = yield* normalizeSome(messageTwinOfMention)
		assert.strictEqual(twin.idempotencyKey, mention.idempotencyKey)
		assert.strictEqual(mention.mentioned, true)
		assert.strictEqual(twin.mentioned, false)
		const reply = yield* normalizeSome(humanReply)
		assert.notStrictEqual(reply.idempotencyKey, mention.idempotencyKey)
	}).pipe(Effect.provide(NodeCrypto.layer)),
)

it.effect('acknowledges ineligible subtypes at the webhook without touching ingress', () =>
	Effect.gen(function* () {
		const ingress = makeTestIngress({})
		const callback = yield* Schema.decodeEffect(SlackEventCallback)(channelJoin)
		const request = yield* signedSlackRequest(callback)
		const { dispose, handler } = HttpRouter.toWebHandler(testRouteLayer, { disableLogger: true })
		yield* Effect.addFinalizer(() => Effect.promise(dispose))
		const response = yield* Effect.promise(() => handler(request, Context.make(Ingress, ingress)))
		assert.strictEqual(response.status, 200)
	}).pipe(Effect.provide(NodeCrypto.layer)),
)

it.effect('hands a signed human reply to ingress with the root thread id', () =>
	Effect.gen(function* () {
		const accepted = yield* Queue.unbounded<NormalizedMessage>()
		const ingress = makeTestIngress({
			acceptMessage: (message) =>
				Queue.offer(accepted, message).pipe(
					Effect.as(IngressAccepted.make({ idempotencyKey: message.idempotencyKey })),
				),
		})
		const callback = yield* Schema.decodeEffect(SlackEventCallback)(humanReply)
		const request = yield* signedSlackRequest(callback)
		const { dispose, handler } = HttpRouter.toWebHandler(testRouteLayer, { disableLogger: true })
		yield* Effect.addFinalizer(() => Effect.promise(dispose))
		const response = yield* Effect.promise(() => handler(request, Context.make(Ingress, ingress)))
		const normalized = yield* Queue.take(accepted)
		assert.strictEqual(response.status, 200)
		assert.strictEqual(normalized.thread.ref.id, rootThreadId)
		assert.strictEqual(normalized.thread.ref.isNew, false)
		assert.strictEqual(normalized.mentioned, false)
		assert.strictEqual(normalized.message.ref, '100.2')
	}).pipe(Effect.provide(NodeCrypto.layer)),
)
