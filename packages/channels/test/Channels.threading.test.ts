import { assert, it } from '@effect/vitest'
import { Effect, Fiber, Queue } from 'effect'
import { TestClock } from 'effect/testing'

import {
	Channels,
	Ingress,
	IngressAccepted,
	IngressDropped,
	ThreadId,
	type Message,
	type NormalizedMessage,
	type Thread,
} from '../src/index.ts'
import { ChannelsWithIngressLayer, makeTestAuthor, makeTestNormalizedMessage } from './support.ts'

type Delivery = { readonly threadId: string; readonly messageRef: string }

const deliveryOf = (thread: Thread, message: Message): Delivery => ({
	threadId: thread.ref.id,
	messageRef: message.ref,
})

const rootThreadId = ThreadId.make('slack:v1:T_TEST:C_TEST:100.1')
const otherThreadId = ThreadId.make('slack:v1:T_TEST:C_TEST:200.1')

const expectAccepted = (ingress: Ingress['Service'], message: NormalizedMessage) =>
	Effect.map(ingress.acceptMessage(message), (result) =>
		assert.deepStrictEqual(result, IngressAccepted.make({ idempotencyKey: message.idempotencyKey })),
	)

it.effect('routes the root mention once and every later reply through the subscription', () =>
	Effect.gen(function* () {
		const channels = yield* Channels
		const ingress = yield* Ingress
		const mentions = yield* Queue.unbounded<Delivery>()
		const subscribed = yield* Queue.unbounded<Delivery>()
		yield* channels.onNewMention((thread, message) =>
			thread.subscribe().pipe(Effect.andThen(Queue.offer(mentions, deliveryOf(thread, message))), Effect.asVoid),
		)
		yield* channels.onSubscribedMessage((thread, message) =>
			Queue.offer(subscribed, deliveryOf(thread, message)).pipe(Effect.asVoid),
		)
		const worker = yield* Effect.forkChild(channels.run)

		yield* expectAccepted(ingress, makeTestNormalizedMessage({ messageTs: '100.1', mentioned: true }))
		assert.deepStrictEqual(yield* Queue.take(mentions), { threadId: rootThreadId, messageRef: '100.1' })
		assert.strictEqual(yield* channels.isSubscribed({ threadId: rootThreadId }), true)

		yield* expectAccepted(ingress, makeTestNormalizedMessage({ messageTs: '100.2', rootTs: '100.1' }))
		assert.deepStrictEqual(yield* Queue.take(subscribed), { threadId: rootThreadId, messageRef: '100.2' })

		yield* expectAccepted(
			ingress,
			makeTestNormalizedMessage({
				messageTs: '100.3',
				rootTs: '100.1',
				author: makeTestAuthor({ userId: 'B_OTHER', isBot: true }),
			}),
		)
		assert.deepStrictEqual(yield* Queue.take(subscribed), { threadId: rootThreadId, messageRef: '100.3' })

		yield* expectAccepted(
			ingress,
			makeTestNormalizedMessage({ messageTs: '100.4', rootTs: '100.1', mentioned: true }),
		)
		assert.deepStrictEqual(yield* Queue.take(subscribed), { threadId: rootThreadId, messageRef: '100.4' })
		assert.strictEqual(yield* Queue.size(mentions), 0)

		const ownEcho = makeTestNormalizedMessage({
			messageTs: '100.5',
			rootTs: '100.1',
			author: makeTestAuthor({ userId: 'U_BOT', isBot: true, isMe: true }),
		})
		assert.deepStrictEqual(yield* ingress.acceptMessage(ownEcho), IngressDropped.make({ reason: 'bot' }))

		const unrelated = makeTestNormalizedMessage({ messageTs: '200.2', rootTs: '200.1' })
		assert.deepStrictEqual(yield* ingress.acceptMessage(unrelated), IngressDropped.make({ reason: 'irrelevant' }))

		yield* channels.unsubscribe({ threadId: rootThreadId })
		const afterUnsubscribe = makeTestNormalizedMessage({ messageTs: '100.6', rootTs: '100.1' })
		assert.deepStrictEqual(
			yield* ingress.acceptMessage(afterUnsubscribe),
			IngressDropped.make({ reason: 'irrelevant' }),
		)
		assert.strictEqual(yield* Queue.size(subscribed), 0)
		assert.strictEqual(yield* Queue.size(mentions), 0)

		yield* Fiber.interrupt(worker)
	}).pipe(Effect.provide(ChannelsWithIngressLayer)),
)

it.effect('renews the subscription TTL on accepted subscribed activity and expires idle threads', () =>
	Effect.gen(function* () {
		const channels = yield* Channels
		const ingress = yield* Ingress
		const subscribed = yield* Queue.unbounded<Delivery>()
		yield* channels.onSubscribedMessage((thread, message) =>
			Queue.offer(subscribed, deliveryOf(thread, message)).pipe(Effect.asVoid),
		)
		const worker = yield* Effect.forkChild(channels.run)
		yield* channels.subscribe({ threadId: rootThreadId })
		yield* channels.subscribe({ threadId: otherThreadId })

		yield* TestClock.adjust('20 days')
		yield* expectAccepted(ingress, makeTestNormalizedMessage({ messageTs: '100.2', rootTs: '100.1' }))
		assert.deepStrictEqual(yield* Queue.take(subscribed), { threadId: rootThreadId, messageRef: '100.2' })

		yield* TestClock.adjust('15 days')
		assert.strictEqual(yield* channels.isSubscribed({ threadId: rootThreadId }), true)
		assert.strictEqual(yield* channels.isSubscribed({ threadId: otherThreadId }), false)
		assert.deepStrictEqual(
			yield* ingress.acceptMessage(makeTestNormalizedMessage({ messageTs: '200.2', rootTs: '200.1' })),
			IngressDropped.make({ reason: 'irrelevant' }),
		)

		yield* Fiber.interrupt(worker)
	}).pipe(Effect.provide(ChannelsWithIngressLayer)),
)
