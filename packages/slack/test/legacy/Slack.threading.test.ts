import { assert, it } from '@effect/vitest'
import { Effect, Fiber, Queue } from 'effect'
import { TestClock } from 'effect/testing'

import {
	SlackSubscriptions,
	SlackIngress,
	IngressAccepted,
	IngressDropped,
	ThreadId,
	type Message,
	type NormalizedMessage,
	type Thread,
} from '../../src/index'
import { ingressLayer, runnerOptions, makeTestAuthor, makeTestNormalizedMessage } from './support'

type Delivery = { readonly threadId: string; readonly messageRef: string }

const deliveryOf = (thread: Thread, message: Message): Delivery => ({
	threadId: thread.ref.id,
	messageRef: message.ref,
})

const rootThreadId = ThreadId.make('slack:v1:T_TEST:C_TEST:100.1')
const otherThreadId = ThreadId.make('slack:v1:T_TEST:C_TEST:200.1')

const expectAccepted = (ingress: SlackIngress['Service'], message: NormalizedMessage) =>
	Effect.map(ingress.acceptMessage(message), (result) =>
		assert.deepStrictEqual(result, IngressAccepted.make({ idempotencyKey: message.idempotencyKey })),
	)

it.effect('routes the root mention once and every later reply through the subscription', () =>
	Effect.gen(function* () {
		const mentions = yield* Queue.unbounded<Delivery>()
		const subscribed = yield* Queue.unbounded<Delivery>()
		yield* Effect.gen(function* () {
			const channels = yield* SlackSubscriptions
			const ingress = yield* SlackIngress
			const worker = yield* Effect.forkChild(ingress.run(runnerOptions))

			yield* expectAccepted(ingress, makeTestNormalizedMessage({ messageTs: '100.1', mentioned: true }))
			yield* TestClock.adjust('20 millis')
			assert.deepStrictEqual(yield* Queue.take(mentions), { threadId: rootThreadId, messageRef: '100.1' })
			assert.strictEqual(yield* channels.isSubscribed({ threadId: rootThreadId }), true)

			yield* expectAccepted(ingress, makeTestNormalizedMessage({ messageTs: '100.2', rootTs: '100.1' }))
			yield* TestClock.adjust('20 millis')
			yield* TestClock.adjust('20 millis')
			assert.deepStrictEqual(yield* Queue.take(subscribed), { threadId: rootThreadId, messageRef: '100.2' })

			yield* expectAccepted(
				ingress,
				makeTestNormalizedMessage({
					messageTs: '100.3',
					rootTs: '100.1',
					author: makeTestAuthor({ userId: 'B_OTHER', isBot: true }),
				}),
			)
			yield* TestClock.adjust('20 millis')
			assert.deepStrictEqual(yield* Queue.take(subscribed), { threadId: rootThreadId, messageRef: '100.3' })

			yield* expectAccepted(
				ingress,
				makeTestNormalizedMessage({ messageTs: '100.4', rootTs: '100.1', mentioned: true }),
			)
			yield* TestClock.adjust('20 millis')
			assert.deepStrictEqual(yield* Queue.take(subscribed), { threadId: rootThreadId, messageRef: '100.4' })
			assert.strictEqual(yield* Queue.size(mentions), 0)

			const ownEcho = makeTestNormalizedMessage({
				messageTs: '100.5',
				rootTs: '100.1',
				author: makeTestAuthor({ userId: 'U_BOT', isBot: true, isMe: true }),
			})
			assert.deepStrictEqual(yield* ingress.acceptMessage(ownEcho), IngressDropped.make({ reason: 'bot' }))

			const unrelated = makeTestNormalizedMessage({ messageTs: '200.2', rootTs: '200.1' })
			assert.deepStrictEqual(
				yield* ingress.acceptMessage(unrelated),
				IngressDropped.make({ reason: 'irrelevant' }),
			)

			yield* channels.unsubscribe({ threadId: rootThreadId })
			const afterUnsubscribe = makeTestNormalizedMessage({ messageTs: '100.6', rootTs: '100.1' })
			assert.deepStrictEqual(
				yield* ingress.acceptMessage(afterUnsubscribe),
				IngressDropped.make({ reason: 'irrelevant' }),
			)
			assert.strictEqual(yield* Queue.size(subscribed), 0)
			assert.strictEqual(yield* Queue.size(mentions), 0)

			yield* Fiber.interrupt(worker)
		}).pipe(
			Effect.provide(
				ingressLayer({
					onNewMention: [
						{
							id: 'mention',
							handler: ({ thread, message }) =>
								thread
									.subscribe()
									.pipe(
										Effect.andThen(Queue.offer(mentions, deliveryOf(thread, message))),
										Effect.asVoid,
									),
						},
					],
					onSubscribedMessage: [
						{
							id: 'subscribed',
							handler: ({ thread, message }) =>
								Queue.offer(subscribed, deliveryOf(thread, message)).pipe(Effect.asVoid),
						},
					],
				}),
			),
		)
	}),
)

it.effect('renews the subscription TTL on accepted subscribed activity and expires idle threads', () =>
	Effect.gen(function* () {
		const channels = yield* SlackSubscriptions
		const ingress = yield* SlackIngress

		yield* channels.subscribe({ threadId: rootThreadId })
		yield* channels.subscribe({ threadId: otherThreadId })

		yield* TestClock.adjust('20 days')
		yield* expectAccepted(ingress, makeTestNormalizedMessage({ messageTs: '100.2', rootTs: '100.1' }))

		yield* TestClock.adjust('15 days')
		assert.strictEqual(yield* channels.isSubscribed({ threadId: rootThreadId }), true)
		assert.strictEqual(yield* channels.isSubscribed({ threadId: otherThreadId }), false)
		assert.deepStrictEqual(
			yield* ingress.acceptMessage(makeTestNormalizedMessage({ messageTs: '200.2', rootTs: '200.1' })),
			IngressDropped.make({ reason: 'irrelevant' }),
		)
	}).pipe(Effect.provide(ingressLayer({ onSubscribedMessage: [{ id: 'subscribed', handler: () => Effect.void }] }))),
)
