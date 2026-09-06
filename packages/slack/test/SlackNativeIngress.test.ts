import { NodeCrypto } from '@effect/platform-node'
import { assert, it } from '@effect/vitest'
import { MailboxStore, MailboxStoreError } from '@humanlayer/channels-delivery'
import { layer as memory } from '@humanlayer/channels-delivery/memory'
import { Clock, Context, Deferred, Effect, Fiber, Layer, Queue, Ref, Schema } from 'effect'
import { TestClock } from 'effect/testing'
import { HttpRouter } from 'effect/unstable/http'

import { IngressAccepted, MessageEvent, SlackEventCallback, SlackIngress, SlackSubscriptions } from '../src/index.ts'
import { nativeIngressLayer, nativeMailbox, nativeMessage, nativeRunner } from './nativeSupport.ts'
import { appMentionCallback, signedSlackRequest, signSlackBody, testRouteLayer } from './support.ts'

it.effect('rejects partial fan-out, repairs on retry, and keeps mention ownership after subscription changes', () =>
	Effect.gen(function* () {
		yield* TestClock.setTime(yield* Clock.currentTimeMillis.pipe(TestClock.withLive))
		const calls = yield* Queue.unbounded<string>()
		const storage = Layer.effect(
			MailboxStore,
			Effect.gen(function* () {
				const store = yield* MailboxStore
				const failSecond = yield* Ref.make(true)
				return MailboxStore.of({
					loadMailbox: store.loadMailbox,
					commitMailbox: (input) =>
						Effect.gen(function* () {
							if (input.key.includes('second') && (yield* Ref.getAndSet(failSecond, false))) {
								return yield* MailboxStoreError.make({ operation: 'commit' })
							}
							return yield* store.commitMailbox(input)
						}),
				})
			}),
		).pipe(Layer.provideMerge(memory({ maxMailboxes: 100 })))
		const services = nativeIngressLayer(
			{
				onNewMention: ['first', 'second'].map((id) => ({
					id,
					handler: () => Queue.offer(calls, id).pipe(Effect.asVoid),
				})),
				onSubscribedMessage: [
					{ id: 'subscribed', handler: () => Queue.offer(calls, 'wrong-owner').pipe(Effect.asVoid) },
				],
			},
			storage,
		)
		yield* Effect.gen(function* () {
			const ingress = yield* SlackIngress
			const context = Context.make(SlackIngress, ingress).pipe(Context.add(Clock.Clock, yield* Clock.Clock))
			const web = HttpRouter.toWebHandler(testRouteLayer, { disableLogger: true })
			yield* Effect.addFinalizer(() => Effect.promise(web.dispose))
			const callback = yield* Schema.decodeEffect(SlackEventCallback)(appMentionCallback)
			const deliver = Effect.flatMap(signedSlackRequest(callback), (request) =>
				Effect.promise(() => web.handler(request, context)),
			)
			assert.strictEqual((yield* deliver).status, 503)
			assert.strictEqual((yield* nativeMailbox('first', nativeMessage('a')))?.state.pending.length, 1)
			assert.strictEqual(yield* nativeMailbox('second', nativeMessage('a')), undefined)
			const subscriptions = yield* SlackSubscriptions
			yield* subscriptions.subscribe({ threadId: nativeMessage('a').thread.ref.id })
			assert.strictEqual((yield* deliver).status, 200)
			assert.strictEqual((yield* deliver).status, 200)
			assert.strictEqual((yield* nativeMailbox('first', nativeMessage('a')))?.state.pending.length, 1)
			assert.strictEqual((yield* nativeMailbox('second', nativeMessage('a')))?.state.pending.length, 1)
			assert.strictEqual(yield* nativeMailbox('subscribed', nativeMessage('a')), undefined)
			const worker = yield* ingress.run(nativeRunner).pipe(Effect.forkChild)
			assert.deepStrictEqual([yield* Queue.take(calls), yield* Queue.take(calls)].sort(), ['first', 'second'])
			yield* TestClock.adjust(10)
			assert.strictEqual(yield* Queue.size(calls), 0)
			yield* Fiber.interrupt(worker)
		}).pipe(Effect.provide(services))
	}).pipe(Effect.provide(NodeCrypto.layer)),
)

it.effect('handles verification separately and rejects body tampering without mailbox admission', () =>
	Effect.gen(function* () {
		yield* TestClock.setTime(yield* Clock.currentTimeMillis.pipe(TestClock.withLive))
		yield* Effect.gen(function* () {
			const ingress = yield* SlackIngress
			const context = Context.make(SlackIngress, ingress).pipe(Context.add(Clock.Clock, yield* Clock.Clock))
			const web = HttpRouter.toWebHandler(testRouteLayer, { disableLogger: true })
			yield* Effect.addFinalizer(() => Effect.promise(web.dispose))
			const timestamp = String(Math.floor((yield* Clock.currentTimeMillis) / 1000))
			const body = '{"type":"url_verification","challenge":"verified","token":"fixture"}'
			const signature = yield* signSlackBody(body, timestamp)
			const request = (text: string) =>
				new Request('http://localhost/api/v1/integrations/slack/webhook', {
					method: 'POST',
					body: text,
					headers: {
						'content-type': 'application/json',
						'x-slack-request-timestamp': timestamp,
						'x-slack-signature': signature,
					},
				})
			const verified = yield* Effect.promise(() => web.handler(request(body), context))
			assert.strictEqual(verified.status, 200)
			assert.strictEqual(yield* Effect.promise(() => verified.text()), 'verified')
			assert.strictEqual((yield* Effect.promise(() => web.handler(request(body + ' '), context))).status, 401)
			assert.strictEqual(yield* nativeMailbox('reply', nativeMessage('a')), undefined)
		}).pipe(
			Effect.provide(
				nativeIngressLayer({ onNewMention: [{ id: 'reply', handler: () => Effect.die('must not execute') }] }),
			),
		)
	}).pipe(Effect.provide(NodeCrypto.layer)),
)

it.effect('acknowledges a signed request after memory admission without waiting for its handler', () =>
	Effect.gen(function* () {
		yield* TestClock.setTime(yield* Clock.currentTimeMillis.pipe(TestClock.withLive))
		const entered = yield* Deferred.make<MessageEvent>()
		const release = yield* Deferred.make<void>()
		const completed = yield* Deferred.make<void>()
		const layer = nativeIngressLayer({
			onNewMention: [
				{
					id: 'reply',
					handler: (event) =>
						Deferred.succeed(entered, event).pipe(
							Effect.andThen(Deferred.await(release)),
							Effect.andThen(Deferred.succeed(completed, undefined)),
							Effect.asVoid,
						),
				},
			],
		})
		yield* Effect.gen(function* () {
			const ingress = yield* SlackIngress
			const requestContext = Context.make(SlackIngress, ingress).pipe(
				Context.add(Clock.Clock, yield* Clock.Clock),
			)
			const { handler, dispose } = HttpRouter.toWebHandler(testRouteLayer, { disableLogger: true })
			yield* Effect.addFinalizer(() => Effect.promise(dispose))
			const callback = yield* Schema.decodeEffect(SlackEventCallback)(appMentionCallback)
			const request = yield* signedSlackRequest(callback)
			const response = yield* Effect.promise(() => handler(request, requestContext))
			assert.strictEqual(response.status, 200)
			assert.strictEqual(yield* Deferred.isDone(entered), false)
			const pending = yield* nativeMailbox('reply', nativeMessage('a'))
			assert.strictEqual(pending?.state.pending.length, 1)
			assert.strictEqual(pending?.state.active, null)
			const worker = yield* ingress.run(nativeRunner).pipe(Effect.forkChild)
			const event = yield* Deferred.await(entered)
			assert.strictEqual(event.message.text, 'hello from Slack')
			assert.strictEqual(event.message.author.fullName, 'Hydrated User')
			const replay = yield* signedSlackRequest(callback)
			assert.strictEqual((yield* Effect.promise(() => handler(replay, requestContext))).status, 200)
			assert.strictEqual(yield* Deferred.isDone(completed), false)
			const active = yield* nativeMailbox('reply', nativeMessage('a'))
			assert.strictEqual(active?.state.active?.envelopes.length, 1)
			assert.strictEqual(active?.state.pending.length, 0)
			yield* Deferred.succeed(release, undefined)
			yield* Deferred.await(completed)
			yield* TestClock.adjust(10)
			const final = yield* nativeMailbox('reply', nativeMessage('a'))
			assert.strictEqual(final?.state.active, null)
			assert.deepStrictEqual(
				final?.state.outcomes.map((outcome) => outcome.kind),
				['completed'],
			)
			yield* Fiber.interrupt(worker)
		}).pipe(Effect.provide(layer))
	}).pipe(Effect.provide(NodeCrypto.layer)),
)

it.effect('queues A then D with hydrated skipped B/C while duplicates do not add envelopes', () =>
	Effect.gen(function* () {
		const releaseFirst = yield* Deferred.make<void>()
		const releaseLast = yield* Deferred.make<void>()
		const calls = yield* Queue.unbounded<{
			readonly event: MessageEvent
			readonly skipped: ReadonlyArray<MessageEvent>
		}>()
		const layer = nativeIngressLayer({
			onNewMention: [
				{
					id: 'reply',
					handler: (event, context) =>
						Queue.offer(calls, { event, skipped: context.skipped }).pipe(
							Effect.andThen(Deferred.await(event.message.text === 'a' ? releaseFirst : releaseLast)),
						),
				},
			],
		})
		yield* Effect.gen(function* () {
			const ingress = yield* SlackIngress
			const first = nativeMessage('a')
			assert.deepStrictEqual(
				yield* ingress.acceptMessage(first),
				IngressAccepted.make({ idempotencyKey: first.idempotencyKey }),
			)
			const worker = yield* ingress.run(nativeRunner).pipe(Effect.forkChild)
			const initial = yield* Queue.take(calls)
			assert.strictEqual(initial.event.message.text, 'a')
			assert.deepStrictEqual(initial.skipped, [])
			for (const seed of ['b', 'c', 'd', 'd']) yield* ingress.acceptMessage(nativeMessage(seed))
			const queued = yield* nativeMailbox('reply', first)
			assert.deepStrictEqual(
				queued?.state.pending.map((envelope) => envelope.eventId),
				['b', 'c', 'd'].map((seed) => nativeMessage(seed).idempotencyKey),
			)
			assert.strictEqual(queued?.state.active?.envelopes[0].eventId, first.idempotencyKey)
			assert.strictEqual(yield* Queue.size(calls), 0)
			yield* Deferred.succeed(releaseFirst, undefined)
			yield* TestClock.adjust(10)
			const latest = yield* Queue.take(calls)
			assert.strictEqual(latest.event.message.text, 'd')
			assert.deepStrictEqual(
				latest.skipped.map((event) => event.message.text),
				['b', 'c'],
			)
			for (const event of [...latest.skipped, latest.event]) {
				assert.strictEqual(Schema.is(MessageEvent)(event), true)
				assert.strictEqual(event.thread.currentMessage?.author.fullName, 'Hydrated User')
				assert.strictEqual(event.message.author.fullName, 'Hydrated User')
				assert.strictEqual(event.thread.ref.id, first.thread.ref.id)
			}
			yield* Deferred.succeed(releaseLast, undefined)
			yield* TestClock.adjust(10)
			const final = yield* nativeMailbox('reply', first)
			assert.strictEqual(final?.state.active, null)
			assert.strictEqual(final?.state.pending.length, 0)
			assert.strictEqual(final?.state.readyAt, null)
			assert.deepStrictEqual(
				final?.state.outcomes.map((outcome) => outcome.kind),
				['completed', 'completed', 'completed', 'completed'],
			)
			assert.strictEqual(yield* Queue.size(calls), 0)
			yield* Fiber.interrupt(worker)
		}).pipe(Effect.provide(layer))
	}),
)
