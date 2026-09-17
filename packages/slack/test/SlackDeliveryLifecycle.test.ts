import { assert, it } from '@effect/vitest'
import {
	activeBatches,
	currentMailbox,
	DeliveryControl,
	HandlerFailure,
	type DeliveryId,
	type DeliveryPolicy,
} from '@humanlayer/channels-delivery'
import { layer as deliveryMemory } from '@humanlayer/channels-delivery/memory'
import { Deferred, Effect, Fiber, Layer, Queue } from 'effect'
import { TestClock } from 'effect/testing'

import { NormalizedConversationStopped, NormalizedMessageUpdated, SlackIngress } from '../src/index'
import { SlackMessageTs } from '../src/Schema'
import { nativeIngressLayer, nativeMailbox, nativeMessage, nativePolicy, nativeRunner } from './nativeSupport'

const policies: ReadonlyArray<DeliveryPolicy> = [
	{ ...nativePolicy, mode: 'concurrent', maxConcurrency: 2 },
	{ ...nativePolicy, mode: 'debounce', quietPeriodMs: 1000 },
	{ ...nativePolicy, mode: 'burst', windowMs: 1000 },
	{ ...nativePolicy, mode: 'drop' },
]

it.effect('concurrent Stop waits for A finalizers and retries its callback while unrelated B keeps running', () =>
	Effect.gen(function* () {
		const calls = yield* Queue.unbounded<string>()
		const cleaning = yield* Deferred.make<void>()
		const release = yield* Deferred.make<void>()
		const callbacks = yield* Queue.unbounded<number>()
		let attempts = 0
		const services = nativeIngressLayer(
			{
				onNewMention: [
					{
						id: 'messages',
						handler: (event) =>
							Effect.gen(function* () {
								if (event.message.text === 'a')
									yield* Effect.addFinalizer(() =>
										Deferred.succeed(cleaning, undefined).pipe(
											Effect.andThen(Deferred.await(release)),
										),
									)
								yield* Queue.offer(calls, event.message.text)
								return yield* Effect.never
							}),
					},
				],
				onConversationStopped: [
					{
						id: 'stopped',
						handler: () =>
							Effect.gen(function* () {
								yield* Queue.offer(callbacks, ++attempts)
								if (attempts === 1) return yield* HandlerFailure.make({ retryable: true })
							}),
					},
				],
			},
			undefined,
			{ ...nativePolicy, mode: 'concurrent', maxConcurrency: 2, retentionMs: 50 },
		)
		yield* Effect.gen(function* () {
			const ingress = yield* SlackIngress
			const a = nativeMessage('a')
			yield* ingress.acceptMessage(a)
			const runner = yield* ingress.run(nativeRunner).pipe(Effect.forkChild)
			assert.strictEqual(yield* Queue.take(calls), 'a')
			yield* ingress.acceptMessage(nativeMessage('b'))
			yield* TestClock.adjust(10)
			assert.strictEqual(yield* Queue.take(calls), 'b')
			const stop = NormalizedConversationStopped.make({
				...a,
				idempotencyKey: nativeMessage('c').idempotencyKey,
				threadRef: a.thread.ref,
			})
			yield* ingress.acceptConversationStopped(stop)
			yield* TestClock.adjust(100)
			yield* Deferred.await(cleaning)
			yield* TestClock.adjust(2000)
			yield* ingress.acceptMessage(nativeMessage('b'))
			yield* ingress.acceptConversationStopped(stop)
			assert.strictEqual(yield* Queue.size(callbacks), 0)
			const during = yield* nativeMailbox('messages', a)
			assert.ok(during !== undefined)
			assert.deepStrictEqual(
				activeBatches(during.state).map((batch) => batch.cancelled),
				[true, false],
			)
			assert.ok(
				during.state.outcomes.some(
					(outcome) => outcome.kind === 'control' && outcome.cancellationTarget !== undefined,
				),
			)
			yield* Deferred.succeed(release, undefined)
			yield* TestClock.adjust(110)
			assert.strictEqual(yield* Queue.take(callbacks), 1)
			yield* TestClock.adjust(110)
			assert.strictEqual(yield* Queue.take(callbacks), 2)
			const after = yield* nativeMailbox('messages', a)
			assert.ok(after !== undefined)
			assert.deepStrictEqual(
				activeBatches(after.state).map((batch) => ({
					id: batch.envelopes[0].eventId,
					cancelled: batch.cancelled,
				})),
				[{ id: nativeMessage('b').idempotencyKey, cancelled: false }],
			)
			const lifecycle = yield* nativeMailbox('stopped', a)
			assert.strictEqual(lifecycle?.state.active, null)
			assert.strictEqual(lifecycle?.state.pending.length, 0)
			yield* Fiber.interrupt(runner)
		}).pipe(Effect.provide(services))
	}),
)

for (const policy of policies) {
	it.effect(`${policy.mode} messages do not change serial-every-event Slack lifecycle delivery`, () =>
		Effect.gen(function* () {
			const calls = yield* Queue.unbounded<string>()
			const gate = yield* Deferred.make<void>()
			const services = nativeIngressLayer(
				{
					onMessageUpdated: [
						{
							id: 'edits',
							handler: (event, context) => {
								assert.deepStrictEqual(context.skipped, [])
								return Queue.offer(calls, event.message.text).pipe(Effect.andThen(Deferred.await(gate)))
							},
						},
					],
				},
				undefined,
				policy,
			)
			yield* Effect.gen(function* () {
				const ingress = yield* SlackIngress
				const admit = (id: string) =>
					ingress.acceptMessageUpdated(NormalizedMessageUpdated.make(nativeMessage(id)))
				yield* admit('a')
				yield* admit('b')
				yield* admit('b')
				const runner = yield* ingress.run(nativeRunner).pipe(Effect.forkChild)
				assert.strictEqual(yield* Queue.take(calls), 'a')
				yield* admit('c')
				yield* TestClock.adjust(100)
				assert.strictEqual(yield* Queue.size(calls), 0)
				yield* Deferred.succeed(gate, undefined)
				yield* TestClock.adjust(10)
				assert.strictEqual(yield* Queue.take(calls), 'b')
				yield* TestClock.adjust(10)
				assert.strictEqual(yield* Queue.take(calls), 'c')
				yield* TestClock.adjust(10)
				const final = yield* nativeMailbox('edits', nativeMessage('a'))
				assert.deepStrictEqual(
					final?.state.outcomes.map((outcome) => outcome.kind),
					['completed', 'completed', 'completed'],
				)
				assert.strictEqual(final?.state.readyAt, null)
				yield* Fiber.interrupt(runner)
			}).pipe(Effect.provide(services))
		}),
	)
}

it.effect('delivers final Markdown returned by an onConversationStopped registration', () =>
	Effect.gen(function* () {
		const deliveryIds = yield* Queue.unbounded<DeliveryId>()
		const storage = deliveryMemory({ maxMailboxes: 100 })
		const ingress = nativeIngressLayer(
			{
				onConversationStopped: [
					{
						id: 'stopped-output',
						handler: (_event, context) =>
							Effect.gen(function* () {
								yield* Queue.offer(deliveryIds, context.deliveryId)
								yield* (yield* DeliveryControl).finish({
									deliveryId: context.deliveryId,
									outcome: 'completed',
									markdown: 'Conversation stopped.',
								})
							}),
					},
				],
			},
			storage,
			nativePolicy,
			{
				postMessage: (input) =>
					Effect.succeed({ channelId: input.channelId, ts: SlackMessageTs.make('100.2') }),
			},
		)
		const services = ingress.pipe(Layer.provideMerge(DeliveryControl.layer.pipe(Layer.provide(storage))))
		yield* Effect.gen(function* () {
			const slackIngress = yield* SlackIngress
			const message = nativeMessage('d')
			yield* slackIngress.acceptConversationStopped(
				NormalizedConversationStopped.make({
					...message,
					threadRef: message.thread.ref,
				}),
			)
			const runner = yield* slackIngress.run(nativeRunner).pipe(Effect.forkChild)
			yield* Queue.take(deliveryIds)
			yield* TestClock.adjust(120)
			const mailbox = yield* nativeMailbox('stopped-output', message)
			assert.strictEqual(
				mailbox === undefined ? undefined : currentMailbox(mailbox.state).operations?.[0]?.state._tag,
				'Delivered',
			)
			assert.strictEqual(activeBatches(mailbox?.state ?? assert.fail('missing mailbox')).length, 0)
			yield* Fiber.interrupt(runner)
		}).pipe(Effect.provide(services))
	}),
)
