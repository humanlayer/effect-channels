import { assert, it } from '@effect/vitest'
import { layer as deliveryMemory } from '@humanlayer/channels-delivery/memory'
import { Deferred, Effect, Fiber, Layer, Queue, Tracer } from 'effect'
import { TestClock } from 'effect/testing'

import { Author, Thread, UserProfile } from '../src/index.ts'
import { SlackAuthors } from '../src/SlackAuthors.ts'
import { ConversationStoppedEvent, MessageUpdatedEvent, NormalizedConversationStopped } from '../src/SlackEvents.ts'
import { SlackChannelId } from '../src/SlackIdentity.ts'
import { awaitStoppedTargets } from '../src/SlackIngressBindings.ts'
import {
	acceptConversationStopped,
	acceptMessage,
	resolveDirectMessageIdentity,
	resolveUpdated,
	run,
	SlackIngressBindings,
} from '../src/SlackIngressOperations.ts'
import { SlackSubscriptions } from '../src/SlackSubscriptions.ts'
import { SlackDmConversationTs, slackThreadRef } from '../src/SlackThreadId.ts'
import { nativeMailbox, nativeMessage, nativePolicy, nativeRunner, testAuthor, testMessage } from './nativeSupport.ts'
import { stubSlackClientLayer, testRootTs, testTeamId } from './support.ts'

it.effect('enriches an update and its previous message with only the author dependency graph', () =>
	Effect.gen(function* () {
		const requests = yield* Queue.unbounded<string>()
		const event = MessageUpdatedEvent.make({
			...nativeMessage('1'),
			previousMessage: testMessage,
		})
		const resolved = yield* resolveUpdated(event).pipe(
			Effect.provide(
				SlackAuthors.layer.pipe(
					Layer.provide(
						stubSlackClientLayer({
							getUser: (input) =>
								Queue.offer(requests, input.userId).pipe(
									Effect.as(
										UserProfile.make({
											author: Author.make({ ...testAuthor, fullName: 'Resolved User' }),
										}),
									),
								),
						}),
					),
				),
			),
		)
		assert.strictEqual(resolved.message.author.fullName, 'Resolved User')
		assert.strictEqual(resolved.previousMessage?.author.fullName, 'Resolved User')
		assert.strictEqual(resolved.previousMessage?.text, testMessage.text)
		assert.strictEqual(resolved.thread.currentMessage?.author.fullName, 'Resolved User')
		assert.strictEqual(yield* Queue.take(requests), testAuthor.userId)
		assert.strictEqual(yield* Queue.size(requests), 0)
	}),
)

const rootedDm = slackThreadRef(
	{
		teamId: testTeamId,
		channelId: SlackChannelId.make('D_TEST'),
		threadTs: testRootTs,
		directMessageKind: 'im',
	},
	true,
)
const proactiveDm = slackThreadRef(
	{
		teamId: testTeamId,
		channelId: SlackChannelId.make('D_TEST'),
		threadTs: SlackDmConversationTs,
		directMessageKind: 'im',
	},
	false,
)

it.effect('pins a proactive DM route using only subscriptions, even after unsubscribe', () =>
	Effect.gen(function* () {
		const subscriptions = yield* SlackSubscriptions
		yield* subscriptions.subscribe({ threadId: proactiveDm.id })
		const input = {
			idempotencyKey: nativeMessage('2').idempotencyKey,
			threadRef: rootedDm,
			directMessageThread: proactiveDm,
		}
		const first = yield* resolveDirectMessageIdentity(input)
		yield* subscriptions.unsubscribe({ threadId: proactiveDm.id })
		const replay = yield* resolveDirectMessageIdentity(input)
		assert.deepStrictEqual(first, { threadRef: proactiveDm, subscribed: true })
		assert.deepStrictEqual(replay, first)
		assert.strictEqual(yield* subscriptions.isSubscribed({ threadId: proactiveDm.id }), false)
	}).pipe(Effect.provide(SlackSubscriptions.layerMemory())),
)

it.effect('fans out and dedupes with real bindings and only admission dependencies', () =>
	Effect.gen(function* () {
		const event = nativeMessage('3')
		const result = yield* acceptMessage(event)
		yield* acceptMessage(event)
		assert.strictEqual(result._tag, 'IngressAccepted')
		for (const id of ['first', 'second']) {
			const mailbox = yield* nativeMailbox(id, event)
			assert.deepStrictEqual(
				mailbox?.state.pending.map((envelope) => envelope.eventId),
				[event.idempotencyKey],
			)
			assert.strictEqual(mailbox?.state.active, null)
		}
		assert.strictEqual(yield* nativeMailbox('subscribed', event), undefined)
	}).pipe(
		Effect.provide(
			Layer.mergeAll(
				SlackIngressBindings.layer({
					namespace: 'slack-native-test',
					policy: nativePolicy,
					handlers: {
						onNewMention: ['first', 'second'].map((id) => ({
							id,
							handler: () => Effect.die('admission must not execute handlers'),
						})),
						onSubscribedMessage: [{ id: 'subscribed', handler: () => Effect.die('wrong route') }],
					},
				}),
				SlackSubscriptions.layerMemory(),
				deliveryMemory({ maxMailboxes: 100 }),
			),
		),
	),
)

it.effect('pins Stop cancellation and its barrier to the same real DM targets without hydration', () =>
	Effect.gen(function* () {
		const subscriptions = yield* SlackSubscriptions
		yield* subscriptions.subscribe({ threadId: proactiveDm.id })
		const event = NormalizedConversationStopped.make({
			...nativeMessage('4'),
			threadRef: rootedDm,
			directMessageThread: proactiveDm,
		})
		const result = yield* acceptConversationStopped(event)
		yield* subscriptions.unsubscribe({ threadId: proactiveDm.id })
		yield* acceptConversationStopped(event)
		yield* awaitStoppedTargets({ event: ConversationStoppedEvent.make({ ...event, threadRef: proactiveDm }) })
		assert.strictEqual(result._tag, 'IngressAccepted')
		const probe = { ...nativeMessage('4'), thread: Thread.fromRef(proactiveDm) }
		for (const id of ['mention', 'dm', 'subscribed']) {
			const mailbox = yield* nativeMailbox(id, probe)
			assert.deepStrictEqual(
				mailbox?.state.outcomes.map((outcome) => outcome.identity),
				[`control:${event.idempotencyKey}`],
			)
			assert.strictEqual(yield* nativeMailbox(id, nativeMessage('4')), undefined)
		}
		const stop = yield* nativeMailbox('stop', probe)
		assert.deepStrictEqual(
			stop?.state.pending.map((envelope) => envelope.eventId),
			[event.idempotencyKey],
		)
	}).pipe(
		Effect.provide(
			Layer.mergeAll(
				SlackIngressBindings.layer({
					namespace: 'slack-native-test',
					policy: nativePolicy,
					handlers: {
						onNewMention: [{ id: 'mention', handler: () => Effect.void }],
						onDirectMessage: [{ id: 'dm', handler: () => Effect.void }],
						onSubscribedMessage: [{ id: 'subscribed', handler: () => Effect.void }],
						onConversationStopped: [{ id: 'stop', handler: () => Effect.void }],
					},
				}),
				SlackSubscriptions.layerMemory(),
				deliveryMemory({ maxMailboxes: 100 }),
			),
		),
	),
)

it.effect('real operations wait for every fan-out finalizer before invoking the Stop registration', () =>
	Effect.gen(function* () {
		const started = yield* Queue.unbounded<string>()
		const cleaning = yield* Queue.unbounded<string>()
		const stopped = yield* Queue.unbounded<string>()
		const release = yield* Deferred.make<void>()
		let invocations = 0
		const bindings = SlackIngressBindings.layer({
			namespace: 'slack-native-test',
			policy: nativePolicy,
			handlers: {
				onNewMention: ['first', 'second'].map((id) => ({
					id,
					handler: () =>
						Effect.gen(function* () {
							yield* Effect.addFinalizer(() =>
								Queue.offer(cleaning, id).pipe(Effect.andThen(Deferred.await(release))),
							)
							yield* Queue.offer(started, id)
							return yield* Effect.never
						}),
				})),
				onConversationStopped: [
					{
						id: 'stop',
						handler: (event) => {
							invocations++
							return Queue.offer(stopped, event.idempotencyKey).pipe(Effect.asVoid)
						},
					},
				],
			},
		})
		yield* Effect.gen(function* () {
			const event = nativeMessage('6')
			const stop = NormalizedConversationStopped.make({
				...event,
				idempotencyKey: nativeMessage('7').idempotencyKey,
				threadRef: event.thread.ref,
			})
			yield* acceptMessage(event)
			const worker = yield* run(nativeRunner).pipe(Effect.forkChild)
			assert.deepStrictEqual([yield* Queue.take(started), yield* Queue.take(started)].sort(), ['first', 'second'])
			yield* acceptConversationStopped(stop)
			yield* TestClock.adjust(100)
			assert.deepStrictEqual([yield* Queue.take(cleaning), yield* Queue.take(cleaning)].sort(), [
				'first',
				'second',
			])
			yield* acceptConversationStopped(stop)
			assert.strictEqual(invocations, 0)
			yield* Deferred.succeed(release, undefined)
			yield* TestClock.adjust(110)
			assert.strictEqual(yield* Queue.take(stopped), stop.idempotencyKey)
			yield* acceptConversationStopped(stop)
			yield* TestClock.adjust(110)
			assert.strictEqual(invocations, 1)
			for (const id of ['first', 'second']) {
				const mailbox = yield* nativeMailbox(id, event)
				assert.strictEqual(mailbox?.state.active, null)
				assert.strictEqual(mailbox?.state.outcomes.filter((outcome) => outcome.kind === 'cancelled').length, 1)
			}
			const mailbox = yield* nativeMailbox('stop', event)
			assert.strictEqual(mailbox?.state.pending.length, 0)
			assert.deepStrictEqual(
				mailbox?.state.outcomes.map((outcome) => outcome.kind),
				['completed'],
			)
			yield* Fiber.interrupt(worker)
		}).pipe(
			Effect.provide(
				Layer.mergeAll(
					bindings,
					SlackSubscriptions.layerMemory(),
					deliveryMemory({ maxMailboxes: 100 }),
					SlackAuthors.layer.pipe(
						Layer.provide(
							stubSlackClientLayer({
								getUser: () => Effect.succeed(UserProfile.make({ author: testAuthor })),
							}),
						),
					),
				),
			),
		)
	}),
)

for (const empty of [true, false]) {
	it.effect(`traces and interrupts the ${empty ? 'empty' : 'configured'} real ingress runner`, () =>
		Effect.gen(function* () {
			const spans = yield* Queue.unbounded<Tracer.Span>()
			const tracer = yield* Effect.tracer
			const recording = Tracer.make({
				span(options) {
					const span = tracer.span(options)
					if (options.name === 'slack.ingress.run') Queue.offerUnsafe(spans, span)
					return span
				},
			})
			const started = yield* Queue.unbounded<string>()
			const finalized = yield* Queue.unbounded<string>()
			yield* Effect.gen(function* () {
				yield* acceptMessage(nativeMessage('5'))
				const fiber = yield* run(nativeRunner).pipe(Effect.withTracer(recording), Effect.forkChild)
				const span = yield* Queue.take(spans)
				if (!empty) assert.strictEqual(yield* Queue.take(started), 'ready')
				assert.strictEqual(span.status._tag, 'Started')
				yield* Fiber.interrupt(fiber)
				assert.strictEqual(span.status._tag, 'Ended')
				if (!empty) assert.strictEqual(yield* Queue.take(finalized), 'done')
			}).pipe(
				Effect.provide(
					Layer.mergeAll(
						SlackIngressBindings.layer({
							namespace: 'slack-native-test',
							policy: nativePolicy,
							handlers: {
								onNewMention: empty
									? []
									: [
											{
												id: 'reply',
												handler: () =>
													Queue.offer(started, 'ready').pipe(
														Effect.andThen(Effect.never),
														Effect.ensuring(Queue.offer(finalized, 'done')),
													),
											},
										],
							},
						}),
						SlackSubscriptions.layerMemory(),
						deliveryMemory({ maxMailboxes: 100 }),
						SlackAuthors.layer.pipe(
							Layer.provide(
								stubSlackClientLayer({
									getUser: () => Effect.succeed(UserProfile.make({ author: testAuthor })),
								}),
							),
						),
					),
				),
			)
		}),
	)
}
