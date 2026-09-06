import { assert, it } from '@effect/vitest'
import { Effect, Fiber, Layer, Queue, Tracer } from 'effect'

import { Author, UserProfile } from '../src/index.ts'
import { SlackAuthors } from '../src/SlackAuthors.ts'
import { ConversationStoppedEvent, MessageUpdatedEvent, NormalizedConversationStopped } from '../src/SlackEvents.ts'
import { SlackChannelId } from '../src/SlackIdentity.ts'
import {
	acceptConversationStopped,
	acceptMessage,
	awaitStoppedTargets,
	resolveDirectMessageIdentity,
	resolveUpdated,
	run,
	SlackIngressBindings,
	type DeliveryBinding,
} from '../src/SlackIngressOperations.ts'
import { SlackSubscriptions } from '../src/SlackSubscriptions.ts'
import { SlackDmConversationTs, slackThreadRef } from '../src/SlackThreadId.ts'
import { nativeMessage, nativeRunner, testAuthor, testMessage } from './nativeSupport.ts'
import { stubSlackClientLayer, testRootTs, testTeamId, unimplemented } from './support.ts'

const emptyBindings = SlackIngressBindings.of({
	newMention: [],
	subscribedMessage: [],
	directMessage: [],
	messageBindings: [],
	updated: [],
	deleted: [],
	reactions: [],
	stopped: [],
	allBindings: [],
})

const unusedBinding = <A>(): DeliveryBinding<A> => ({
	admit: () => unimplemented('admit'),
	keyForResource: () => unimplemented('keyForResource'),
	cancelActive: () => unimplemented('cancelActive'),
	awaitCancellation: () => unimplemented('awaitCancellation'),
	run: () => unimplemented('run'),
})

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

it.effect('admits a mention without author, client, mailbox, or runner dependencies', () =>
	Effect.gen(function* () {
		const events = yield* Queue.unbounded<string>()
		const event = nativeMessage('3')
		const result = yield* acceptMessage(event).pipe(
			Effect.provide(
				Layer.succeed(SlackIngressBindings, {
					...emptyBindings,
					newMention: [
						{
							...unusedBinding(),
							admit: ({ event }) =>
								Queue.offer(events, event.message.text).pipe(
									Effect.as({ key: 'mention', accepted: true }),
								),
						},
					],
				}),
			),
		)
		assert.strictEqual(result._tag, 'IngressAccepted')
		assert.strictEqual(yield* Queue.take(events), event.message.text)
		assert.strictEqual(yield* Queue.size(events), 0)
	}).pipe(Effect.provide(SlackSubscriptions.layerMemory())),
)

it.effect('routes Stop cancellation and its barrier to the same configured message target without hydration', () =>
	Effect.gen(function* () {
		const calls = yield* Queue.unbounded<string>()
		const subscriptions = yield* SlackSubscriptions
		yield* subscriptions.subscribe({ threadId: proactiveDm.id })
		const event = NormalizedConversationStopped.make({
			...nativeMessage('4'),
			threadRef: rootedDm,
			directMessageThread: proactiveDm,
		})
		const bindings = Layer.succeed(SlackIngressBindings, {
			...emptyBindings,
			messageBindings: [
				{
					...unusedBinding(),
					keyForResource: ({ installation, resource }) =>
						Effect.succeed(`${installation}/${resource.threadId}`),
					cancelActive: ({ key, controlId }) =>
						Queue.offer(calls, `cancel:${key}:${controlId}`).pipe(Effect.as(true)),
					awaitCancellation: ({ key, controlId }) =>
						Queue.offer(calls, `wait:${key}:${controlId}`).pipe(Effect.asVoid),
				},
			],
			stopped: [
				{
					...unusedBinding(),
					admit: ({ event }) =>
						Queue.offer(calls, `admit:${event.threadRef.id}`).pipe(
							Effect.as({ key: 'stop', accepted: true }),
						),
				},
			],
		})
		const result = yield* acceptConversationStopped(event).pipe(Effect.provide(bindings))
		assert.strictEqual(result._tag, 'IngressAccepted')
		yield* awaitStoppedTargets(ConversationStoppedEvent.make({ ...event, threadRef: proactiveDm })).pipe(
			Effect.provide(bindings),
		)
		const target = `${event.tenant}/${proactiveDm.id}:${event.idempotencyKey}`
		assert.strictEqual(yield* Queue.take(calls), `cancel:${target}`)
		assert.strictEqual(yield* Queue.take(calls), `admit:${proactiveDm.id}`)
		assert.strictEqual(yield* Queue.take(calls), `wait:${target}`)
		assert.strictEqual(yield* Queue.size(calls), 0)
	}).pipe(Effect.provide(SlackSubscriptions.layerMemory())),
)

for (const empty of [true, false]) {
	it.effect(`traces and interrupts the ${empty ? 'empty' : 'configured'} ingress runner with only bindings`, () =>
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
			const fiber = yield* run(nativeRunner).pipe(
				Effect.provide(
					Layer.succeed(SlackIngressBindings, {
						...emptyBindings,
						allBindings: empty
							? []
							: [
									{
										run: () =>
											Queue.offer(started, 'ready').pipe(
												Effect.andThen(Effect.never),
												Effect.ensuring(Queue.offer(finalized, 'done')),
											),
									},
								],
					}),
				),
				Effect.withTracer(recording),
				Effect.forkChild,
			)
			const span = yield* Queue.take(spans)
			if (!empty) assert.strictEqual(yield* Queue.take(started), 'ready')
			assert.strictEqual(span.status._tag, 'Started')
			yield* Fiber.interrupt(fiber)
			assert.strictEqual(span.status._tag, 'Ended')
			if (!empty) assert.strictEqual(yield* Queue.take(finalized), 'done')
		}),
	)
}
