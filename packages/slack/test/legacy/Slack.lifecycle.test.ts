import { assert, it } from '@effect/vitest'
import { DateTime, Deferred, Effect, Fiber, Queue } from 'effect'
import { TestClock } from 'effect/testing'

import {
	SlackSubscriptions,
	ConversationStoppedEvent,
	Emoji,
	IdempotencyKey,
	SlackIngress,
	IngressAccepted,
	IngressDropped,
	Message,
	MessageDeletedEvent,
	MessageUpdatedEvent,
	NormalizedConversationStopped,
	NormalizedMessage,
	NormalizedMessageDeleted,
	NormalizedMessageUpdated,
	NormalizedReaction,
	ReactionAdded,
	ReactionEvent,
	Thread,
	ThreadId,
} from '../../src/index.ts'
import {
	ingressLayer,
	runnerOptions,
	makeTestAuthor,
	testAuthor,
	testMessage,
	testThread,
	testThreadRef,
} from './support.ts'

it.effect('dispatches updates, deletes, and filtered typed reactions once', () =>
	Effect.gen(function* () {
		const updates = yield* Queue.unbounded<MessageUpdatedEvent>()
		const deletes = yield* Queue.unbounded<MessageDeletedEvent>()
		const reactions = yield* Queue.unbounded<ReactionEvent>()
		const heartReactions = yield* Queue.unbounded<ReactionEvent>()
		const handlers = {
			onMessageUpdated: [
				{
					id: 'updates',
					handler: (event: MessageUpdatedEvent) => Queue.offer(updates, event).pipe(Effect.asVoid),
				},
			],
			onMessageDeleted: [
				{
					id: 'deletes',
					handler: (event: MessageDeletedEvent) => Queue.offer(deletes, event).pipe(Effect.asVoid),
				},
			],
			onReaction: [
				{
					id: 'reactions',
					handler: (event: ReactionEvent) =>
						Queue.offer(event.emoji.name === Emoji.Heart.name ? heartReactions : reactions, event).pipe(
							Effect.asVoid,
						),
				},
			],
		}
		yield* Effect.gen(function* () {
			const ingress = yield* SlackIngress
			const worker = yield* Effect.forkChild(ingress.run(runnerOptions))

			const updated = NormalizedMessageUpdated.make({
				provider: 'slack',
				tenant: testThreadRef.channel.tenant,
				idempotencyKey: IdempotencyKey.make(`evt_${'1'.repeat(32)}`),
				thread: testThread,
				message: testMessage,
				raw: {},
			})
			assert.deepStrictEqual(
				yield* ingress.acceptMessageUpdated(updated),
				IngressAccepted.make({ idempotencyKey: updated.idempotencyKey }),
			)
			yield* TestClock.adjust('20 millis')
			assert.strictEqual((yield* Queue.take(updates)).message.ref, testMessage.ref)
			const ownMessage = Message.make({
				ref: testMessage.ref,
				threadRef: testMessage.threadRef,
				text: testMessage.text,
				markdown: testMessage.markdown,
				author: makeTestAuthor({ userId: 'U_BOT', isBot: true, isMe: true }),
				metadata: testMessage.metadata,
				attachments: testMessage.attachments,
				raw: testMessage.raw,
			})
			const ownUpdate = NormalizedMessageUpdated.make({
				...updated,
				idempotencyKey: IdempotencyKey.make(`evt_${'4'.repeat(32)}`),
				message: ownMessage,
			})
			assert.deepStrictEqual(
				yield* ingress.acceptMessageUpdated(ownUpdate),
				IngressDropped.make({ reason: 'bot' }),
			)
			assert.strictEqual(yield* Queue.size(updates), 0)

			const deleted = NormalizedMessageDeleted.make({
				provider: 'slack',
				tenant: testThreadRef.channel.tenant,
				idempotencyKey: IdempotencyKey.make(`evt_${'2'.repeat(32)}`),
				threadRef: testThreadRef,
				messageRef: testMessage.ref,
				deletedAt: DateTime.makeUnsafe('2026-09-04T12:00:00Z'),
				raw: {},
			})
			yield* ingress.acceptMessageDeleted(deleted)
			yield* TestClock.adjust('20 millis')
			assert.strictEqual((yield* Queue.take(deletes)).messageRef, testMessage.ref)

			const reaction = NormalizedReaction.make({
				provider: 'slack',
				tenant: testThreadRef.channel.tenant,
				idempotencyKey: IdempotencyKey.make(`evt_${'3'.repeat(32)}`),
				thread: testThread,
				messageRef: testMessage.ref,
				change: ReactionAdded.make({}),
				emoji: Emoji.ThumbsUp,
				rawEmoji: 'thumbsup',
				actor: testAuthor,
				raw: {},
			})
			yield* ingress.acceptReaction(reaction)
			yield* TestClock.adjust('20 millis')
			assert.strictEqual((yield* Queue.take(reactions)).emoji.name, Emoji.ThumbsUp.name)
			assert.strictEqual(yield* Queue.size(heartReactions), 0)

			const heartReaction = NormalizedReaction.make({
				...reaction,
				idempotencyKey: IdempotencyKey.make(`evt_${'5'.repeat(32)}`),
				emoji: Emoji.Heart,
				rawEmoji: 'heart',
			})
			yield* ingress.acceptReaction(heartReaction)
			yield* TestClock.adjust('20 millis')
			assert.strictEqual((yield* Queue.take(heartReactions)).emoji.name, Emoji.Heart.name)
			assert.strictEqual(yield* Queue.size(reactions), 0)

			yield* Fiber.interrupt(worker)
		}).pipe(Effect.provide(ingressLayer(handlers)))
	}),
)

const dmRootRef = {
	...testThreadRef,
	id: ThreadId.make('slack:v1:T_TEST:im:D_TEST:100.1'),
	channel: { ...testThreadRef.channel, isDm: true },
}

const dmConversationRef = {
	...dmRootRef,
	id: ThreadId.make('slack:v1:T_TEST:im:D_TEST'),
}

const dmMessage = Message.make({
	ref: testMessage.ref,
	threadRef: dmRootRef,
	text: testMessage.text,
	markdown: testMessage.markdown,
	author: testMessage.author,
	metadata: testMessage.metadata,
	attachments: testMessage.attachments,
	raw: testMessage.raw,
})
const dmThread = Thread.make({ ref: dmRootRef, currentMessage: dmMessage, recentMessages: [dmMessage] })

it.effect('bridges DM edit, delete, and reaction lifecycle events to a subscribed proactive identity', () =>
	Effect.gen(function* () {
		const updates = yield* Queue.unbounded<MessageUpdatedEvent>()
		const deletes = yield* Queue.unbounded<MessageDeletedEvent>()
		const reactions = yield* Queue.unbounded<ReactionEvent>()
		const handlers = {
			onMessageUpdated: [
				{
					id: 'updates',
					handler: (event: MessageUpdatedEvent) => Queue.offer(updates, event).pipe(Effect.asVoid),
				},
			],
			onMessageDeleted: [
				{
					id: 'deletes',
					handler: (event: MessageDeletedEvent) => Queue.offer(deletes, event).pipe(Effect.asVoid),
				},
			],
			onReaction: [
				{
					id: 'reactions',
					handler: (event: ReactionEvent) => Queue.offer(reactions, event).pipe(Effect.asVoid),
				},
			],
		}
		yield* Effect.gen(function* () {
			const ingress = yield* SlackIngress
			const subscriptions = yield* SlackSubscriptions
			yield* subscriptions.subscribe({ threadId: dmConversationRef.id })
			const worker = yield* Effect.forkChild(ingress.run(runnerOptions))

			yield* ingress.acceptMessageUpdated(
				NormalizedMessageUpdated.make({
					provider: 'slack',
					tenant: dmRootRef.channel.tenant,
					idempotencyKey: IdempotencyKey.make(`evt_${'6'.repeat(32)}`),
					thread: dmThread,
					directMessageThread: dmConversationRef,
					message: dmMessage,
					previousMessage: dmMessage,
					raw: {},
				}),
			)
			yield* TestClock.adjust('20 millis')
			const updated = yield* Queue.take(updates)
			assert.strictEqual(updated.thread.ref.id, dmConversationRef.id)
			assert.strictEqual(updated.message.threadRef.id, dmConversationRef.id)
			assert.strictEqual(updated.previousMessage?.threadRef.id, dmConversationRef.id)

			yield* ingress.acceptMessageDeleted(
				NormalizedMessageDeleted.make({
					provider: 'slack',
					tenant: dmRootRef.channel.tenant,
					idempotencyKey: IdempotencyKey.make(`evt_${'7'.repeat(32)}`),
					threadRef: dmRootRef,
					directMessageThread: dmConversationRef,
					messageRef: dmMessage.ref,
					previousMessage: dmMessage,
					raw: {},
				}),
			)
			yield* TestClock.adjust('20 millis')
			const deleted = yield* Queue.take(deletes)
			assert.strictEqual(deleted.threadRef.id, dmConversationRef.id)
			assert.strictEqual(deleted.previousMessage?.threadRef.id, dmConversationRef.id)

			yield* ingress.acceptReaction(
				NormalizedReaction.make({
					provider: 'slack',
					tenant: dmRootRef.channel.tenant,
					idempotencyKey: IdempotencyKey.make(`evt_${'8'.repeat(32)}`),
					thread: dmThread,
					directMessageThread: dmConversationRef,
					messageRef: dmMessage.ref,
					change: ReactionAdded.make({}),
					emoji: Emoji.ThumbsUp,
					rawEmoji: 'thumbsup',
					actor: testAuthor,
					raw: {},
				}),
			)
			yield* TestClock.adjust('20 millis')
			const reaction = yield* Queue.take(reactions)
			assert.strictEqual(reaction.thread.ref.id, dmConversationRef.id)
			assert.strictEqual(reaction.thread.ref.channel.isDm, true)
			yield* Fiber.interrupt(worker)
		}).pipe(Effect.provide(ingressLayer(handlers)))
	}),
)

it.effect('drops own DM lifecycle events before proactive identity resolution', () =>
	Effect.gen(function* () {
		const ingress = yield* SlackIngress
		const ownAuthor = makeTestAuthor({ userId: 'U_BOT', isBot: true, isMe: true })
		const ownMessage = Message.make({
			ref: dmMessage.ref,
			threadRef: dmMessage.threadRef,
			text: dmMessage.text,
			markdown: dmMessage.markdown,
			author: ownAuthor,
			metadata: dmMessage.metadata,
			attachments: dmMessage.attachments,
			raw: dmMessage.raw,
		})
		const ownThread = Thread.make({ ref: dmRootRef, currentMessage: ownMessage, recentMessages: [ownMessage] })

		assert.deepStrictEqual(
			yield* ingress.acceptMessageUpdated(
				NormalizedMessageUpdated.make({
					provider: 'slack',
					tenant: dmRootRef.channel.tenant,
					idempotencyKey: IdempotencyKey.make(`evt_${'a'.repeat(32)}`),
					thread: ownThread,
					directMessageThread: dmConversationRef,
					message: ownMessage,
					raw: {},
				}),
			),
			IngressDropped.make({ reason: 'bot' }),
		)
		assert.deepStrictEqual(
			yield* ingress.acceptMessageDeleted(
				NormalizedMessageDeleted.make({
					provider: 'slack',
					tenant: dmRootRef.channel.tenant,
					idempotencyKey: IdempotencyKey.make(`evt_${'b'.repeat(32)}`),
					threadRef: dmRootRef,
					directMessageThread: dmConversationRef,
					messageRef: ownMessage.ref,
					previousMessage: ownMessage,
					raw: {},
				}),
			),
			IngressDropped.make({ reason: 'bot' }),
		)
		assert.deepStrictEqual(
			yield* ingress.acceptReaction(
				NormalizedReaction.make({
					provider: 'slack',
					tenant: dmRootRef.channel.tenant,
					idempotencyKey: IdempotencyKey.make(`evt_${'c'.repeat(32)}`),
					thread: ownThread,
					directMessageThread: dmConversationRef,
					messageRef: ownMessage.ref,
					change: ReactionAdded.make({}),
					emoji: Emoji.Check,
					rawEmoji: 'white_check_mark',
					actor: ownAuthor,
					raw: {},
				}),
			),
			IngressDropped.make({ reason: 'bot' }),
		)
	}).pipe(Effect.provide(ingressLayer({}))),
)

it.effect('cancels a subscribed proactive DM mailbox for a rooted agent session stop', () =>
	Effect.gen(function* () {
		const started = yield* Deferred.make<void>()
		const finalized = yield* Queue.unbounded<void>()
		const stopped = yield* Queue.unbounded<string>()
		const handlers = {
			onDirectMessage: [
				{
					id: 'dm',
					handler: () =>
						Deferred.succeed(started, undefined).pipe(
							Effect.andThen(Effect.never),
							Effect.ensuring(Queue.offer(finalized, undefined)),
						),
				},
			],
			onConversationStopped: [
				{
					id: 'stop',
					handler: (event: ConversationStoppedEvent) =>
						Queue.offer(stopped, event.threadRef.id).pipe(Effect.asVoid),
				},
			],
		}
		yield* Effect.gen(function* () {
			const ingress = yield* SlackIngress
			const subscriptions = yield* SlackSubscriptions
			yield* subscriptions.subscribe({ threadId: dmConversationRef.id })
			const worker = yield* Effect.forkChild(ingress.run(runnerOptions))

			yield* ingress.acceptMessage(
				NormalizedMessage.make({
					provider: 'slack',
					tenant: dmRootRef.channel.tenant,
					idempotencyKey: IdempotencyKey.make(`evt_${'9'.repeat(32)}`),
					thread: dmThread,
					directMessageThread: dmConversationRef,
					message: dmMessage,
					mentioned: false,
					raw: {},
				}),
			)
			yield* TestClock.adjust('20 millis')
			yield* Deferred.await(started)
			yield* ingress.acceptConversationStopped(
				NormalizedConversationStopped.make({
					provider: 'slack',
					tenant: dmRootRef.channel.tenant,
					idempotencyKey: IdempotencyKey.make(`evt_${'a'.repeat(32)}`),
					threadRef: dmRootRef,
					directMessageThread: dmConversationRef,
					raw: {},
				}),
			)
			yield* TestClock.adjust('100 millis')
			yield* Queue.take(finalized)
			assert.strictEqual(yield* Queue.take(stopped), dmConversationRef.id)
			yield* Fiber.interrupt(worker)
		}).pipe(Effect.provide(ingressLayer(handlers)))
	}),
)
