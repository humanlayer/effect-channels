import { assert, it } from '@effect/vitest'
import { DateTime, Effect, Fiber, Queue } from 'effect'

import {
	Channels,
	Emoji,
	IdempotencyKey,
	Ingress,
	IngressAccepted,
	IngressDropped,
	Message,
	MessageDeletedEvent,
	MessageUpdatedEvent,
	NormalizedMessageDeleted,
	NormalizedMessageUpdated,
	NormalizedReaction,
	ReactionAdded,
	ReactionEvent,
} from '../src/index.ts'
import {
	ChannelsWithIngressLayer,
	makeTestAuthor,
	testAuthor,
	testMessage,
	testThread,
	testThreadRef,
} from './support.ts'

it.effect('dispatches updates, deletes, and filtered typed reactions once', () =>
	Effect.gen(function* () {
		const channels = yield* Channels
		const ingress = yield* Ingress
		const updates = yield* Queue.unbounded<MessageUpdatedEvent>()
		const deletes = yield* Queue.unbounded<MessageDeletedEvent>()
		const reactions = yield* Queue.unbounded<ReactionEvent>()
		const heartReactions = yield* Queue.unbounded<ReactionEvent>()
		yield* channels.onMessageUpdated((event) => Queue.offer(updates, event).pipe(Effect.asVoid))
		yield* channels.onMessageDeleted((event) => Queue.offer(deletes, event).pipe(Effect.asVoid))
		yield* channels.onReaction([Emoji.ThumbsUp], (event) => Queue.offer(reactions, event).pipe(Effect.asVoid))
		yield* channels.onReaction([Emoji.Heart], (event) => Queue.offer(heartReactions, event).pipe(Effect.asVoid))
		const worker = yield* Effect.forkChild(channels.run)

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
		assert.deepStrictEqual(yield* ingress.acceptMessageUpdated(ownUpdate), IngressDropped.make({ reason: 'bot' }))
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
		assert.strictEqual((yield* Queue.take(reactions)).emoji.name, Emoji.ThumbsUp.name)
		assert.strictEqual(yield* Queue.size(heartReactions), 0)

		const heartReaction = NormalizedReaction.make({
			...reaction,
			idempotencyKey: IdempotencyKey.make(`evt_${'5'.repeat(32)}`),
			emoji: Emoji.Heart,
			rawEmoji: 'heart',
		})
		yield* ingress.acceptReaction(heartReaction)
		assert.strictEqual((yield* Queue.take(heartReactions)).emoji.name, Emoji.Heart.name)
		assert.strictEqual(yield* Queue.size(reactions), 0)

		yield* Fiber.interrupt(worker)
	}).pipe(Effect.provide(ChannelsWithIngressLayer)),
)
