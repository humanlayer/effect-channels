import { assert, it } from '@effect/vitest'
import { Deferred, Effect, Exit, Fiber, Layer, Queue, Stream } from 'effect'

import { ConversationCoordinator } from '../../channels/src/ConversationCoordinator.ts'
import { ConversationStoppedEvent } from '../../channels/src/Events.ts'
import { IdempotencyKey, ThreadId, UserId } from '../../channels/src/Schema.ts'
import { MarkdownTextChunk } from '../../channels/src/StreamChunk.ts'
import { makeTestMessageEvent } from '../../channels/test/support.ts'
import { SlackApiError } from '../src/Errors.ts'
import { SlackMessageTs, SlackStreamRef } from '../src/Schema.ts'
import { SlackClient } from '../src/SlackClient.ts'
import { SlackProvider } from '../src/SlackProvider.ts'
import { makeStubSlackClient, testChannelId, testRootThreadId, testRootTs } from './support.ts'

it.effect('cancels the targeted head and delivers the stop callback next without retrying it', () =>
	Effect.gen(function* () {
		const coordinator = yield* ConversationCoordinator
		const entered = yield* Deferred.make<void>()
		const nextEntered = yield* Deferred.make<void>()
		const observed = yield* Queue.unbounded<string>()
		const nextFinalized = yield* Queue.unbounded<void>()
		const event = makeTestMessageEvent(`evt_${'c'.repeat(32)}`)
		const nextEvent = makeTestMessageEvent(`evt_${'e'.repeat(32)}`)
		const stop = ConversationStoppedEvent.make({
			orgId: event.orgId,
			provider: event.provider,
			tenant: event.tenant,
			idempotencyKey: IdempotencyKey.make(`evt_${'d'.repeat(32)}`),
			threadRef: event.thread.ref,
			raw: {},
		})
		const worker = yield* coordinator
			.run((current) => {
				if (current.idempotencyKey === event.idempotencyKey) {
					return Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never))
				}
				if (current.idempotencyKey === stop.idempotencyKey) {
					return Queue.offer(observed, current._tag).pipe(Effect.asVoid)
				}
				return Deferred.succeed(nextEntered, undefined).pipe(
					Effect.andThen(Effect.never),
					Effect.ensuring(Queue.offer(nextFinalized, undefined)),
				)
			})
			.pipe(Effect.forkChild)
		yield* coordinator.submit(event)
		yield* Deferred.await(entered)
		assert.strictEqual(yield* coordinator.submitCancellation(stop), true)
		assert.strictEqual(yield* Queue.take(observed), 'ConversationStoppedEvent')
		yield* coordinator.submit(nextEvent)
		yield* Deferred.await(nextEntered)
		assert.strictEqual(yield* coordinator.submitCancellation(stop), false)
		assert.strictEqual(yield* Queue.size(nextFinalized), 0)
		yield* Fiber.interrupt(worker)
	}).pipe(Effect.provide(ConversationCoordinator.layerMemory())),
)

it.effect('restores active status after an interrupted stream and leaves no streaming fiber alive', () =>
	Effect.gen(function* () {
		const statuses = yield* Queue.unbounded<string>()
		const streamEntered = yield* Deferred.make<void>()
		const client = makeStubSlackClient({
			setSessionStatus: (input) => Queue.offer(statuses, input.status).pipe(Effect.asVoid),
		})
		const providerLayer = SlackProvider.layerWith({ streaming: 'post_and_edit' }).pipe(
			Layer.provide(Layer.succeed(SlackClient, client)),
		)
		yield* Effect.gen(function* () {
			const provider = yield* SlackProvider
			const threadId = ThreadId.make(testRootThreadId)
			yield* provider.startThreadTyping({ threadId })
			assert.strictEqual(yield* Queue.take(statuses), 'processing')
			const streamFiber = yield* provider
				.stream(
					{ threadId },
					Stream.fromEffect(Deferred.succeed(streamEntered, undefined).pipe(Effect.andThen(Effect.never))),
				)
				.pipe(Effect.forkChild)
			yield* Deferred.await(streamEntered)
			yield* Fiber.interrupt(streamFiber)
			assert.strictEqual(yield* Queue.take(statuses), 'active')
			assert.strictEqual(yield* Queue.size(statuses), 0)
		}).pipe(Effect.provide(providerLayer))
	}),
)

it.effect('stops an opened native stream when a later append fails', () =>
	Effect.gen(function* () {
		const stopped = yield* Queue.unbounded<string>()
		const client = makeStubSlackClient({
			startStream: () =>
				Effect.succeed(
					SlackStreamRef.make({
						channelId: testChannelId,
						messageTs: SlackMessageTs.make('100.2'),
						threadTs: testRootTs,
					}),
				),
			appendStream: () =>
				Effect.fail(SlackApiError.make({ operation: 'chat.appendStream', code: 'internal_error' })),
			stopStream: (input) =>
				Queue.offer(stopped, input.stream.messageTs).pipe(
					Effect.as({ channelId: input.stream.channelId, ts: input.stream.messageTs }),
				),
		})
		const providerLayer = SlackProvider.layer.pipe(Layer.provide(Layer.succeed(SlackClient, client)))
		const exit = yield* Effect.exit(
			Effect.flatMap(SlackProvider, (provider) =>
				provider.stream(
					{ threadId: ThreadId.make(testRootThreadId), recipientUserId: UserId.make('U_TEST') },
					Stream.make(MarkdownTextChunk.make({ text: 'first' }), MarkdownTextChunk.make({ text: 'second' })),
				),
			).pipe(Effect.provide(providerLayer)),
		)
		assert.strictEqual(Exit.isFailure(exit), true)
		assert.strictEqual(yield* Queue.take(stopped), '100.2')
	}),
)
