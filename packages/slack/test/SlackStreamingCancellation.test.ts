import { assert, it } from '@effect/vitest'
import { Deferred, Effect, Exit, Fiber, Layer, Queue, Stream } from 'effect'
import { TestClock } from 'effect/testing'

import { SlackApiError } from '../src/Errors'
import {
	NormalizedConversationStopped,
	SlackIngress,
	IdempotencyKey,
	ThreadId,
	UserId,
	MarkdownTextChunk,
} from '../src/index'
import { SlackMessageTs, type SlackPostMessageInput, SlackSentMessage, SlackStreamRef } from '../src/Schema'
import { Slack } from '../src/Slack'
import { SlackClient } from '../src/SlackClient'
import { nativeIngressLayer, nativeMailbox, nativeMessage, nativeRunner } from './nativeSupport'
import { testConnectionStoreLayer } from './support'
import { makeStubSlackClient, testChannelId, testRootThreadId, testRootTs } from './support'

it.effect('cancels the targeted head and delivers the stop callback next without retrying it', () =>
	Effect.gen(function* () {
		const entered = yield* Deferred.make<void>()
		const nextEntered = yield* Deferred.make<void>()
		const observed = yield* Queue.unbounded<string>()
		const nextFinalized = yield* Queue.unbounded<void>()
		const finalized = yield* Deferred.make<void>()
		const event = nativeMessage('c')
		const nextEvent = nativeMessage('e')
		const stop = NormalizedConversationStopped.make({
			provider: event.provider,
			tenant: event.tenant,
			idempotencyKey: IdempotencyKey.make(`evt_${'d'.repeat(32)}`),
			threadRef: event.thread.ref,
			raw: {},
		})
		const layer = nativeIngressLayer({
			onNewMention: [
				{
					id: 'reply',
					handler: (current) => {
						if (current.idempotencyKey === event.idempotencyKey) {
							return Deferred.succeed(entered, undefined).pipe(
								Effect.andThen(Effect.never),
								Effect.ensuring(Deferred.succeed(finalized, undefined)),
							)
						}
						return Deferred.succeed(nextEntered, undefined).pipe(
							Effect.andThen(Effect.never),
							Effect.ensuring(Queue.offer(nextFinalized, undefined)),
						)
					},
				},
			],
			onConversationStopped: [
				{
					id: 'stop',
					handler: (current) =>
						Effect.gen(function* () {
							assert.strictEqual(yield* Deferred.isDone(finalized), true)
							yield* Queue.offer(observed, current._tag)
						}),
				},
			],
		})
		yield* Effect.gen(function* () {
			const ingress = yield* SlackIngress
			yield* ingress.acceptMessage(event)
			const worker = yield* ingress.run(nativeRunner).pipe(Effect.forkChild)
			yield* Deferred.await(entered)
			yield* ingress.acceptConversationStopped(stop)
			yield* TestClock.adjust(200)
			assert.strictEqual(yield* Queue.take(observed), 'ConversationStoppedEvent')
			assert.strictEqual(
				(yield* nativeMailbox('reply', event))?.state.outcomes.some((outcome) => outcome.kind === 'cancelled'),
				true,
			)
			yield* ingress.acceptMessage(nextEvent)
			yield* TestClock.adjust(10)
			yield* Deferred.await(nextEntered)
			yield* ingress.acceptConversationStopped(stop)
			yield* TestClock.adjust(200)
			assert.strictEqual(yield* Queue.size(nextFinalized), 0)
			assert.strictEqual(yield* Queue.size(observed), 0)
			assert.strictEqual((yield* nativeMailbox('reply', nextEvent))?.state.active?.attempt, 1)
			yield* Fiber.interrupt(worker)
		}).pipe(Effect.provide(layer))
	}),
)

it.effect('restores active status after an interrupted stream and leaves no streaming fiber alive', () =>
	Effect.gen(function* () {
		const statuses = yield* Queue.unbounded<string>()
		const streamEntered = yield* Deferred.make<void>()
		const client = makeStubSlackClient({
			setSessionStatus: (input) => Queue.offer(statuses, input.status).pipe(Effect.asVoid),
		})
		const providerLayer = Slack.layerWith({ streaming: 'post_and_edit' }).pipe(
			Layer.provide(testConnectionStoreLayer),
			Layer.provide(Layer.succeed(SlackClient, client)),
		)
		yield* Effect.gen(function* () {
			const provider = yield* Slack
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

it.effect('never sends the proactive DM sentinel as a streaming thread timestamp', () =>
	Effect.gen(function* () {
		const posts = yield* Queue.unbounded<SlackPostMessageInput>()
		const sent = SlackSentMessage.make({ channelId: testChannelId, ts: SlackMessageTs.make('100.2') })
		const client = makeStubSlackClient({
			postMessage: (input) => Queue.offer(posts, input).pipe(Effect.as(sent)),
			updateMessage: () => Effect.succeed(sent),
		})
		const providerLayer = Slack.layer.pipe(
			Layer.provide(testConnectionStoreLayer),
			Layer.provide(Layer.succeed(SlackClient, client)),
		)
		yield* Effect.flatMap(Slack, (provider) =>
			provider.stream(
				{ threadId: ThreadId.make('slack:v1:T_TEST:im:C_TEST') },
				Stream.make(MarkdownTextChunk.make({ text: 'hello' })),
			),
		).pipe(Effect.provide(providerLayer))
		const post = yield* Queue.take(posts)
		assert.strictEqual(post.threadTs, undefined)
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
		const providerLayer = Slack.layer.pipe(
			Layer.provide(testConnectionStoreLayer),
			Layer.provide(Layer.succeed(SlackClient, client)),
		)
		const exit = yield* Effect.exit(
			Effect.flatMap(Slack, (provider) =>
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
