import { assert, it } from '@effect/vitest'
import { Deferred, Effect, Fiber, Layer, Queue } from 'effect'

import { SlackApiError } from '../../slack/src/Errors.ts'
import { SlackMessageTs, SlackSentMessage } from '../../slack/src/Schema.ts'
import { SlackProvider } from '../../slack/src/SlackProvider.ts'
import { stubSlackClientLayer, testChannelId } from '../../slack/test/support.ts'
import { MarkdownContent, ProviderRegistry, Thread } from '../src/index.ts'
import { ChannelsWithIngressLayer, expectTaggedFailure, testThreadRef } from './support.ts'

const content = MarkdownContent.make({ markdown: 'working on it' })
const sentReply = SlackSentMessage.make({ channelId: testChannelId, ts: SlackMessageTs.make('100.2') })

const withSlackProvider = <A, E, R>(
	client: Parameters<typeof stubSlackClientLayer>[0],
	program: Effect.Effect<A, E, R>,
) =>
	Effect.gen(function* () {
		const registry = yield* ProviderRegistry
		yield* registry.register(yield* SlackProvider)
		return yield* program
	}).pipe(
		Effect.provide(
			Layer.merge(
				ChannelsWithIngressLayer,
				SlackProvider.layer.pipe(Layer.provide(stubSlackClientLayer(client))),
			),
		),
	)

it.effect('restores the active status when a post is interrupted after processing started', () =>
	Effect.gen(function* () {
		const statuses = yield* Queue.unbounded<string>()
		const posting = yield* Deferred.make<void>()
		yield* withSlackProvider(
			{
				setSessionStatus: (input) => Queue.offer(statuses, input.status).pipe(Effect.asVoid),
				postMessage: () => Deferred.succeed(posting, undefined).pipe(Effect.andThen(Effect.never)),
			},
			Effect.gen(function* () {
				const thread = Thread.fromRef(testThreadRef)
				const fiber = yield* Effect.forkChild(thread.startTyping().pipe(Effect.andThen(thread.post(content))))
				yield* Deferred.await(posting)
				assert.strictEqual(yield* Queue.take(statuses), 'processing')
				assert.strictEqual(yield* Queue.size(statuses), 0)
				yield* Fiber.interrupt(fiber)
				assert.strictEqual(yield* Queue.take(statuses), 'active')
				assert.strictEqual(yield* Queue.size(statuses), 0)
			}),
		)
	}),
)

it.effect('restores the active status and fails with PostFailed when Slack rejects the post', () =>
	Effect.gen(function* () {
		const statuses = yield* Queue.unbounded<string>()
		yield* withSlackProvider(
			{
				setSessionStatus: (input) => Queue.offer(statuses, input.status).pipe(Effect.asVoid),
				postMessage: () =>
					Effect.fail(SlackApiError.make({ operation: 'chat.postMessage', code: 'channel_not_found' })),
			},
			Effect.gen(function* () {
				const thread = Thread.fromRef(testThreadRef)
				yield* thread.startTyping()
				const error = yield* expectTaggedFailure('PostFailed')(thread.post(content))
				assert.strictEqual(error.threadId, testThreadRef.id)
				assert.strictEqual(error.provider, 'slack')
				assert.deepStrictEqual(yield* Queue.takeAll(statuses), ['processing', 'active'])
			}),
		)
	}),
)

it.effect('leaves the session status alone when a post never started typing', () =>
	Effect.gen(function* () {
		const statuses = yield* Queue.unbounded<string>()
		yield* withSlackProvider(
			{
				setSessionStatus: (input) => Queue.offer(statuses, input.status).pipe(Effect.asVoid),
				postMessage: () => Effect.succeed(sentReply),
			},
			Effect.gen(function* () {
				const thread = Thread.fromRef(testThreadRef)
				const sent = yield* thread.post(content)
				assert.strictEqual(sent.ref.messageRef, '100.2')
				assert.strictEqual(yield* Queue.size(statuses), 0)
			}),
		)
	}),
)

it.effect('restores the active status once per startTyping, so a second post stays silent', () =>
	Effect.gen(function* () {
		const statuses = yield* Queue.unbounded<string>()
		yield* withSlackProvider(
			{
				setSessionStatus: (input) => Queue.offer(statuses, input.status).pipe(Effect.asVoid),
				postMessage: () => Effect.succeed(sentReply),
			},
			Effect.gen(function* () {
				const thread = Thread.fromRef(testThreadRef)
				yield* thread.startTyping()
				yield* thread.post(content)
				yield* thread.post(content)
				assert.deepStrictEqual(yield* Queue.takeAll(statuses), ['processing', 'active'])
			}),
		)
	}),
)
