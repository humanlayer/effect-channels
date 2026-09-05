import { assert, it } from '@effect/vitest'
import { MarkdownTextChunk, PlanUpdateChunk, ThreadId } from '@humanlayer/channels'
import { Effect, Exit, Fiber, Layer, Queue, Stream } from 'effect'
import { TestClock } from 'effect/testing'

import { SlackApiError } from '../src/Errors.ts'
import { SlackMessageTs } from '../src/Schema.ts'
import { SlackClient } from '../src/SlackClient.ts'
import { SlackProvider } from '../src/SlackProvider.ts'
import { makeStubSlackClient, testChannelId, testRootThreadId } from './support.ts'

it.effect('falls back to one post plus throttled edits with final raw markdown', () =>
	Effect.gen(function* () {
		const calls = yield* Queue.unbounded<{ readonly kind: string; readonly text: string }>()
		const client = makeStubSlackClient({
			startStream: () =>
				Effect.fail(SlackApiError.make({ operation: 'chat.startStream', code: 'feature_not_enabled' })),
			postMessage: (input) =>
				Queue.offer(calls, { kind: 'post', text: input.text }).pipe(
					Effect.as({ channelId: testChannelId, ts: SlackMessageTs.make('100.2') }),
				),
			updateMessage: (input) =>
				Queue.offer(calls, { kind: 'edit', text: input.text }).pipe(
					Effect.as({ channelId: testChannelId, ts: input.ts }),
				),
		})
		const providerLayer = SlackProvider.layer.pipe(Layer.provide(Layer.succeed(SlackClient, client)))
		const fiber = yield* Effect.flatMap(SlackProvider, (provider) =>
			provider.stream(
				{ threadId: ThreadId.make(testRootThreadId) },
				Stream.make(
					MarkdownTextChunk.make({ text: '```ts\n' }),
					MarkdownTextChunk.make({ text: 'const answer = 42' }),
					PlanUpdateChunk.make({ title: 'ignored in fallback' }),
				),
			),
		).pipe(Effect.provide(providerLayer), Effect.forkChild)
		const posted = yield* Queue.take(calls)
		assert.deepStrictEqual(posted, { kind: 'post', text: '```ts\n\n```' })
		yield* TestClock.adjust('500 millis')
		const sent = yield* Fiber.join(fiber)
		const rest = yield* Queue.takeAll(calls)
		assert.strictEqual(rest.at(-1)?.text, '```ts\nconst answer = 42')
		assert.strictEqual(sent.ref.messageRef, '100.2')
		assert.deepStrictEqual([...sent.ref.degraded].sort(), ['native_streaming', 'plan_update'])
	}),
)

it.effect('posts one explicit placeholder for an empty stream', () =>
	Effect.gen(function* () {
		const client = makeStubSlackClient({
			postMessage: () => Effect.succeed({ channelId: testChannelId, ts: SlackMessageTs.make('100.3') }),
		})
		const sent = yield* Effect.flatMap(SlackProvider, (provider) =>
			provider.stream({ threadId: ThreadId.make(testRootThreadId) }, Stream.empty),
		).pipe(Effect.provide(SlackProvider.layer.pipe(Layer.provide(Layer.succeed(SlackClient, client)))))
		assert.strictEqual(sent.message.markdown, '…')
		assert.deepStrictEqual(sent.ref.degraded, ['native_streaming', 'empty_stream'])
	}),
)

it.effect('keeps source failures typed before and after the first output without duplicating the reply', () =>
	Effect.gen(function* () {
		const calls = yield* Queue.unbounded<string>()
		const client = makeStubSlackClient({
			postMessage: (input) =>
				Queue.offer(calls, input.text).pipe(
					Effect.as({ channelId: testChannelId, ts: SlackMessageTs.make('100.4') }),
				),
		})
		const layer = SlackProvider.layerWith({ streaming: 'post_and_edit' }).pipe(
			Layer.provide(Layer.succeed(SlackClient, client)),
		)
		const before = yield* Effect.flatMap(SlackProvider, (provider) =>
			provider.stream({ threadId: ThreadId.make(testRootThreadId) }, Stream.fail('before')),
		).pipe(Effect.provide(layer), Effect.exit)
		assert.strictEqual(Exit.isFailure(before), true)
		assert.strictEqual(yield* Queue.size(calls), 0)

		const after = yield* Effect.flatMap(SlackProvider, (provider) =>
			provider.stream(
				{ threadId: ThreadId.make(testRootThreadId) },
				Stream.make(MarkdownTextChunk.make({ text: 'started' })).pipe(Stream.concat(Stream.fail('after'))),
			),
		).pipe(Effect.provide(layer), Effect.exit)
		assert.strictEqual(Exit.isFailure(after), true)
		assert.deepStrictEqual(yield* Queue.takeAll(calls), ['started'])
	}),
)
