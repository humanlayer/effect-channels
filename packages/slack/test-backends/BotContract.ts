import { assert } from '@effect/vitest'
import { ConfigProvider, Effect, Fiber, Layer, Queue, Redacted } from 'effect'
import { HttpClient, HttpClientResponse } from 'effect/unstable/http'

import {
	MarkdownContent,
	SlackBot,
	SlackIngress,
	SlackState,
	SlackSubscriptions,
	SlackTeamId,
	type MessageEvent,
} from '../src/index.ts'
import { nativeMessage, nativeRunner, testAuthor } from '../test/nativeSupport.ts'

/** Same production bot over the ambient real storage bundle; only Slack's external HTTP is replaced. */
export const botContract = Effect.gen(function* () {
	const delivered = yield* Queue.unbounded<string>()
	const requests = yield* Queue.unbounded<string>()
	const http = Layer.succeed(
		HttpClient.HttpClient,
		HttpClient.make((request) =>
			Effect.gen(function* () {
				assert.strictEqual(request.headers.authorization, 'Bearer backend-bot-token')
				yield* Queue.offer(requests, request.url)
				if (request.url.endsWith('/users.info'))
					return HttpClientResponse.fromWeb(
						request,
						Response.json({
							ok: true,
							user: { id: testAuthor.userId, real_name: testAuthor.fullName },
						}),
					)
				assert.ok(request.url.endsWith('/chat.postMessage'), `unexpected Slack operation: ${request.url}`)
				return HttpClientResponse.fromWeb(request, Response.json({ ok: true, channel: 'C_TEST', ts: '200.1' }))
			}),
		),
	)
	const bot = SlackBot.make({
		namespace: 'backend-bot-roundtrip',
		handlers: {
			onNewMention: ({ thread, message }: MessageEvent, context) =>
				Effect.gen(function* () {
					yield* thread.subscribe()
					yield* thread.post(MarkdownContent.make({ markdown: `Echo: ${message.text}` }))
					yield* Queue.offer(
						delivered,
						[message.text, ...context.skipped.map((event) => event.message.text)].join(','),
					)
				}),
		},
	})
	const runtime = Layer.fresh(
		bot.services.pipe(Layer.provide(http), Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({})))),
	)
	const event = nativeMessage('a')
	yield* Effect.gen(function* () {
		const state = yield* SlackState
		yield* state.upsertConnection({
			workspaceId: SlackTeamId.make(event.tenant),
			connection: {
				credentials: {
					botToken: Redacted.make('backend-bot-token'),
					botUserId: 'U_BACKEND_BOT',
					botId: 'B_BACKEND_BOT',
				},
			},
		})
		const ingress = yield* SlackIngress
		for (const id of ['a', 'b', 'c']) yield* ingress.acceptMessage(nativeMessage(id))
		assert.strictEqual(yield* Queue.size(requests), 0)
		assert.strictEqual(yield* Queue.size(delivered), 0)
	}).pipe(Effect.provide(runtime))
	yield* Effect.gen(function* () {
		const ingress = yield* SlackIngress
		const worker = yield* ingress.run(nativeRunner).pipe(Effect.forkChild)
		assert.strictEqual(yield* Queue.take(delivered), 'c,a,b')
		assert.strictEqual(yield* (yield* SlackSubscriptions).isSubscribed({ threadId: event.thread.ref.id }), true)
		const calls = yield* Queue.takeAll(requests)
		assert.strictEqual(calls.filter((url) => url.endsWith('/chat.postMessage')).length, 1)
		assert.strictEqual(calls.filter((url) => url.endsWith('/users.info')).length, 1)
		yield* Fiber.interrupt(worker)
	}).pipe(Effect.provide(runtime))
})
