import { assert, it } from '@effect/vitest'
import { Context, Effect, Fiber, Layer, Queue, Ref } from 'effect'
import { HttpClient, HttpClientResponse } from 'effect/unstable/http'

import { Slack, SlackBot, SlackIngress, SlackSubscriptions, type MessageEvent } from '../src/index.js'
import { nativeMessage, nativeRunner, testAuthor } from './nativeSupport.js'
import { testConnectionStoreLayer } from './support.js'

class Replies extends Context.Service<
	Replies,
	{
		readonly record: (input: { readonly name: string }) => Effect.Effect<void>
	}
>()('test/Replies') {}

it.effect('the memory bot captures application services and shares users between ingress and handler operations', () =>
	Effect.gen(function* () {
		const calls = yield* Ref.make(0)
		const delivered = yield* Queue.unbounded<string>()
		const client = Layer.merge(
			testConnectionStoreLayer,
			Layer.succeed(
				HttpClient.HttpClient,
				HttpClient.make((request) =>
					Ref.update(calls, (n) => n + 1).pipe(
						Effect.as(
							HttpClientResponse.fromWeb(
								request,
								Response.json({
									ok: true,
									user: {
										id: testAuthor.userId,
										real_name: testAuthor.fullName,
										name: testAuthor.userName,
										is_bot: false,
									},
								}),
							),
						),
					),
				),
			),
		)
		const bot = SlackBot.memory({
			namespace: 'bot-services',
			handlers: {
				onNewMention: ({ thread, message }: MessageEvent) =>
					Effect.gen(function* () {
						const replies = yield* Replies
						const slack = yield* Slack
						const user = yield* slack.getUser({
							provider: 'slack',
							tenant: thread.ref.channel.tenant,
							userId: message.author.userId,
						})
						yield* thread.subscribe()
						yield* replies.record({ name: user.author.fullName })
					}),
			},
		})
		const dependencies = Layer.merge(
			client,
			Layer.succeed(
				Replies,
				Replies.of({
					record: ({ name }) => Queue.offer(delivered, name).pipe(Effect.asVoid),
				}),
			),
		)
		yield* Effect.gen(function* () {
			const ingress = yield* SlackIngress
			const event = nativeMessage('a')
			yield* ingress.acceptMessage(event)
			assert.strictEqual(yield* Queue.size(delivered), 0)
			assert.strictEqual(yield* Ref.get(calls), 0)
			const worker = yield* ingress.run(nativeRunner).pipe(Effect.forkChild)
			assert.strictEqual(yield* Queue.take(delivered), testAuthor.fullName)
			assert.strictEqual(yield* Ref.get(calls), 1)
			assert.strictEqual(yield* (yield* SlackSubscriptions).isSubscribed({ threadId: event.thread.ref.id }), true)
			yield* Fiber.interrupt(worker)
		}).pipe(Effect.provide(bot.services.pipe(Layer.provide(dependencies))))
	}),
)
