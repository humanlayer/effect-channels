import { assert, it } from '@effect/vitest'
import { Context, Effect, Fiber, Layer, Queue, Ref } from 'effect'

import { Slack, SlackBot, SlackIngress, SlackSubscriptions, UserProfile, type MessageEvent } from '../src/index.ts'
import { nativeMessage, nativeRunner, testAuthor } from './nativeSupport.ts'
import { stubSlackClientLayer } from './support.ts'

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
		const client = stubSlackClientLayer({
			getUser: () => Ref.update(calls, (n) => n + 1).pipe(Effect.as(UserProfile.make({ author: testAuthor }))),
		})
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
