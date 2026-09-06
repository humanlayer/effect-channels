import { NodeCrypto } from '@effect/platform-node'
import { assert, it } from '@effect/vitest'
import { layer as memory } from '@humanlayer/channels-delivery/memory'
import {
	Clock,
	ConfigProvider,
	Context,
	Deferred,
	Effect,
	Fiber,
	Layer,
	Option,
	Queue,
	Redacted,
	Ref,
	Schema,
} from 'effect'
import { TestClock } from 'effect/testing'
import { HttpClient, HttpClientRequest, HttpClientResponse, HttpRouter } from 'effect/unstable/http'

import { SlackEventCallback } from '../../src/index.ts'
import { SlackClient } from '../../src/index.ts'
import { Slack } from '../../src/index.ts'
import { SlackRoutes } from '../../src/index.ts'
import { SlackTenantCredentials } from '../../src/index.ts'
import { SlackIngress, SlackSubscriptions, MarkdownContent, ThreadId } from '../../src/index.ts'
import { testConnectionStoreLayer } from '../support.ts'
import { appMentionCallback, signSlackBody } from '../support.ts'
import { policy, runnerOptions } from './support.ts'

const testRootThreadId = ThreadId.make('slack:v1:T_TEST:C_TEST:100.1')

const SlackPostBody = Schema.Struct({
	channel: Schema.String,
	thread_ts: Schema.String,
	text: Schema.String,
})

const webhookUrl = 'http://channels.test/api/v1/integrations/slack/webhook'

it.effect('delivers one signed mention end to end, subscribes explicitly, and posts in the Slack root thread', () =>
	Effect.gen(function* () {
		yield* TestClock.setTime(yield* Clock.currentTimeMillis.pipe(TestClock.withLive))
		const requests = yield* Queue.unbounded<string>()
		const completed = yield* Deferred.make<void>()
		const handlerCount = yield* Ref.make(0)
		const httpClient = HttpClient.make((request) =>
			Effect.gen(function* () {
				const webRequest = yield* HttpClientRequest.toWeb(request).pipe(Effect.orDie)
				const isUserLookup = webRequest.url.includes('/users.info')
				if (!isUserLookup) {
					yield* Queue.offer(requests, yield* Effect.promise(() => webRequest.text()))
				}
				return HttpClientResponse.fromWeb(
					request,
					new Response(
						isUserLookup
							? '{"ok":true,"user":{"id":"U_HUMAN","name":"Human"}}'
							: '{"ok":true,"channel":"C_TEST","ts":"100.2"}',
						{
							status: 200,
							headers: { 'content-type': 'application/json' },
						},
					),
				)
			}),
		)
		const credentials = SlackTenantCredentials.make({
			load: () => Effect.succeed(Option.some({ botToken: Redacted.make('xoxb-test-token') })),
			save: () => Effect.void,
		})
		const slackClient = SlackClient.layer.pipe(
			Layer.provide(Layer.merge(Layer.succeed(HttpClient.HttpClient, httpClient), credentials)),
		)
		const slack = Slack.layer.pipe(Layer.provide(testConnectionStoreLayer), Layer.provide(slackClient))
		const subscriptions = SlackSubscriptions.layerMemory()
		const ingressLayer = SlackIngress.layer({
			namespace: 'legacy-root',
			policy,
			handlers: {
				onNewMention: [
					{
						id: 'echo',
						handler: ({ thread, message }) =>
							Effect.gen(function* () {
								yield* Ref.update(handlerCount, (count) => count + 1)
								yield* thread.subscribe()
								yield* thread.post(MarkdownContent.make({ markdown: `echo: ${message.text}` }))
								yield* Deferred.succeed(completed, undefined)
							}),
					},
				],
			},
		}).pipe(Layer.provideMerge(Layer.mergeAll(slack, subscriptions, memory({ maxMailboxes: 100 }))))
		const testApplication = Layer.merge(ingressLayer, NodeCrypto.layer)
		const program = Effect.gen(function* () {
			const ingress = yield* SlackIngress
			const channels = yield* SlackSubscriptions
			const worker = yield* Effect.forkChild(ingress.run(runnerOptions))

			const routeLayer = SlackRoutes.layer.pipe(
				HttpRouter.provideRequest(NodeCrypto.layer),
				Layer.provide(
					ConfigProvider.layer(
						ConfigProvider.fromUnknown({
							SLACK_SIGNING_SECRET: 'test-signing-secret',
							SLACK_BOT_USER_ID: 'U_BOT',
						}),
					),
				),
				Layer.provide(credentials),
				Layer.provide(slackClient),
			)
			const { dispose, handler } = HttpRouter.toWebHandler(routeLayer, { disableLogger: true })
			yield* Effect.addFinalizer(() => Effect.promise(dispose))

			const callback = yield* Schema.decodeEffect(SlackEventCallback)(appMentionCallback)
			const body = yield* Schema.encodeEffect(Schema.fromJsonString(SlackEventCallback))(callback)
			const currentTime = yield* Clock.currentTimeMillis.pipe(TestClock.withLive)
			const timestamp = Math.floor(currentTime / 1000).toString()
			const signature = yield* signSlackBody(body, timestamp)
			const requestContext = Context.make(SlackIngress, ingress).pipe(
				Context.add(Clock.Clock, yield* Clock.Clock),
			)
			const deliver = Effect.promise(() =>
				handler(
					new Request(webhookUrl, {
						method: 'POST',
						headers: {
							'content-type': 'application/json',
							'x-slack-request-timestamp': timestamp,
							'x-slack-signature': signature,
						},
						body,
					}),
					requestContext,
				),
			)
			const first = yield* deliver
			const redelivery = yield* deliver
			yield* TestClock.adjust('20 millis')
			yield* Deferred.await(completed)
			const requestBody = yield* Queue.take(requests)
			const decodedBody = yield* Schema.decodeEffect(Schema.fromJsonString(SlackPostBody))(requestBody)
			const subscribed = yield* channels.isSubscribed({ threadId: testRootThreadId })
			const count = yield* Ref.get(handlerCount)
			yield* Fiber.interrupt(worker)

			assert.strictEqual(first.status, 200)
			assert.strictEqual(redelivery.status, 200)
			assert.deepStrictEqual(decodedBody, {
				channel: 'C_TEST',
				thread_ts: '100.1',
				text: 'echo: hello from Slack',
			})
			assert.strictEqual(subscribed, true)
			assert.strictEqual(count, 1)
		})
		yield* program.pipe(Effect.provide(testApplication))
	}),
)
