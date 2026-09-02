import { NodeCrypto } from '@effect/platform-node'
import { assert, it } from '@effect/vitest'
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
import { Persistence } from 'effect/unstable/persistence'

import { SlackEventCallback } from '../../slack/src/Schema.ts'
import { SlackClient } from '../../slack/src/SlackClient.ts'
import { SlackProvider } from '../../slack/src/SlackProvider.ts'
import { SlackRoutes } from '../../slack/src/SlackRoutes.ts'
import { SlackTenantCredentials } from '../../slack/src/SlackTenantCredentials.ts'
import { appMentionCallback, signSlackBody } from '../../slack/test/support.ts'
import {
	Channels,
	ChannelsGate,
	ChannelsObserver,
	ConversationCoordinator,
	ConversationSignals,
	Ingress,
	MarkdownContent,
	Organizations,
	OrgId,
	ProviderRegistry,
	Subscriptions,
	ThreadId,
} from '../src/index.ts'

const testRootThreadId = ThreadId.make('slack:v1:T_TEST:C_TEST:100.1')

const SlackPostBody = Schema.Struct({
	channel: Schema.String,
	thread_ts: Schema.String,
	text: Schema.String,
})

const webhookUrl = 'http://channels.test/api/v1/integrations/slack/webhook'

it.effect('delivers one signed mention end to end, subscribes explicitly, and posts in the Slack root thread', () =>
	Effect.gen(function* () {
		const requests = yield* Queue.unbounded<string>()
		const completed = yield* Deferred.make<void>()
		const handlerCount = yield* Ref.make(0)
		const httpClient = HttpClient.make((request) =>
			Effect.gen(function* () {
				const webRequest = yield* HttpClientRequest.toWeb(request).pipe(Effect.orDie)
				yield* Queue.offer(requests, yield* Effect.promise(() => webRequest.text()))
				return HttpClientResponse.fromWeb(
					request,
					new Response('{"ok":true,"channel":"C_TEST","ts":"100.2"}', {
						status: 200,
						headers: { 'content-type': 'application/json' },
					}),
				)
			}),
		)
		const persistence = Persistence.layerMemory
		const subscriptions = Subscriptions.layer.pipe(Layer.provide(persistence))
		const coordinator = ConversationCoordinator.layerMemory()
		const signals = ConversationSignals.layerMemory
		const registry = ProviderRegistry.layer
		const organizations = Organizations.make((input) =>
			Effect.succeed(input.tenant === 'T_TEST' ? Option.some(OrgId.make('org_test')) : Option.none()),
		)
		const common = Layer.mergeAll(
			coordinator,
			signals,
			registry,
			organizations,
			ChannelsGate.layerAllowAll,
			ChannelsObserver.layerLogger,
			subscriptions,
		)
		const core = Layer.merge(Channels.layer(), Ingress.layer).pipe(Layer.provideMerge(common))
		const credentials = SlackTenantCredentials.make({
			load: () => Effect.succeed(Option.some({ botToken: Redacted.make('xoxb-test-token') })),
			save: () => Effect.void,
		})
		const slackClient = SlackClient.layer.pipe(
			Layer.provide(Layer.merge(Layer.succeed(HttpClient.HttpClient, httpClient), credentials)),
		)
		const slackProvider = SlackProvider.layer.pipe(Layer.provide(slackClient))
		const application = Layer.merge(core, slackProvider)
		const testApplication = Layer.merge(application, NodeCrypto.layer)
		const program = Effect.gen(function* () {
			const channels = yield* Channels
			const ingress = yield* Ingress
			const providerRegistry = yield* ProviderRegistry
			const provider = yield* SlackProvider
			yield* providerRegistry.register(provider)
			yield* channels.onNewMention((thread, message) =>
				Effect.gen(function* () {
					yield* Ref.update(handlerCount, (count) => count + 1)
					yield* thread.subscribe()
					yield* thread.post(MarkdownContent.make({ markdown: `echo: ${message.text}` }))
					yield* Deferred.succeed(completed, undefined)
				}),
			)
			const worker = yield* Effect.forkChild(channels.run)

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
			)
			const { dispose, handler } = HttpRouter.toWebHandler(routeLayer, { disableLogger: true })
			yield* Effect.addFinalizer(() => Effect.promise(dispose))

			const callback = yield* Schema.decodeEffect(SlackEventCallback)(appMentionCallback)
			const body = yield* Schema.encodeEffect(Schema.fromJsonString(SlackEventCallback))(callback)
			const currentTime = yield* Clock.currentTimeMillis.pipe(TestClock.withLive)
			const timestamp = Math.floor(currentTime / 1000).toString()
			const signature = yield* signSlackBody(body, timestamp)
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
					Context.make(Ingress, ingress),
				),
			)
			const first = yield* deliver
			const redelivery = yield* deliver
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
