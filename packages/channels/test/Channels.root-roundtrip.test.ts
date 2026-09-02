import { NodeCrypto } from '@effect/platform-node'
import { assert, it } from '@effect/vitest'
import { Deferred, Effect, Fiber, Layer, Option, Queue, Redacted, Ref, Schema } from 'effect'
import { HttpClient, HttpClientRequest, HttpClientResponse } from 'effect/unstable/http'
import { Persistence } from 'effect/unstable/persistence'

import { SlackEventCallback } from '../../slack/src/Schema.ts'
import { SlackClient } from '../../slack/src/SlackClient.ts'
import { normalizeSlackMessage } from '../../slack/src/SlackNormalize.ts'
import { SlackProvider } from '../../slack/src/SlackProvider.ts'
import { SlackTenantCredentials } from '../../slack/src/SlackTenantCredentials.ts'
import { appMentionCallback } from '../../slack/test/support.ts'
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
} from '../src/index.ts'

const SlackPostBody = Schema.Struct({
	channel: Schema.String,
	thread_ts: Schema.String,
	text: Schema.String,
})

it.effect('delivers one mention, subscribes explicitly, and posts in the Slack root thread', () =>
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
			const callback = yield* Schema.decodeEffect(SlackEventCallback)(appMentionCallback)
			const normalized = yield* normalizeSlackMessage({
				callback,
				botUserId: 'U_BOT',
			})
			yield* ingress.acceptMessage(normalized)
			yield* ingress.acceptMessage(normalized)
			yield* Deferred.await(completed)
			const requestBody = yield* Queue.take(requests)
			const decodedBody = yield* Schema.decodeEffect(Schema.fromJsonString(SlackPostBody))(requestBody)
			const subscribed = yield* channels.isSubscribed({ threadId: normalized.thread.ref.id })
			const count = yield* Ref.get(handlerCount)
			yield* Fiber.interrupt(worker)

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
