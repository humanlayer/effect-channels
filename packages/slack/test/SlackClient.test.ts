import { assert, it } from '@effect/vitest'
import { Effect, Layer, Option, Queue, Redacted, Schema } from 'effect'
import { HttpClient, HttpClientRequest, HttpClientResponse } from 'effect/unstable/http'

import { SlackTransportError } from '../src/Errors.js'
import { SlackChannelId, SlackMessageTs, SlackPostMessageInput, SlackTeamId } from '../src/Schema.js'
import { SlackClient } from '../src/SlackClient.js'
import { SlackTenantCredentials } from '../src/SlackTenantCredentials.js'

const SlackPostBody = Schema.Struct({
	channel: Schema.String,
	thread_ts: Schema.String,
	text: Schema.String,
})

it.effect('encodes authenticated threaded chat.postMessage requests', () =>
	Effect.gen(function* () {
		const requests = yield* Queue.unbounded<{ readonly authorization: string | null; readonly body: string }>()
		const httpClient = HttpClient.make((request) =>
			Effect.gen(function* () {
				const webRequest = yield* HttpClientRequest.toWeb(request).pipe(Effect.orDie)
				const body = yield* Effect.promise(() => webRequest.text())
				yield* Queue.offer(requests, {
					authorization: webRequest.headers.get('authorization'),
					body,
				})
				return HttpClientResponse.fromWeb(
					request,
					new Response('{"ok":true,"channel":"C_TEST","ts":"100.2"}', {
						status: 200,
						headers: { 'content-type': 'application/json' },
					}),
				)
			}),
		)
		const credentials = SlackTenantCredentials.make({
			load: () =>
				Effect.succeed(
					Option.some({
						botToken: Redacted.make('xoxb-test-token'),
					}),
				),
			save: () => Effect.void,
		})
		const dependencies = Layer.merge(Layer.succeed(HttpClient.HttpClient, httpClient), credentials)
		const clientLayer = SlackClient.layer.pipe(Layer.provide(dependencies))
		const sent = yield* Effect.flatMap(SlackClient, (client) =>
			client.postMessage(
				SlackPostMessageInput.make({
					teamId: SlackTeamId.make('T_TEST'),
					channelId: SlackChannelId.make('C_TEST'),
					threadTs: SlackMessageTs.make('100.1'),
					text: 'threaded reply',
				}),
			),
		).pipe(Effect.provide(clientLayer))
		const recorded = yield* Queue.take(requests)
		const body = yield* Schema.decodeEffect(Schema.fromJsonString(SlackPostBody))(recorded.body)

		assert.strictEqual(recorded.authorization, 'Bearer xoxb-test-token')
		assert.deepStrictEqual(body, { channel: 'C_TEST', thread_ts: '100.1', text: 'threaded reply' })
		assert.deepStrictEqual(sent, { channelId: SlackChannelId.make('C_TEST'), ts: SlackMessageTs.make('100.2') })
	}),
)

it.effect('decodes Retry-After seconds from a Slack rate-limit response', () =>
	Effect.gen(function* () {
		const httpClient = HttpClient.make((request) =>
			Effect.succeed(
				HttpClientResponse.fromWeb(
					request,
					new Response('{"ok":false,"error":"ratelimited"}', {
						status: 429,
						headers: { 'content-type': 'application/json', 'retry-after': '2.5' },
					}),
				),
			),
		)
		const credentials = SlackTenantCredentials.make({
			load: () => Effect.succeed(Option.some({ botToken: Redacted.make('xoxb-test-token') })),
			save: () => Effect.void,
		})
		const clientLayer = SlackClient.layer.pipe(
			Layer.provide(Layer.merge(Layer.succeed(HttpClient.HttpClient, httpClient), credentials)),
		)
		const error = yield* Effect.flip(
			Effect.flatMap(SlackClient, (client) =>
				client.postMessage(
					SlackPostMessageInput.make({
						teamId: SlackTeamId.make('T_TEST'),
						channelId: SlackChannelId.make('C_TEST'),
						threadTs: SlackMessageTs.make('100.1'),
						text: 'threaded reply',
					}),
				),
			).pipe(Effect.provide(clientLayer)),
		)
		assert(Schema.is(SlackTransportError)(error))
		assert.strictEqual(error.status, 429)
		assert.strictEqual(error.retryAfterMs, 2500)
	}),
)
