import { assert, it } from '@effect/vitest'
import { Effect, Layer, Option, Queue, Redacted, Schema } from 'effect'
import { HttpClient, HttpClientRequest, HttpClientResponse } from 'effect/unstable/http'

import { SlackChannelId, SlackMessageTs, SlackPostMessageInput, SlackTeamId } from '../src/Schema.ts'
import { SlackClient } from '../src/SlackClient.ts'
import { SlackTenantCredentials } from '../src/SlackTenantCredentials.ts'

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
		assert.deepStrictEqual(sent, { channelId: 'C_TEST', ts: '100.2' })
	}),
)
