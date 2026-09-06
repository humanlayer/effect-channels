import { assert, it } from '@effect/vitest'
import { ConfigProvider, Effect, Layer, Option, Queue, Redacted, Schema } from 'effect'

import { MarkdownContent, Slack, SlackClient, SlackTenantCredentials } from '../src/index.ts'
import { expectTaggedFailure, testThread } from './nativeSupport.ts'
import { makeSlackClientHarness, slackJsonResponse, testTeamId } from './support.ts'

it.effect('posts through a native Thread with only SlackClient dependencies and no ingress runtime', () =>
	Effect.gen(function* () {
		const harness = yield* makeSlackClientHarness(() =>
			slackJsonResponse('{"ok":true,"channel":"C_TEST","ts":"100.9"}'),
		)
		const sent = yield* testThread
			.post(MarkdownContent.make({ markdown: 'outbound only' }))
			.pipe(Effect.provide(Slack.layer.pipe(Layer.provide(harness.layer))))
		assert.strictEqual(sent.ref.messageRef, '100.9')
		assert.strictEqual(sent.ref.threadId, testThread.ref.id)
		assert.strictEqual(sent.message.text, 'outbound only')
		assert.deepStrictEqual(sent.ref.degraded, [])
		const request = yield* Queue.take(harness.requests)
		assert.strictEqual(request.url.pathname, '/api/chat.postMessage')
		assert.deepStrictEqual(yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Json))(request.body), {
			channel: 'C_TEST',
			thread_ts: '100.1',
			text: 'outbound only',
		})
		assert.strictEqual(yield* Queue.size(harness.requests), 0)
	}),
)

it.effect('exposes the native client API escape hatch with workspace authentication', () =>
	Effect.gen(function* () {
		const harness = yield* makeSlackClientHarness(() =>
			slackJsonResponse('{"ok":true,"scheduled_message_id":"Q1"}'),
		)
		const response = yield* Effect.flatMap(SlackClient, (client) =>
			client.api({
				teamId: testTeamId,
				method: 'chat.scheduleMessage',
				payload: { channel: 'C_TEST', text: 'later', post_at: 1_800_000_000 },
			}),
		).pipe(Effect.provide(harness.layer))
		assert.deepStrictEqual(response, { ok: true, scheduled_message_id: 'Q1' })
		const request = yield* Queue.take(harness.requests)
		assert.strictEqual(request.url.pathname, '/api/chat.scheduleMessage')
		assert.strictEqual(request.authorization, 'Bearer xoxb-test-token')
		assert.deepStrictEqual(yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Json))(request.body), {
			channel: 'C_TEST',
			text: 'later',
			post_at: 1_800_000_000,
		})
	}),
)

it.effect('loads configured credentials and rejects saving them with a typed error', () =>
	Effect.gen(function* () {
		const credentials = yield* SlackTenantCredentials
		const before = Option.getOrThrow(yield* credentials.load({ teamId: testTeamId }))
		assert.strictEqual(Redacted.value(before.botToken), 'xoxb-config-token')
		const error = yield* expectTaggedFailure('CredentialStoreError')(
			credentials.save({ teamId: testTeamId, credentials: { botToken: Redacted.make('xoxb-replacement') } }),
		)
		assert.strictEqual(error.operation, 'save_not_supported')
		assert.strictEqual(error.teamId, testTeamId)
		const after = Option.getOrThrow(yield* credentials.load({ teamId: testTeamId }))
		assert.strictEqual(Redacted.value(after.botToken), 'xoxb-config-token')
	}).pipe(
		Effect.provide(
			SlackTenantCredentials.layerFromConfig.pipe(
				Layer.provide(
					ConfigProvider.layer(ConfigProvider.fromUnknown({ SLACK_BOT_TOKEN: 'xoxb-config-token' })),
				),
			),
		),
	),
)

it.effect('advertises native and post-and-edit streaming without making unsupported channel typing requests', () =>
	Effect.gen(function* () {
		const harness = yield* makeSlackClientHarness(() => {
			throw new Error('unexpected HTTP request')
		})
		for (const streaming of ['native', 'post_and_edit'] as const) {
			yield* Effect.gen(function* () {
				const slack = yield* Slack
				assert.strictEqual(slack.capabilities.streaming, streaming)
				assert.deepStrictEqual(slack.capabilities.typing, { thread: true, channel: false })
				assert.deepStrictEqual(slack.capabilities.directMessages, { ingress: true, open: true })
				assert.deepStrictEqual(slack.capabilities.ephemeral, { native: true, dmFallback: true })
				yield* slack.startChannelTyping({ channel: testThread.ref.channel })
			}).pipe(Effect.provide(Slack.layerWith({ streaming }).pipe(Layer.provide(harness.layer))))
		}
		assert.strictEqual(yield* Queue.size(harness.requests), 0)
	}),
)
