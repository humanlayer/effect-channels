import { assert, it } from '@effect/vitest'
import { Effect, Layer, Queue, Schema } from 'effect'

import { expectTaggedFailure } from '../../channels/test/support.ts'
import { SlackSessionStatusInput } from '../src/Schema.ts'
import { Slack } from '../src/Slack.ts'
import { SlackClient } from '../src/SlackClient.ts'
import {
	makeSlackClientHarness,
	slackJsonResponse,
	testBotToken,
	testChannelId,
	testRootThreadId,
	testRootTs,
	testTeamId,
} from './support.ts'

const SessionStatusBody = Schema.Struct({
	channel_id: Schema.String,
	thread_ts: Schema.String,
	status: Schema.String,
})

const processing = SlackSessionStatusInput.make({
	teamId: testTeamId,
	channelId: testChannelId,
	threadTs: testRootTs,
	status: 'processing',
})

const setStatus = (input: SlackSessionStatusInput) =>
	Effect.flatMap(SlackClient, (client) => client.setSessionStatus(input))

it.effect('posts agents.sessions.setStatus with the bot token and the thread address', () =>
	Effect.gen(function* () {
		const harness = yield* makeSlackClientHarness(() => slackJsonResponse('{"ok":true}'))
		yield* setStatus(processing).pipe(Effect.provide(harness.layer))
		const request = yield* Queue.take(harness.requests)
		const body = yield* Schema.decodeEffect(Schema.fromJsonString(SessionStatusBody))(request.body)
		assert.strictEqual(request.method, 'POST')
		assert.strictEqual(request.url.href, 'https://slack.com/api/agents.sessions.setStatus')
		assert.strictEqual(request.authorization, `Bearer ${testBotToken}`)
		assert.deepStrictEqual(body, { channel_id: 'C_TEST', thread_ts: '100.1', status: 'processing' })
		assert.strictEqual(yield* Queue.size(harness.requests), 0)
	}),
)

it.effect('surfaces Slack API rejections as SlackApiError with the Slack error code', () =>
	Effect.gen(function* () {
		const harness = yield* makeSlackClientHarness(() => slackJsonResponse('{"ok":false,"error":"not_allowed"}'))
		const error = yield* expectTaggedFailure('SlackApiError')(setStatus(processing)).pipe(
			Effect.provide(harness.layer),
		)
		assert.strictEqual(error.operation, 'agents.sessions.setStatus')
		assert.strictEqual(error.code, 'not_allowed')
	}),
)

it.effect('surfaces non-2xx responses as SlackTransportError with the status', () =>
	Effect.gen(function* () {
		const harness = yield* makeSlackClientHarness(() => slackJsonResponse('server error', 500))
		const error = yield* expectTaggedFailure('SlackTransportError')(setStatus(processing)).pipe(
			Effect.provide(harness.layer),
		)
		assert.strictEqual(error.operation, 'agents.sessions.setStatus')
		assert.strictEqual(error.status, 500)
	}),
)

it.effect('exposes the native Slack.setSessionStatus and narrows failures to StatusFailed', () =>
	Effect.gen(function* () {
		const harness = yield* makeSlackClientHarness((request) =>
			request.body.includes('"suspended"')
				? slackJsonResponse('{"ok":false,"error":"invalid_status"}')
				: slackJsonResponse('{"ok":true}'),
		)
		const slackLayer = Slack.layer.pipe(Layer.provide(harness.layer))
		yield* Effect.gen(function* () {
			const slack = yield* Slack
			yield* slack.setSessionStatus(processing)
			const error = yield* expectTaggedFailure('StatusFailed')(
				slack.setSessionStatus({ ...processing, status: 'suspended' }),
			)
			assert.strictEqual(error.provider, 'slack')
			assert.strictEqual(error.threadId, testRootThreadId)
		}).pipe(Effect.provide(slackLayer))
		const requests = yield* Queue.takeAll(harness.requests)
		assert.deepStrictEqual(
			requests.map((request) => request.url.pathname),
			['/api/agents.sessions.setStatus', '/api/agents.sessions.setStatus'],
		)
	}),
)
