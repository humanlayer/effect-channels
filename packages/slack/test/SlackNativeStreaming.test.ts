import { assert, it } from '@effect/vitest'
import { Effect, Exit, Queue, Schema } from 'effect'

import { MarkdownTextChunk, PlanUpdateChunk, TaskUpdateChunk } from '../src/index'
import { SlackAppendStreamInput, SlackStartStreamInput, SlackStopStreamInput } from '../src/Schema'
import { SlackClient } from '../src/SlackClient'
import { makeSlackClientHarness, slackJsonResponse, testChannelId, testRootTs, testTeamId } from './support'

const StartBody = Schema.Struct({
	channel: Schema.String,
	thread_ts: Schema.String,
	recipient_user_id: Schema.String,
	recipient_team_id: Schema.String,
	chunks: Schema.Array(Schema.Json),
})
const ContinueBody = Schema.Struct({
	channel: Schema.String,
	ts: Schema.String,
	chunks: Schema.Array(Schema.Json),
})

it.effect('encodes native markdown, task, and plan chunks across start, append, and stop', () =>
	Effect.gen(function* () {
		const harness = yield* makeSlackClientHarness((request) =>
			request.url.pathname.endsWith('appendStream')
				? slackJsonResponse('{"ok":true}')
				: slackJsonResponse('{"ok":true,"channel":"C_TEST","ts":"100.2","message":{"user":"U_BOT"}}'),
		)
		yield* Effect.gen(function* () {
			const client = yield* SlackClient
			const stream = yield* client.startStream(
				SlackStartStreamInput.make({
					teamId: testTeamId,
					channelId: testChannelId,
					threadTs: testRootTs,
					recipient: { userId: 'U_TEST', teamId: testTeamId },
					chunks: [MarkdownTextChunk.make({ text: 'Hello ' })],
				}),
			)
			yield* client.appendStream(
				SlackAppendStreamInput.make({
					teamId: testTeamId,
					stream,
					chunks: [
						TaskUpdateChunk.make({ id: 'one', title: 'Work', status: 'in_progress' }),
						PlanUpdateChunk.make({ title: 'Plan' }),
					],
				}),
			)
			const sent = yield* client.stopStream(SlackStopStreamInput.make({ teamId: testTeamId, stream, chunks: [] }))
			assert.strictEqual(sent.ts, '100.2')
		}).pipe(Effect.provide(harness.layer))
		const requests = yield* Queue.takeAll(harness.requests)
		assert.deepStrictEqual(
			requests.map((request) => request.url.pathname),
			['/api/chat.startStream', '/api/chat.appendStream', '/api/chat.stopStream'],
		)
		const startBody = yield* Schema.decodeEffect(Schema.fromJsonString(StartBody))(requests[0]?.body ?? '{}')
		const appendBody = yield* Schema.decodeEffect(Schema.fromJsonString(ContinueBody))(requests[1]?.body ?? '{}')
		assert.deepStrictEqual(startBody, {
			channel: 'C_TEST',
			thread_ts: '100.1',
			recipient_user_id: 'U_TEST',
			recipient_team_id: 'T_TEST',
			chunks: [{ type: 'markdown_text', text: 'Hello ' }],
		})
		assert.deepStrictEqual(appendBody.chunks, [
			{ type: 'task_update', id: 'one', title: 'Work', status: 'in_progress' },
			{ type: 'plan_update', title: 'Plan' },
		])
	}),
)

it.effect('keeps native API failures typed', () =>
	Effect.gen(function* () {
		const harness = yield* makeSlackClientHarness(() =>
			slackJsonResponse('{"ok":false,"error":"feature_not_enabled"}'),
		)
		const exit = yield* Effect.exit(
			Effect.flatMap(SlackClient, (client) =>
				client.startStream({ teamId: testTeamId, channelId: testChannelId, threadTs: testRootTs, chunks: [] }),
			).pipe(Effect.provide(harness.layer)),
		)
		assert.strictEqual(Exit.isFailure(exit), true)
	}),
)
