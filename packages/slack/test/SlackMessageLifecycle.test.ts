import { NodeCrypto } from '@effect/platform-node'
import { assert, it } from '@effect/vitest'
import { Effect, Queue, Schema } from 'effect'

import { SlackEventCallback, SlackMessageTs } from '../src/Schema.ts'
import { SlackClient } from '../src/SlackClient.ts'
import { normalizeSlackMessageDeleted, normalizeSlackMessageUpdated } from '../src/SlackNormalize.ts'
import { makeSlackClientHarness, slackJsonResponse, testChannelId, testTeamId } from './support.ts'

const changed = {
	type: 'event_callback' as const,
	team_id: 'T_TEST',
	event_id: 'Ev_CHANGED',
	event_time: 1_788_000_000,
	event: {
		type: 'message' as const,
		subtype: 'message_changed',
		channel: 'C_TEST',
		text: 'edited',
		ts: '101.1',
		message: { user: 'U_HUMAN', text: 'edited', ts: '100.2', thread_ts: '100.1' },
		previous_message: { user: 'U_HUMAN', text: 'before', ts: '100.2', thread_ts: '100.1' },
	},
}

it.effect('normalizes Slack message changes and deletions', () =>
	Effect.gen(function* () {
		const callback = yield* Schema.decodeEffect(SlackEventCallback)(changed)
		const updated = yield* normalizeSlackMessageUpdated({ callback, identity: { botUserId: 'U_BOT' } })
		assert.strictEqual(updated.thread.ref.id, 'slack:v1:T_TEST:C_TEST:100.1')
		assert.strictEqual(updated.message.text, 'edited')
		assert.strictEqual(updated.previousMessage?.text, 'before')
		assert.ok(updated.message.metadata.editedAt !== undefined)

		const { message: _message, ...deletedEvent } = changed.event
		const deletedCallback = yield* Schema.decodeEffect(SlackEventCallback)({
			...changed,
			event_id: 'Ev_DELETED',
			event: { ...deletedEvent, subtype: 'message_deleted', deleted_ts: '100.2' },
		})
		const deleted = yield* normalizeSlackMessageDeleted({
			callback: deletedCallback,
			identity: { botUserId: 'U_BOT' },
		})
		assert.strictEqual(deleted.threadRef.id, 'slack:v1:T_TEST:C_TEST:100.1')
		assert.strictEqual(deleted.messageRef, '100.2')
		assert.strictEqual(deleted.previousMessage?.text, 'before')
	}).pipe(Effect.provide(NodeCrypto.layer)),
)

it.effect('preserves direct-message namespaces for changes and deletions', () =>
	Effect.gen(function* () {
		const callback = yield* Schema.decodeEffect(SlackEventCallback)({
			...changed,
			event: { ...changed.event, channel: 'G_MPIM', channel_type: 'mpim' },
		})
		const updated = yield* normalizeSlackMessageUpdated({ callback, identity: { botUserId: 'U_BOT' } })
		assert.strictEqual(updated.thread.ref.id, 'slack:v1:T_TEST:mpim:G_MPIM:100.1')
		assert.strictEqual(updated.thread.ref.channel.isDm, true)
		assert.strictEqual(updated.directMessageThread?.id, 'slack:v1:T_TEST:mpim:G_MPIM')

		const { message: _message, ...deletedEvent } = changed.event
		const deletedCallback = yield* Schema.decodeEffect(SlackEventCallback)({
			...changed,
			event: {
				...deletedEvent,
				channel: 'G_MPIM',
				channel_type: 'mpim',
				subtype: 'message_deleted',
				deleted_ts: '100.2',
			},
		})
		const deleted = yield* normalizeSlackMessageDeleted({
			callback: deletedCallback,
			identity: { botUserId: 'U_BOT' },
		})
		assert.strictEqual(deleted.threadRef.id, 'slack:v1:T_TEST:mpim:G_MPIM:100.1')
		assert.strictEqual(deleted.threadRef.channel.isDm, true)
		assert.strictEqual(deleted.directMessageThread?.id, 'slack:v1:T_TEST:mpim:G_MPIM')
	}).pipe(Effect.provide(NodeCrypto.layer)),
)

it.effect('calls chat.update and chat.delete with exact message references', () =>
	Effect.gen(function* () {
		const harness = yield* makeSlackClientHarness((request) =>
			request.url.pathname.endsWith('/chat.update')
				? slackJsonResponse(JSON.stringify({ ok: true, channel: 'C_TEST', ts: '100.2' }))
				: slackJsonResponse(JSON.stringify({ ok: true })),
		)
		const program = Effect.gen(function* () {
			const client = yield* SlackClient
			const ts = SlackMessageTs.make('100.2')
			yield* client.updateMessage({ teamId: testTeamId, channelId: testChannelId, ts, text: 'edited' })
			yield* client.deleteMessage({ teamId: testTeamId, channelId: testChannelId, ts })
			const requests = yield* Queue.takeAll(harness.requests)
			assert.deepStrictEqual(
				requests.map((request) => request.url.pathname),
				['/api/chat.update', '/api/chat.delete'],
			)
			assert.match(requests[0]?.body ?? '', /text=edited/)
			assert.match(requests[1]?.body ?? '', /ts=100.2/)
		})
		yield* program.pipe(Effect.provide(harness.layer))
	}),
)
