import { NodeCrypto } from '@effect/platform-node'
import { assert, it } from '@effect/vitest'
import { Effect, Queue, Schema } from 'effect'

import { SlackEventCallback, SlackMessageTs } from '../src/Schema.js'
import { SlackClient } from '../src/SlackClient.js'
import { normalizeSlackReaction } from '../src/SlackNormalize.js'
import {
	makeSlackClientHarness,
	reactionAddedCallback,
	slackJsonResponse,
	testChannelId,
	testTeamId,
} from './support.js'

it.effect('normalizes canonical and custom Slack reactions', () =>
	Effect.gen(function* () {
		const callback = yield* Schema.decodeEffect(SlackEventCallback)(reactionAddedCallback)
		const canonical = yield* normalizeSlackReaction({ callback, identity: { botUserId: 'U_BOT' } })
		assert.strictEqual(canonical.change._tag, 'ReactionAdded')
		assert.strictEqual(canonical.emoji.name, 'thumbs_up')
		assert.strictEqual(canonical.rawEmoji, 'thumbsup')

		const customCallback = yield* Schema.decodeEffect(SlackEventCallback)({
			...reactionAddedCallback,
			event_id: 'Ev_CUSTOM',
			event: { ...reactionAddedCallback.event, reaction: 'party_parrot' },
		})
		const custom = yield* normalizeSlackReaction({ callback: customCallback, identity: { botUserId: 'U_BOT' } })
		assert.strictEqual(custom.emoji.name, 'party_parrot')
		assert.strictEqual(custom.emoji.unicode, undefined)

		const replyReaction = yield* normalizeSlackReaction({
			callback: customCallback,
			identity: { botUserId: 'U_BOT' },
			parentThreadTs: SlackMessageTs.make('99.1'),
		})
		assert.strictEqual(replyReaction.thread.ref.id, 'slack:v1:T_TEST:C_TEST:99.1')
		assert.strictEqual(replyReaction.messageRef, '100.1')

		const dmReaction = yield* normalizeSlackReaction({
			callback,
			identity: { botUserId: 'U_BOT' },
			directMessageKind: 'mpim',
		})
		assert.strictEqual(dmReaction.thread.ref.id, 'slack:v1:T_TEST:mpim:C_TEST:100.1')
		assert.strictEqual(dmReaction.thread.ref.channel.isDm, true)
		assert.strictEqual(dmReaction.directMessageThread?.id, 'slack:v1:T_TEST:mpim:C_TEST')
	}).pipe(Effect.provide(NodeCrypto.layer)),
)

it.effect('calls reactions.add and reactions.remove', () =>
	Effect.gen(function* () {
		const harness = yield* makeSlackClientHarness(() => slackJsonResponse(JSON.stringify({ ok: true })))
		const program = Effect.gen(function* () {
			const client = yield* SlackClient
			const input = {
				teamId: testTeamId,
				channelId: testChannelId,
				ts: SlackMessageTs.make('100.2'),
				emoji: 'thumbsup',
			}
			yield* client.addReaction(input)
			yield* client.removeReaction(input)
			const requests = yield* Queue.takeAll(harness.requests)
			assert.deepStrictEqual(
				requests.map((request) => request.url.pathname),
				['/api/reactions.add', '/api/reactions.remove'],
			)
			assert.ok(requests.every((request) => (request.body ?? '').includes('name=thumbsup')))
		})
		yield* program.pipe(Effect.provide(harness.layer))
	}),
)
