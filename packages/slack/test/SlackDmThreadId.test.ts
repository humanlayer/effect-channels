import { assert, it } from '@effect/vitest'
import { Effect } from 'effect'

import { SlackChannelId, SlackMessageTs, SlackTeamId, SlackThreadRef } from '../src/Schema.js'
import {
	SlackDmConversationTs,
	decodeSlackThreadId,
	encodeSlackThreadId,
	slackDmConversationRef,
} from '../src/SlackThreadId.js'

const teamId = SlackTeamId.make('T_TEST')
const channelId = SlackChannelId.make('D_TEST')

it.effect('round-trips Agent View DM roots and conversation-scoped proactive IDs', () =>
	Effect.gen(function* () {
		const root = SlackThreadRef.make({
			teamId,
			channelId,
			threadTs: SlackMessageTs.make('100.1'),
			directMessageKind: 'im',
		})
		const rootId = encodeSlackThreadId(root)
		const conversation = slackDmConversationRef(teamId, channelId, 'im')

		assert.strictEqual(rootId, 'slack:v1:T_TEST:im:D_TEST:100.1')
		assert.strictEqual(conversation.id, 'slack:v1:T_TEST:im:D_TEST')
		assert.deepStrictEqual(yield* decodeSlackThreadId(rootId), root)
		assert.deepStrictEqual(yield* decodeSlackThreadId(conversation.id), {
			teamId,
			channelId,
			threadTs: SlackDmConversationTs,
			directMessageKind: 'im',
		})
		assert.strictEqual(conversation.channel.isDm, true)
	}),
)

it.effect('keeps DM, MPIM, and channel thread identities in disjoint canonical namespaces', () =>
	Effect.sync(() => {
		const common = { teamId, channelId, threadTs: SlackMessageTs.make('100.1') }
		const channel = encodeSlackThreadId(SlackThreadRef.make(common))
		const dm = encodeSlackThreadId(SlackThreadRef.make({ ...common, directMessageKind: 'im' }))
		const mpim = encodeSlackThreadId(SlackThreadRef.make({ ...common, directMessageKind: 'mpim' }))
		assert.strictEqual(new Set([channel, dm, mpim]).size, 3)
	}),
)
