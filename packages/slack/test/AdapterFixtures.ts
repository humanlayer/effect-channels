import { Redacted, Schema } from 'effect'

import { IdempotencyKey } from '../src/Model'
import { SlackConnection } from '../src/Schema'
import { SlackChannelId, SlackMessageTs, SlackTeamId } from '../src/SlackIdentity'
import { SlackDirectMessageRoute } from '../src/SlackSubscriptions'
import { slackDmConversationRef, slackThreadRef } from '../src/SlackThreadId'

export const workspaceId = SlackTeamId.make('T-adapter-{other}:😀')
export const installation = SlackConnection.make({
	credentials: { botToken: Redacted.make('private-token-sentinel'), botUserId: 'U-bot', botId: 'B-bot' },
})
export const rotatedInstallation = SlackConnection.make({
	credentials: { botToken: Redacted.make('private-rotated-sentinel'), botUserId: 'U-rotated', botId: 'B-rotated' },
})
export const installationJson = JSON.stringify({
	credentials: { botToken: 'private-token-sentinel', botUserId: 'U-bot', botId: 'B-bot' },
})
export const rootedThread = slackThreadRef(
	{
		teamId: workspaceId,
		channelId: SlackChannelId.make('D-adapter'),
		threadTs: SlackMessageTs.make('1.000001'),
		directMessageKind: 'im',
	},
	true,
)
export const proactiveThread = slackDmConversationRef(workspaceId, SlackChannelId.make('D-adapter'), 'im')
export const routeInput = {
	eventId: IdempotencyKey.make('evt_00000000000000000000000000000001'),
	rootedThread,
	proactiveThread,
}
export const encodeRoute = Schema.encodeSync(Schema.fromJsonString(SlackDirectMessageRoute))
