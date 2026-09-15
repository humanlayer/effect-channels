import { Context, Data, Effect } from 'effect'

import type { SlackChannelId, SlackMessageTs, SlackTeamId } from './SlackIdentity'

export type ResolveSlackReactionThreadInput = {
	readonly teamId: SlackTeamId
	readonly channelId: SlackChannelId
	readonly messageTs: SlackMessageTs
}

export class SlackReactionThreadResolutionUnavailable extends Data.TaggedError(
	'SlackReactionThreadResolutionUnavailable',
)<{
	readonly reason: 'unknown_installation' | 'transport' | 'slack_api' | 'invalid_response'
}> {}

/**
 * Resolves the root thread timestamp for a message receiving a reaction.
 * Slack reaction webhooks contain the reacted-to message timestamp, which may identify a thread reply.
 */
export class SlackReactionThreadResolver extends Context.Service<
	SlackReactionThreadResolver,
	{
		readonly resolve: (
			input: ResolveSlackReactionThreadInput,
		) => Effect.Effect<SlackMessageTs, SlackReactionThreadResolutionUnavailable>
	}
>()('@humanlayer/channels-slack-next/SlackReactionThreadResolver') {}
