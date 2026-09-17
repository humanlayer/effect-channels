import { Context, Effect, Schema } from 'effect'

import { SlackChannelId, SlackMessageTs, SlackTeamId } from './SlackIdentity'

export const ResolveSlackReactionThreadInput = Schema.Struct({
	teamId: SlackTeamId,
	channelId: SlackChannelId,
	messageTs: SlackMessageTs,
})
export type ResolveSlackReactionThreadInput = typeof ResolveSlackReactionThreadInput.Type

export class SlackReactionThreadResolutionUnavailable extends Schema.TaggedError<SlackReactionThreadResolutionUnavailable>()(
	'SlackReactionThreadResolutionUnavailable',
	{
		reason: Schema.Literals(['unknown_installation', 'transport', 'slack_api', 'invalid_response']),
	},
) {}

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
