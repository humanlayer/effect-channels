/** The message the GitHub callback sends an AgentSession, and what both of its services read from it. */
import { DeliveryId } from '@humanlayer/channels-delivery'
import { GitHubIssue, GitHubPullRequest } from '@humanlayer/channels-github'
import { Schema } from 'effect'

import { RequestComment } from './DiscussionContext'

/** A message for the AgentSession: the prompt, its GitHub discussion, and the handed-off delivery to finish. */
export const AgentSessionMessage = Schema.Struct({
	prompt: Schema.String,
	githubDiscussion: Schema.Union([GitHubIssue, GitHubPullRequest]),
	/** The comments the prompt is made of: the one that mentioned the bot and the others in its batch. */
	requestComments: Schema.optional(Schema.Array(RequestComment)),
	deliveryId: DeliveryId,
	accessToken: Schema.RedactedFromValue(Schema.String),
})
export type AgentSessionMessage = typeof AgentSessionMessage.Type

/** The delivery API's target for the message's delivery: its ID and access token. */
export const targetOf = (message: AgentSessionMessage) => ({
	deliveryId: message.deliveryId,
	accessToken: message.accessToken,
})
