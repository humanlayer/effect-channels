import { Schema } from 'effect'

import { LinearWebhookDeliveryId } from './LinearIdentity'
import { LinearAgentGuidance, LinearAgentPrompt, LinearAgentSessionComment, LinearParticipant } from './LinearModels'
import { LinearAgentSession, LinearComment, LinearIssue } from './LinearResources'
import {
	LinearCommentReactionNotification,
	LinearIssueAssignedNotification,
	LinearIssueCommentMentionNotification,
	LinearIssueMentionNotification,
	LinearIssueNewCommentNotification,
	LinearIssueReactionNotification,
	LinearIssueStatusChangedNotification,
	LinearIssueUnassignedNotification,
} from './LinearWebhookEventSchemas'

const issueActivityFields = {
	eventId: LinearWebhookDeliveryId,
	issue: LinearIssue,
	actor: Schema.NullOr(LinearParticipant),
}

export const LinearIssueOpened = Schema.TaggedStruct('LinearIssueOpened', issueActivityFields)
export type LinearIssueOpened = typeof LinearIssueOpened.Type

export const LinearIssueMention = Schema.TaggedStruct('LinearIssueMention', {
	...issueActivityFields,
	notification: LinearIssueMentionNotification,
})
export type LinearIssueMention = typeof LinearIssueMention.Type

export const LinearCommentMention = Schema.TaggedStruct('LinearCommentMention', {
	...issueActivityFields,
	comment: LinearComment,
	notification: LinearIssueCommentMentionNotification,
})
export type LinearCommentMention = typeof LinearCommentMention.Type

export const LinearAssignmentNotification = Schema.TaggedStruct('LinearAssignmentNotification', {
	...issueActivityFields,
	notification: LinearIssueAssignedNotification,
})
export type LinearAssignmentNotification = typeof LinearAssignmentNotification.Type

export const LinearIssueUnassigned = Schema.TaggedStruct('LinearIssueUnassignedNotification', {
	...issueActivityFields,
	notification: LinearIssueUnassignedNotification,
})
export const LinearIssueNewComment = Schema.TaggedStruct('LinearIssueNewCommentNotification', {
	...issueActivityFields,
	comment: LinearComment,
	notification: LinearIssueNewCommentNotification,
})
export const LinearIssueStatusChanged = Schema.TaggedStruct('LinearIssueStatusChangedNotification', {
	...issueActivityFields,
	notification: LinearIssueStatusChangedNotification,
})
export const LinearIssueReaction = Schema.TaggedStruct('LinearIssueReactionNotification', {
	...issueActivityFields,
	notification: LinearIssueReactionNotification,
})
export const LinearCommentReaction = Schema.TaggedStruct('LinearCommentReactionNotification', {
	...issueActivityFields,
	comment: LinearComment,
	notification: LinearCommentReactionNotification,
})

export const LinearIssueActivity = Schema.Union([
	LinearIssueOpened,
	LinearIssueMention,
	LinearCommentMention,
	LinearAssignmentNotification,
	LinearIssueUnassigned,
	LinearIssueNewComment,
	LinearIssueStatusChanged,
	LinearIssueReaction,
	LinearCommentReaction,
])
export type LinearIssueActivity = typeof LinearIssueActivity.Type

const activationFields = {
	issue: LinearIssue,
	events: Schema.Array(LinearIssueActivity),
}

export const LinearIssueCreated = Schema.TaggedStruct('LinearIssueCreated', {
	...activationFields,
	trigger: LinearIssueOpened,
})
export type LinearIssueCreated = typeof LinearIssueCreated.Type

export const LinearIssueMentioned = Schema.TaggedStruct('LinearIssueMentioned', {
	...activationFields,
	trigger: LinearIssueMention,
})
export type LinearIssueMentioned = typeof LinearIssueMentioned.Type

export const LinearCommentMentioned = Schema.TaggedStruct('LinearCommentMentioned', {
	...activationFields,
	trigger: LinearCommentMention,
})
export type LinearCommentMentioned = typeof LinearCommentMentioned.Type

export const LinearMentioned = Schema.Union([LinearIssueMentioned, LinearCommentMentioned])
export type LinearMentioned = typeof LinearMentioned.Type

export const LinearIssueAssigned = Schema.TaggedStruct('LinearIssueAssigned', {
	...activationFields,
	trigger: LinearAssignmentNotification,
})
export type LinearIssueAssigned = typeof LinearIssueAssigned.Type

export const LinearAgentSessionCreated = Schema.TaggedStruct('LinearAgentSessionCreated', {
	session: LinearAgentSession,
	issue: LinearIssue,
	promptContext: Schema.NullOr(Schema.String),
	previousComments: Schema.Array(LinearAgentSessionComment),
	guidance: Schema.Array(LinearAgentGuidance),
	deliveryId: LinearWebhookDeliveryId,
})
export type LinearAgentSessionCreated = typeof LinearAgentSessionCreated.Type

export const LinearAgentSessionPrompted = Schema.TaggedStruct('LinearAgentSessionPrompted', {
	session: LinearAgentSession,
	issue: LinearIssue,
	prompt: LinearAgentPrompt,
	deliveryId: LinearWebhookDeliveryId,
})
export type LinearAgentSessionPrompted = typeof LinearAgentSessionPrompted.Type

/** The single source of truth for callback names and callback argument shapes. */
export interface LinearCallbackEventMap {
	readonly onAgentSessionCreated: LinearAgentSessionCreated
	readonly onAgentSessionPrompted: LinearAgentSessionPrompted
	readonly onIssueCreated: LinearIssueCreated
	readonly onMentioned: LinearMentioned
	readonly onAssigned: LinearIssueAssigned
}
