import { Schema } from 'effect'

import {
	LinearAgentActivityId,
	LinearAgentSessionId,
	LinearCommentId,
	LinearIssueId,
	LinearNotificationId,
	LinearOrganizationId,
	LinearTeamId,
	LinearUserId,
} from './LinearIdentity'

export const LinearWebhookActor = Schema.Struct({
	__typename: Schema.optionalKey(Schema.Literal('UserChildWebhookPayload')),
	id: LinearUserId,
	name: Schema.String,
	email: Schema.optionalKey(Schema.NullOr(Schema.String)),
	url: Schema.optionalKey(Schema.NullOr(Schema.String)),
	avatarUrl: Schema.optionalKey(Schema.NullOr(Schema.String)),
})

export const LinearWebhookTeam = Schema.Struct({
	__typename: Schema.optionalKey(Schema.Literal('TeamChildWebhookPayload')),
	id: LinearTeamId,
	key: Schema.NonEmptyString,
	name: Schema.String,
})

export const LinearWebhookIssue = Schema.Struct({
	id: LinearIssueId,
	identifier: Schema.NonEmptyString,
	number: Schema.Int,
	title: Schema.String,
	description: Schema.optionalKey(Schema.NullOr(Schema.String)),
	priority: Schema.optionalKey(Schema.Int),
	url: Schema.String,
	team: LinearWebhookTeam,
	creator: Schema.optionalKey(Schema.NullOr(LinearWebhookActor)),
})

export const LinearNotificationIssue = Schema.Struct({
	__typename: Schema.optionalKey(Schema.Literal('IssueWithDescriptionChildWebhookPayload')),
	id: LinearIssueId,
	teamId: LinearTeamId,
	url: Schema.String,
	description: Schema.optionalKey(Schema.NullOr(Schema.String)),
	identifier: Schema.NonEmptyString,
	title: Schema.String,
	team: LinearWebhookTeam,
})

export const LinearNotificationComment = Schema.Struct({
	__typename: Schema.optionalKey(Schema.Literal('CommentChildWebhookPayload')),
	id: LinearCommentId,
	documentContentId: Schema.optionalKey(Schema.NullOr(Schema.NonEmptyString)),
	initiativeUpdateId: Schema.optionalKey(Schema.NullOr(Schema.NonEmptyString)),
	issueId: Schema.optionalKey(Schema.NullOr(LinearIssueId)),
	projectUpdateId: Schema.optionalKey(Schema.NullOr(Schema.NonEmptyString)),
	userId: Schema.optionalKey(Schema.NullOr(LinearUserId)),
	body: Schema.String,
})

const notificationFields = {
	id: LinearNotificationId,
	actorId: Schema.optionalKey(Schema.NullOr(LinearUserId)),
	externalUserActorId: Schema.optionalKey(Schema.NullOr(Schema.NonEmptyString)),
	issueId: LinearIssueId,
	userId: LinearUserId,
	actor: Schema.optionalKey(Schema.NullOr(LinearWebhookActor)),
	issue: LinearNotificationIssue,
	archivedAt: Schema.optionalKey(Schema.NullOr(Schema.String)),
	createdAt: Schema.String,
	updatedAt: Schema.String,
}

const commentNotificationFields = {
	...notificationFields,
	commentId: LinearCommentId,
	parentCommentId: Schema.optionalKey(Schema.NullOr(LinearCommentId)),
	comment: LinearNotificationComment,
	parentComment: Schema.optionalKey(Schema.NullOr(LinearNotificationComment)),
}

export const LinearIssueMentionNotification = Schema.Struct({
	...notificationFields,
	__typename: Schema.optionalKey(Schema.Literal('IssueMentionNotificationWebhookPayload')),
	type: Schema.Literal('issueMention'),
})
export const LinearIssueCommentMentionNotification = Schema.Struct({
	...commentNotificationFields,
	__typename: Schema.optionalKey(Schema.Literal('IssueCommentMentionNotificationWebhookPayload')),
	type: Schema.Literal('issueCommentMention'),
})
export const LinearIssueAssignedNotification = Schema.Struct({
	...notificationFields,
	__typename: Schema.optionalKey(Schema.Literal('IssueAssignedToYouNotificationWebhookPayload')),
	type: Schema.Literal('issueAssignedToYou'),
})
export const LinearIssueUnassignedNotification = Schema.Struct({
	...notificationFields,
	__typename: Schema.optionalKey(Schema.Literal('IssueUnassignedFromYouNotificationWebhookPayload')),
	type: Schema.Literal('issueUnassignedFromYou'),
})
export const LinearIssueNewCommentNotification = Schema.Struct({
	...commentNotificationFields,
	__typename: Schema.optionalKey(Schema.Literal('IssueNewCommentNotificationWebhookPayload')),
	type: Schema.Literal('issueNewComment'),
})
export const LinearIssueStatusChangedNotification = Schema.Struct({
	...notificationFields,
	__typename: Schema.optionalKey(Schema.Literal('IssueStatusChangedNotificationWebhookPayload')),
	type: Schema.Literal('issueStatusChanged'),
})
export const LinearIssueReactionNotification = Schema.Struct({
	...notificationFields,
	__typename: Schema.optionalKey(Schema.Literal('IssueEmojiReactionNotificationWebhookPayload')),
	type: Schema.Literal('issueEmojiReaction'),
	reactionEmoji: Schema.String,
})
export const LinearCommentReactionNotification = Schema.Struct({
	...commentNotificationFields,
	__typename: Schema.optionalKey(Schema.Literal('IssueCommentReactionNotificationWebhookPayload')),
	type: Schema.Literal('issueCommentReaction'),
	reactionEmoji: Schema.String,
})

export const LinearAppNotification = Schema.Union([
	LinearIssueMentionNotification,
	LinearIssueCommentMentionNotification,
	LinearIssueAssignedNotification,
	LinearIssueUnassignedNotification,
	LinearIssueNewCommentNotification,
	LinearIssueStatusChangedNotification,
	LinearIssueReactionNotification,
	LinearCommentReactionNotification,
])

const appUserNotificationFields = {
	type: Schema.Literal('AppUserNotification'),
	organizationId: LinearOrganizationId,
	oauthClientId: Schema.NonEmptyString,
	appUserId: LinearUserId,
	// The current webhook SDL includes these fields, while Linear's agent guide and
	// observed Inbox Notification deliveries omit them. Linear-Delivery and
	// Linear-Timestamp remain the authoritative ingress values either way.
	webhookId: Schema.optionalKey(Schema.NonEmptyString),
	webhookTimestamp: Schema.optionalKey(Schema.Number),
	createdAt: Schema.String,
}

export const LinearIssueMentionWebhook = Schema.Struct({
	...appUserNotificationFields,
	action: Schema.Literal('issueMention'),
	notification: LinearIssueMentionNotification,
})
export const LinearIssueCommentMentionWebhook = Schema.Struct({
	...appUserNotificationFields,
	action: Schema.Literal('issueCommentMention'),
	notification: LinearIssueCommentMentionNotification,
})
export const LinearIssueAssignedWebhook = Schema.Struct({
	...appUserNotificationFields,
	action: Schema.Literal('issueAssignedToYou'),
	notification: LinearIssueAssignedNotification,
})
export const LinearIssueUnassignedWebhook = Schema.Struct({
	...appUserNotificationFields,
	action: Schema.Literal('issueUnassignedFromYou'),
	notification: LinearIssueUnassignedNotification,
})
export const LinearIssueNewCommentWebhook = Schema.Struct({
	...appUserNotificationFields,
	action: Schema.Literal('issueNewComment'),
	notification: LinearIssueNewCommentNotification,
})
export const LinearIssueStatusChangedWebhook = Schema.Struct({
	...appUserNotificationFields,
	action: Schema.Literal('issueStatusChanged'),
	notification: LinearIssueStatusChangedNotification,
})
export const LinearIssueReactionWebhook = Schema.Struct({
	...appUserNotificationFields,
	action: Schema.Literal('issueEmojiReaction'),
	notification: LinearIssueReactionNotification,
})
export const LinearCommentReactionWebhook = Schema.Struct({
	...appUserNotificationFields,
	action: Schema.Literal('issueCommentReaction'),
	notification: LinearCommentReactionNotification,
})

export const LinearAppUserNotificationWebhook = Schema.Union([
	LinearIssueMentionWebhook,
	LinearIssueCommentMentionWebhook,
	LinearIssueAssignedWebhook,
	LinearIssueUnassignedWebhook,
	LinearIssueNewCommentWebhook,
	LinearIssueStatusChangedWebhook,
	LinearIssueReactionWebhook,
	LinearCommentReactionWebhook,
])
export type LinearAppUserNotificationWebhook = typeof LinearAppUserNotificationWebhook.Type

export const LinearIssueCreateWebhook = Schema.Struct({
	action: Schema.Literal('create'),
	type: Schema.Literal('Issue'),
	organizationId: LinearOrganizationId,
	data: LinearWebhookIssue,
	actor: Schema.optionalKey(Schema.NullOr(LinearWebhookActor)),
	webhookId: Schema.optionalKey(Schema.NonEmptyString),
	webhookTimestamp: Schema.optionalKey(Schema.Number),
	createdAt: Schema.optionalKey(Schema.String),
})
export type LinearIssueCreateWebhook = typeof LinearIssueCreateWebhook.Type

export const LinearAgentGuidanceOrigin = Schema.Struct({
	type: Schema.NonEmptyString,
})

export const LinearAgentGuidanceWebhook = Schema.Struct({
	body: Schema.String,
	origin: LinearAgentGuidanceOrigin,
})

export const LinearAgentSessionWebhook = Schema.Struct({
	id: LinearAgentSessionId,
	appUserId: LinearUserId,
	organizationId: LinearOrganizationId,
	createdAt: Schema.String,
	archivedAt: Schema.optionalKey(Schema.NullOr(Schema.String)),
	endedAt: Schema.optionalKey(Schema.NullOr(Schema.String)),
	commentId: Schema.optionalKey(Schema.NullOr(LinearCommentId)),
	comment: Schema.optionalKey(Schema.NullOr(LinearNotificationComment)),
	creatorId: Schema.optionalKey(Schema.NullOr(LinearUserId)),
	creator: Schema.optionalKey(Schema.NullOr(LinearWebhookActor)),
	issueId: Schema.optionalKey(Schema.NullOr(LinearIssueId)),
	issue: Schema.optionalKey(Schema.NullOr(LinearNotificationIssue)),
	sourceCommentId: Schema.optionalKey(Schema.NullOr(LinearCommentId)),
	sourceMetadata: Schema.optionalKey(Schema.NullOr(Schema.Unknown)),
})

export const LinearAgentPromptContent = Schema.Struct({
	type: Schema.Literal('prompt'),
	body: Schema.String,
	title: Schema.optionalKey(Schema.NullOr(Schema.String)),
})

export const LinearAgentPromptActivityWebhook = Schema.Struct({
	id: LinearAgentActivityId,
	agentSessionId: LinearAgentSessionId,
	content: LinearAgentPromptContent,
	createdAt: Schema.String,
	updatedAt: Schema.String,
	archivedAt: Schema.optionalKey(Schema.NullOr(Schema.String)),
	signal: Schema.optionalKey(Schema.NullOr(Schema.String)),
	signalMetadata: Schema.optionalKey(Schema.NullOr(Schema.Unknown)),
	sourceCommentId: Schema.optionalKey(Schema.NullOr(LinearCommentId)),
	userId: LinearUserId,
	user: LinearWebhookActor,
})

const agentSessionEventFields = {
	type: Schema.Literal('AgentSessionEvent'),
	organizationId: LinearOrganizationId,
	oauthClientId: Schema.NonEmptyString,
	appUserId: LinearUserId,
	createdAt: Schema.String,
	webhookId: Schema.NonEmptyString,
	webhookTimestamp: Schema.Number,
	agentSession: LinearAgentSessionWebhook,
	guidance: Schema.optionalKey(Schema.NullOr(Schema.Array(LinearAgentGuidanceWebhook))),
	previousComments: Schema.optionalKey(Schema.NullOr(Schema.Array(LinearNotificationComment))),
}

export const LinearAgentSessionCreatedWebhook = Schema.Struct({
	...agentSessionEventFields,
	action: Schema.Literal('created'),
	agentActivity: Schema.optionalKey(Schema.NullOr(LinearAgentPromptActivityWebhook)),
	promptContext: Schema.optionalKey(Schema.NullOr(Schema.String)),
})
export type LinearAgentSessionCreatedWebhook = typeof LinearAgentSessionCreatedWebhook.Type

export const LinearAgentSessionPromptedWebhook = Schema.Struct({
	...agentSessionEventFields,
	action: Schema.Literal('prompted'),
	agentActivity: LinearAgentPromptActivityWebhook,
	promptContext: Schema.optionalKey(Schema.NullOr(Schema.String)),
})
export type LinearAgentSessionPromptedWebhook = typeof LinearAgentSessionPromptedWebhook.Type

export const LinearAgentSessionEventWebhook = Schema.Union([
	LinearAgentSessionCreatedWebhook,
	LinearAgentSessionPromptedWebhook,
])
export type LinearAgentSessionEventWebhook = typeof LinearAgentSessionEventWebhook.Type
