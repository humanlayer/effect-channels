import { Schema } from 'effect'

import {
	LinearAgentActivityId,
	LinearAgentSessionId,
	LinearAttachmentId,
	LinearCommentId,
	LinearIssueId,
	LinearNotificationId,
	LinearOrganizationId,
	LinearReactionId,
	LinearTeamId,
	LinearUserId,
} from './LinearIdentity'

const optionalNullableString = Schema.optionalKey(Schema.NullOr(Schema.String))
const optionalNullableJson = Schema.optionalKey(Schema.NullOr(Schema.Json))

export const LinearWebhookActor = Schema.Struct({
	__typename: Schema.optionalKey(Schema.Literal('UserChildWebhookPayload')),
	id: LinearUserId,
	name: Schema.String,
	type: Schema.optionalKey(Schema.String),
	email: Schema.optionalKey(Schema.NullOr(Schema.String)),
	url: Schema.optionalKey(Schema.NullOr(Schema.String)),
	avatarUrl: Schema.optionalKey(Schema.NullOr(Schema.String)),
})

const LinearUserEntityWebhookActor = Schema.Struct({
	...LinearWebhookActor.fields,
	__typename: Schema.optionalKey(Schema.Literal('UserActorWebhookPayload')),
	type: Schema.String,
})

const LinearExternalUserWebhookActor = Schema.Struct({
	__typename: Schema.optionalKey(Schema.Literal('ExternalUserActorWebhookPayload')),
	id: LinearUserId,
	name: Schema.String,
	type: Schema.String,
	email: Schema.String,
})

const LinearOauthClientWebhookActor = Schema.Struct({
	__typename: Schema.optionalKey(Schema.Literal('OauthClientActorWebhookPayload')),
	id: LinearUserId,
	name: Schema.String,
	type: Schema.String,
})

const LinearIntegrationWebhookActor = Schema.Struct({
	__typename: Schema.optionalKey(Schema.Literal('IntegrationActorWebhookPayload')),
	id: LinearUserId,
	service: Schema.String,
	type: Schema.String,
})

export const LinearEntityWebhookActor = Schema.Union([
	LinearUserEntityWebhookActor,
	LinearExternalUserWebhookActor,
	LinearOauthClientWebhookActor,
	LinearIntegrationWebhookActor,
])

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
	teamId: LinearTeamId,
	team: Schema.NullOr(LinearWebhookTeam),
	creator: Schema.optionalKey(Schema.NullOr(LinearWebhookActor)),
	state: Schema.optionalKey(Schema.Json),
	stateId: optionalNullableString,
	labels: Schema.optionalKey(Schema.Json),
	labelIds: Schema.optionalKey(Schema.Array(Schema.String)),
	assignee: Schema.optionalKey(Schema.Json),
	assigneeId: optionalNullableString,
	delegate: Schema.optionalKey(Schema.Json),
	delegateId: optionalNullableString,
	project: Schema.optionalKey(Schema.Json),
	projectId: optionalNullableString,
	projectMilestone: Schema.optionalKey(Schema.Json),
	projectMilestoneId: optionalNullableString,
	cycle: Schema.optionalKey(Schema.Json),
	cycleId: optionalNullableString,
	parentId: optionalNullableString,
	estimate: Schema.optionalKey(Schema.NullOr(Schema.Number)),
	dueDate: optionalNullableString,
	subscriberIds: Schema.optionalKey(Schema.Array(Schema.String)),
	archivedAt: optionalNullableString,
	triagedAt: optionalNullableString,
	startedTriageAt: optionalNullableString,
	snoozedUntilAt: optionalNullableString,
	startedAt: optionalNullableString,
	completedAt: optionalNullableString,
	canceledAt: optionalNullableString,
	releases: Schema.optionalKey(Schema.Json),
	slaStartedAt: optionalNullableString,
	slaBreachesAt: optionalNullableString,
	slaType: optionalNullableString,
	attachments: Schema.optionalKey(Schema.Json),
})

export const LinearWebhookIssueChild = Schema.Struct({
	id: LinearIssueId,
	identifier: Schema.NonEmptyString,
	team: LinearWebhookTeam,
	teamId: LinearTeamId,
	title: Schema.String,
	url: Schema.String,
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
	/** Linear's webhook SDL includes `webhookId` and `webhookTimestamp`, but its agent guide and observed Inbox Notification deliveries omit them. The `Linear-Delivery` and `Linear-Timestamp` headers remain the authoritative ingress values either way. */
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
	actor: Schema.optionalKey(Schema.NullOr(LinearEntityWebhookActor)),
	url: Schema.optionalKey(Schema.NullOr(Schema.String)),
	webhookId: Schema.NonEmptyString,
	webhookTimestamp: Schema.Number,
	createdAt: Schema.String,
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

const issueField = LinearWebhookIssue.fields
const optionalNullableStrings = Schema.optionalKey(Schema.NullOr(Schema.Array(Schema.String)))

/** The previous value of each issue field an update changed. Linear sends only changed keys and uses `null` when the previous value was unset. */
export const LinearIssueUpdatedFrom = Schema.Struct({
	title: optionalNullableString,
	description: issueField.description,
	descriptionData: optionalNullableJson,
	state: issueField.state,
	stateId: issueField.stateId,
	startedAt: issueField.startedAt,
	completedAt: issueField.completedAt,
	canceledAt: issueField.canceledAt,
	priority: Schema.optionalKey(Schema.NullOr(Schema.Int)),
	priorityLabel: optionalNullableString,
	labels: issueField.labels,
	labelIds: optionalNullableStrings,
	assignee: issueField.assignee,
	assigneeId: issueField.assigneeId,
	delegate: issueField.delegate,
	delegateId: issueField.delegateId,
	project: issueField.project,
	projectId: issueField.projectId,
	projectMilestone: issueField.projectMilestone,
	projectMilestoneId: issueField.projectMilestoneId,
	cycle: issueField.cycle,
	cycleId: issueField.cycleId,
	team: Schema.optionalKey(Schema.NullOr(LinearWebhookTeam)),
	teamId: Schema.optionalKey(Schema.NullOr(LinearTeamId)),
	previousIdentifiers: optionalNullableStrings,
	parentId: issueField.parentId,
	subIssueSortOrder: Schema.optionalKey(Schema.NullOr(Schema.Number)),
	estimate: issueField.estimate,
	dueDate: issueField.dueDate,
	subscriberIds: optionalNullableStrings,
	archivedAt: issueField.archivedAt,
	trashed: Schema.optionalKey(Schema.NullOr(Schema.Boolean)),
	triagedAt: issueField.triagedAt,
	startedTriageAt: issueField.startedTriageAt,
	snoozedUntilAt: issueField.snoozedUntilAt,
	releases: issueField.releases,
	slaStartedAt: issueField.slaStartedAt,
	slaBreachesAt: issueField.slaBreachesAt,
	slaType: issueField.slaType,
})
export type LinearIssueUpdatedFrom = typeof LinearIssueUpdatedFrom.Type

const relatedWebhookFields = {
	organizationId: LinearOrganizationId,
	actor: Schema.optionalKey(Schema.NullOr(LinearEntityWebhookActor)),
	createdAt: Schema.String,
	url: Schema.optionalKey(Schema.NullOr(Schema.String)),
	webhookId: Schema.NonEmptyString,
	webhookTimestamp: Schema.Number,
}

export const LinearIssueUpdateWebhook = Schema.Struct({
	...relatedWebhookFields,
	type: Schema.Literal('Issue'),
	action: Schema.Literal('update'),
	data: LinearWebhookIssue,
	updatedFrom: LinearIssueUpdatedFrom,
})
export const LinearIssueRemoveWebhook = Schema.Struct({
	...relatedWebhookFields,
	type: Schema.Literal('Issue'),
	action: Schema.Literal('remove'),
	data: LinearWebhookIssue,
})

export const LinearWebhookComment = Schema.Struct({
	id: LinearCommentId,
	body: Schema.String,
	issueId: Schema.optionalKey(Schema.NullOr(LinearIssueId)),
	issue: Schema.optionalKey(Schema.NullOr(LinearWebhookIssueChild)),
	parentId: Schema.optionalKey(Schema.NullOr(LinearCommentId)),
	user: Schema.optionalKey(Schema.NullOr(LinearWebhookActor)),
	userId: Schema.optionalKey(Schema.NullOr(LinearUserId)),
	createdAt: Schema.String,
	updatedAt: Schema.String,
	reactionData: Schema.Json,
})
export const LinearWebhookCommentChild = Schema.Struct({
	id: LinearCommentId,
	body: Schema.String,
	issueId: Schema.optionalKey(Schema.NullOr(LinearIssueId)),
	userId: Schema.optionalKey(Schema.NullOr(LinearUserId)),
})
const CommentUpdatedFrom = Schema.Struct({ body: Schema.optionalKey(Schema.NullOr(Schema.String)) })
const commentWebhook = <Action extends 'create' | 'remove'>(action: Action) =>
	Schema.Struct({
		...relatedWebhookFields,
		type: Schema.Literal('Comment'),
		action: Schema.Literal(action),
		data: LinearWebhookComment,
	})
export const LinearCommentCreateWebhook = commentWebhook('create')
export const LinearCommentUpdateWebhook = Schema.Struct({
	...relatedWebhookFields,
	type: Schema.Literal('Comment'),
	action: Schema.Literal('update'),
	data: LinearWebhookComment,
	updatedFrom: CommentUpdatedFrom,
})
export const LinearCommentRemoveWebhook = commentWebhook('remove')

export const LinearWebhookReaction = Schema.Struct({
	id: LinearReactionId,
	emoji: Schema.String,
	user: Schema.optionalKey(Schema.NullOr(LinearWebhookActor)),
	userId: Schema.optionalKey(Schema.NullOr(LinearUserId)),
	issue: Schema.optionalKey(Schema.NullOr(LinearWebhookIssueChild)),
	issueId: Schema.optionalKey(Schema.NullOr(LinearIssueId)),
	comment: Schema.optionalKey(Schema.NullOr(LinearWebhookCommentChild)),
	commentId: Schema.optionalKey(Schema.NullOr(LinearCommentId)),
	createdAt: Schema.String,
	updatedAt: Schema.String,
})
const reactionWebhook = <Action extends 'create' | 'remove'>(action: Action) =>
	Schema.Struct({
		...relatedWebhookFields,
		type: Schema.Literal('Reaction'),
		action: Schema.Literal(action),
		data: LinearWebhookReaction,
	})
export const LinearReactionCreateWebhook = reactionWebhook('create')
export const LinearReactionRemoveWebhook = reactionWebhook('remove')

export const LinearWebhookAttachment = Schema.Struct({
	id: LinearAttachmentId,
	issueId: LinearIssueId,
	title: Schema.String,
	subtitle: optionalNullableString,
	url: Schema.String,
	metadata: Schema.Json,
	groupBySource: Schema.Boolean,
	createdAt: Schema.String,
	updatedAt: Schema.String,
	archivedAt: optionalNullableString,
	creatorId: optionalNullableString,
	externalUserCreatorId: optionalNullableString,
	originalIssueId: optionalNullableString,
	source: optionalNullableJson,
	sourceType: optionalNullableString,
})
const AttachmentUpdatedFrom = Schema.Struct({
	title: optionalNullableString,
	subtitle: optionalNullableString,
	url: optionalNullableString,
	metadata: Schema.optionalKey(Schema.Json),
})
const attachmentWebhook = <Action extends 'create' | 'remove'>(action: Action) =>
	Schema.Struct({
		...relatedWebhookFields,
		type: Schema.Literal('Attachment'),
		action: Schema.Literal(action),
		data: LinearWebhookAttachment,
	})
export const LinearAttachmentCreateWebhook = attachmentWebhook('create')
export const LinearAttachmentUpdateWebhook = Schema.Struct({
	...relatedWebhookFields,
	type: Schema.Literal('Attachment'),
	action: Schema.Literal('update'),
	data: LinearWebhookAttachment,
	updatedFrom: AttachmentUpdatedFrom,
})
export const LinearAttachmentRemoveWebhook = attachmentWebhook('remove')

export const LinearResourceWebhookEvent = Schema.Union([
	LinearIssueCreateWebhook,
	LinearIssueUpdateWebhook,
	LinearIssueRemoveWebhook,
	LinearCommentCreateWebhook,
	LinearCommentUpdateWebhook,
	LinearCommentRemoveWebhook,
	LinearReactionCreateWebhook,
	LinearReactionRemoveWebhook,
	LinearAttachmentCreateWebhook,
	LinearAttachmentUpdateWebhook,
	LinearAttachmentRemoveWebhook,
])
export type LinearResourceWebhookEvent = typeof LinearResourceWebhookEvent.Type

export const LinearTeamAccessChangedWebhook = Schema.Struct({
	type: Schema.Literal('PermissionChange'),
	action: Schema.Literal('teamAccessChanged'),
	organizationId: LinearOrganizationId,
	appUserId: LinearUserId,
	oauthClientId: Schema.NonEmptyString,
	canAccessAllPublicTeams: Schema.Boolean,
	createdAt: Schema.String,
	addedTeamIds: Schema.Array(LinearTeamId),
	removedTeamIds: Schema.Array(LinearTeamId),
	webhookId: Schema.NonEmptyString,
	webhookTimestamp: Schema.Number,
})
export const LinearInstallationRevokedWebhook = Schema.Struct({
	type: Schema.Literal('OAuthApp'),
	action: Schema.Literal('revoked'),
	organizationId: LinearOrganizationId,
	oauthClientId: Schema.NonEmptyString,
	createdAt: Schema.String,
	webhookId: Schema.NonEmptyString,
	webhookTimestamp: Schema.Number,
})
export const LinearLifecycleWebhookEvent = Schema.Union([
	LinearTeamAccessChangedWebhook,
	LinearInstallationRevokedWebhook,
])
export type LinearLifecycleWebhookEvent = typeof LinearLifecycleWebhookEvent.Type
