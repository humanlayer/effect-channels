import { Schema } from 'effect'

import {
	LinearAgentActivityId,
	LinearAgentSessionId,
	LinearAttachmentId,
	LinearCommentId,
	LinearIssueId,
	LinearOrganizationId,
	LinearReactionId,
	LinearTeamId,
	LinearUserId,
	LinearWebhookDeliveryId,
} from './LinearIdentity'

export const LinearInstallationRef = Schema.Struct({ organizationId: LinearOrganizationId })
export interface LinearInstallationRef extends Schema.Schema.Type<typeof LinearInstallationRef> {}

export const LinearIssueRef = Schema.Struct({
	organizationId: LinearOrganizationId,
	teamId: Schema.NullOr(LinearTeamId),
	issueId: LinearIssueId,
})
export interface LinearIssueRef extends Schema.Schema.Type<typeof LinearIssueRef> {}

export const LinearParticipant = Schema.Struct({
	id: LinearUserId,
	name: Schema.String,
	email: Schema.optionalKey(Schema.NullOr(Schema.String)),
	url: Schema.optionalKey(Schema.NullOr(Schema.String)),
	avatarUrl: Schema.optionalKey(Schema.NullOr(Schema.String)),
})
export interface LinearParticipant extends Schema.Schema.Type<typeof LinearParticipant> {}

export const LinearTeamSnapshot = Schema.Struct({
	id: LinearTeamId,
	key: Schema.NonEmptyString,
	name: Schema.String,
})
export interface LinearTeamSnapshot extends Schema.Schema.Type<typeof LinearTeamSnapshot> {}

export const LinearIssueSnapshot = Schema.Struct({
	ref: LinearIssueRef,
	identifier: Schema.NullOr(Schema.NonEmptyString),
	number: Schema.NullOr(Schema.Int),
	title: Schema.NullOr(Schema.String),
	description: Schema.NullOr(Schema.String),
	priority: Schema.NullOr(Schema.Int),
	url: Schema.NullOr(Schema.String),
	team: Schema.NullOr(LinearTeamSnapshot),
	creator: Schema.NullOr(LinearParticipant),
})
export interface LinearIssueSnapshot extends Schema.Schema.Type<typeof LinearIssueSnapshot> {}

export const LinearCommentRef = Schema.Struct({
	organizationId: LinearOrganizationId,
	teamId: Schema.NullOr(LinearTeamId),
	issueId: LinearIssueId,
	commentId: LinearCommentId,
})
export interface LinearCommentRef extends Schema.Schema.Type<typeof LinearCommentRef> {}

export const LinearContent = Schema.Struct({ markdown: Schema.String })
export interface LinearContent extends Schema.Schema.Type<typeof LinearContent> {}

export const LinearCommentSnapshot = Schema.Struct({
	ref: LinearCommentRef,
	issue: LinearIssueRef,
	parentCommentId: Schema.NullOr(LinearCommentId),
	content: LinearContent,
	author: Schema.NullOr(LinearParticipant),
})
export interface LinearCommentSnapshot extends Schema.Schema.Type<typeof LinearCommentSnapshot> {}

export const LinearReactionSnapshot = Schema.Struct({
	id: LinearReactionId,
	issueId: LinearIssueId,
	commentId: Schema.NullOr(LinearCommentId),
	emoji: Schema.String,
	author: Schema.NullOr(LinearParticipant),
})
export interface LinearReactionSnapshot extends Schema.Schema.Type<typeof LinearReactionSnapshot> {}

export const LinearIssueAttachmentSnapshot = Schema.Struct({
	id: LinearAttachmentId,
	issueId: LinearIssueId,
	title: Schema.String,
	subtitle: Schema.NullOr(Schema.String),
	url: Schema.String,
	metadata: Schema.optionalKey(Schema.Json),
})
export interface LinearIssueAttachmentSnapshot extends Schema.Schema.Type<typeof LinearIssueAttachmentSnapshot> {}

export const LinearAgentSessionRef = Schema.Struct({
	organizationId: LinearOrganizationId,
	appUserId: LinearUserId,
	sessionId: LinearAgentSessionId,
	issueId: LinearIssueId,
})
export interface LinearAgentSessionRef extends Schema.Schema.Type<typeof LinearAgentSessionRef> {}

export const LinearAgentSessionSnapshot = Schema.Struct({
	ref: LinearAgentSessionRef,
	triggerEventId: Schema.NonEmptyString,
	createdAt: Schema.String,
	endedAt: Schema.NullOr(Schema.String),
	commentId: Schema.NullOr(LinearCommentId),
	sourceCommentId: Schema.NullOr(LinearCommentId),
	creator: Schema.NullOr(LinearParticipant),
})
export interface LinearAgentSessionSnapshot extends Schema.Schema.Type<typeof LinearAgentSessionSnapshot> {}

export const LinearAgentPrompt = Schema.Struct({
	id: LinearAgentActivityId,
	body: Schema.String,
	createdAt: Schema.String,
	user: LinearParticipant,
})
export interface LinearAgentPrompt extends Schema.Schema.Type<typeof LinearAgentPrompt> {}

export const LinearAgentSessionComment = Schema.Struct({
	id: LinearCommentId,
	body: Schema.String,
	issueId: Schema.NullOr(LinearIssueId),
	userId: Schema.NullOr(LinearUserId),
})
export interface LinearAgentSessionComment extends Schema.Schema.Type<typeof LinearAgentSessionComment> {}

export const LinearAgentGuidance = Schema.Struct({
	body: Schema.String,
	origin: Schema.String,
})
export interface LinearAgentGuidance extends Schema.Schema.Type<typeof LinearAgentGuidance> {}

export const LinearActivityContent = Schema.TaggedUnion({
	Thought: { body: Schema.String },
	Response: { body: Schema.String },
})
export type LinearActivityContent = typeof LinearActivityContent.Type

export const LinearAgentActivityReceipt = Schema.Struct({
	activityId: LinearAgentActivityId,
	sessionId: LinearAgentSessionId,
})
export interface LinearAgentActivityReceipt extends Schema.Schema.Type<typeof LinearAgentActivityReceipt> {}

export const LinearCreateAgentActivityRequest = Schema.Struct({
	organizationId: LinearOrganizationId,
	sessionId: LinearAgentSessionId,
	content: LinearActivityContent,
	ephemeral: Schema.Boolean,
	deliveryId: LinearWebhookDeliveryId,
})
export interface LinearCreateAgentActivityRequest extends Schema.Schema.Type<typeof LinearCreateAgentActivityRequest> {}
