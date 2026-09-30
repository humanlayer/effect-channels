import { Schema } from 'effect'

import {
	LinearAgentActivityId,
	LinearAgentSessionId,
	LinearAttachmentId,
	LinearCommentId,
	LinearIssueId,
	LinearIssueLabelId,
	LinearOrganizationId,
	LinearReactionId,
	LinearTeamId,
	LinearUserId,
	LinearWebhookDeliveryId,
	LinearWorkflowStateId,
} from './LinearIdentity'

export const LinearInstallationRef = Schema.Struct({ organizationId: LinearOrganizationId })
export type LinearInstallationRef = typeof LinearInstallationRef.Type

export const LinearIssueRef = Schema.Struct({
	organizationId: LinearOrganizationId,
	teamId: Schema.NullOr(LinearTeamId),
	issueId: LinearIssueId,
})
export type LinearIssueRef = typeof LinearIssueRef.Type

export const LinearParticipant = Schema.Struct({
	id: LinearUserId,
	name: Schema.String,
	email: Schema.optionalKey(Schema.NullOr(Schema.String)),
	url: Schema.optionalKey(Schema.NullOr(Schema.String)),
	avatarUrl: Schema.optionalKey(Schema.NullOr(Schema.String)),
})
export type LinearParticipant = typeof LinearParticipant.Type

export const LinearTeamSnapshot = Schema.Struct({
	id: LinearTeamId,
	key: Schema.NonEmptyString,
	name: Schema.String,
})
export type LinearTeamSnapshot = typeof LinearTeamSnapshot.Type

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
export type LinearIssueSnapshot = typeof LinearIssueSnapshot.Type

export const LinearCommentRef = Schema.Struct({
	organizationId: LinearOrganizationId,
	teamId: Schema.NullOr(LinearTeamId),
	issueId: LinearIssueId,
	commentId: LinearCommentId,
})
export type LinearCommentRef = typeof LinearCommentRef.Type

export const LinearContent = Schema.Struct({ markdown: Schema.String })
export type LinearContent = typeof LinearContent.Type

export const LinearCommentSnapshot = Schema.Struct({
	ref: LinearCommentRef,
	issue: LinearIssueRef,
	parentCommentId: Schema.NullOr(LinearCommentId),
	content: LinearContent,
	author: Schema.NullOr(LinearParticipant),
})
export type LinearCommentSnapshot = typeof LinearCommentSnapshot.Type

export const LinearReactionSnapshot = Schema.Struct({
	id: LinearReactionId,
	issueId: LinearIssueId,
	commentId: Schema.NullOr(LinearCommentId),
	emoji: Schema.String,
	author: Schema.NullOr(LinearParticipant),
})
export type LinearReactionSnapshot = typeof LinearReactionSnapshot.Type

export const LinearIssueAttachmentSnapshot = Schema.Struct({
	id: LinearAttachmentId,
	issueId: LinearIssueId,
	title: Schema.String,
	subtitle: Schema.NullOr(Schema.String),
	url: Schema.String,
	metadata: Schema.optionalKey(Schema.Record(Schema.String, Schema.Json)),
})
export type LinearIssueAttachmentSnapshot = typeof LinearIssueAttachmentSnapshot.Type

export const LinearWorkflowState = Schema.Struct({
	id: LinearWorkflowStateId,
	name: Schema.String,
	type: Schema.String,
})
export type LinearWorkflowState = typeof LinearWorkflowState.Type

export const LinearIssueLabel = Schema.Struct({ id: LinearIssueLabelId, name: Schema.String, color: Schema.String })
export type LinearIssueLabel = typeof LinearIssueLabel.Type

export const LinearIssueInfo = Schema.Struct({
	ref: LinearIssueRef,
	identifier: Schema.NonEmptyString,
	title: Schema.String,
	description: Schema.NullOr(Schema.String),
	priority: Schema.Int,
	url: Schema.String,
	state: LinearWorkflowState,
	labels: Schema.Array(LinearIssueLabel),
	assignee: Schema.NullOr(LinearParticipant),
	delegate: Schema.NullOr(LinearParticipant),
})
export type LinearIssueInfo = typeof LinearIssueInfo.Type

export const LinearUser = Schema.Struct({
	id: LinearUserId,
	name: Schema.String,
	email: Schema.NullOr(Schema.String),
	active: Schema.Boolean,
	app: Schema.Boolean,
	isAssignable: Schema.Boolean,
	canAccessAnyPublicTeam: Schema.Boolean,
	teamIds: Schema.Array(LinearTeamId),
})
export type LinearUser = typeof LinearUser.Type

export const LinearAppUser = Schema.Struct({ ...LinearUser.fields, app: Schema.Literal(true) })
export type LinearAppUser = typeof LinearAppUser.Type

export const LinearPageInfo = Schema.Struct({ endCursor: Schema.NullOr(Schema.String), hasNextPage: Schema.Boolean })
export type LinearPageInfo = typeof LinearPageInfo.Type

export const LinearUserPage = Schema.Struct({ users: Schema.Array(LinearUser), pageInfo: LinearPageInfo })
export type LinearUserPage = typeof LinearUserPage.Type

export const LinearAppUserPage = Schema.Struct({ users: Schema.Array(LinearAppUser), pageInfo: LinearPageInfo })
export type LinearAppUserPage = typeof LinearAppUserPage.Type

export const LinearAgentSessionRef = Schema.Struct({
	organizationId: LinearOrganizationId,
	appUserId: LinearUserId,
	sessionId: LinearAgentSessionId,
	issueId: LinearIssueId,
})
export type LinearAgentSessionRef = typeof LinearAgentSessionRef.Type

export const LinearAgentSessionSnapshot = Schema.Struct({
	ref: LinearAgentSessionRef,
	triggerEventId: Schema.NonEmptyString,
	createdAt: Schema.String,
	endedAt: Schema.NullOr(Schema.String),
	commentId: Schema.NullOr(LinearCommentId),
	sourceCommentId: Schema.NullOr(LinearCommentId),
	creator: Schema.NullOr(LinearParticipant),
})
export type LinearAgentSessionSnapshot = typeof LinearAgentSessionSnapshot.Type

export const LinearAgentPrompt = Schema.Struct({
	id: LinearAgentActivityId,
	body: Schema.String,
	createdAt: Schema.String,
	user: LinearParticipant,
	/** Linear's signal on the prompt, such as `stop` when the user asks the agent to stop. */
	signal: Schema.NullOr(Schema.String),
})
export type LinearAgentPrompt = typeof LinearAgentPrompt.Type

export const LinearAgentSessionComment = Schema.Struct({
	id: LinearCommentId,
	body: Schema.String,
	issueId: Schema.NullOr(LinearIssueId),
	userId: Schema.NullOr(LinearUserId),
})
export type LinearAgentSessionComment = typeof LinearAgentSessionComment.Type

export const LinearAgentGuidance = Schema.Struct({
	body: Schema.String,
	origin: Schema.String,
})
export type LinearAgentGuidance = typeof LinearAgentGuidance.Type

export const LinearActivityContent = Schema.TaggedUnion({
	Thought: { body: Schema.String },
	Response: { body: Schema.String },
})
export type LinearActivityContent = typeof LinearActivityContent.Type

export const LinearAgentActivityReceipt = Schema.Struct({
	activityId: LinearAgentActivityId,
	sessionId: LinearAgentSessionId,
})
export type LinearAgentActivityReceipt = typeof LinearAgentActivityReceipt.Type

export const LinearCreateAgentActivityRequest = Schema.Struct({
	organizationId: LinearOrganizationId,
	sessionId: LinearAgentSessionId,
	content: LinearActivityContent,
	ephemeral: Schema.Boolean,
	deliveryId: LinearWebhookDeliveryId,
})
export type LinearCreateAgentActivityRequest = typeof LinearCreateAgentActivityRequest.Type
