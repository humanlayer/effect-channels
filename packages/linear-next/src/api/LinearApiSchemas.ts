import { Schema } from 'effect'

import {
	LinearAttachmentId,
	LinearCommentId,
	LinearIssueLabelId,
	LinearReactionId,
	LinearTeamId,
	LinearUserId,
	LinearWorkflowStateId,
} from '../LinearIdentity'

export const LinearApiParticipant = Schema.Struct({
	id: LinearUserId,
	name: Schema.String,
	email: Schema.optionalKey(Schema.NullOr(Schema.String)),
	active: Schema.optionalKey(Schema.Boolean),
	app: Schema.optionalKey(Schema.Boolean),
	isAssignable: Schema.optionalKey(Schema.Boolean),
	canAccessAnyPublicTeam: Schema.optionalKey(Schema.Boolean),
	teams: Schema.optionalKey(Schema.Struct({ nodes: Schema.Array(Schema.Struct({ id: LinearTeamId })) })),
})

export const LinearApiState = Schema.Struct({
	id: LinearWorkflowStateId,
	name: Schema.String,
	type: Schema.String,
})

export const LinearApiLabel = Schema.Struct({
	id: LinearIssueLabelId,
	name: Schema.String,
	color: Schema.String,
})

export const LinearApiIssue = Schema.Struct({
	id: Schema.NonEmptyString,
	identifier: Schema.NonEmptyString,
	title: Schema.String,
	description: Schema.NullOr(Schema.String),
	priority: Schema.Int,
	url: Schema.String,
	team: Schema.Struct({ id: LinearTeamId }),
	state: LinearApiState,
	labels: Schema.Struct({ nodes: Schema.Array(LinearApiLabel) }),
	assignee: Schema.NullOr(LinearApiParticipant),
	delegate: Schema.NullOr(LinearApiParticipant),
})

export const LinearApiComment = Schema.Struct({
	id: LinearCommentId,
	body: Schema.String,
	parent: Schema.optionalKey(Schema.NullOr(Schema.Struct({ id: LinearCommentId }))),
	user: Schema.NullOr(LinearApiParticipant),
})

export const LinearApiAttachment = Schema.Struct({
	id: LinearAttachmentId,
	title: Schema.String,
	subtitle: Schema.NullOr(Schema.String),
	url: Schema.String,
	metadata: Schema.optionalKey(Schema.Record(Schema.String, Schema.Json)),
})

export const LinearApiReaction = Schema.Struct({
	id: LinearReactionId,
	emoji: Schema.String,
	user: Schema.NullOr(LinearApiParticipant),
})

export const LinearApiPageInfo = Schema.Struct({
	endCursor: Schema.NullOr(Schema.String),
	hasNextPage: Schema.Boolean,
})

export const linearApiIssueFields = `id identifier title description priority url team { id } state { id name type } labels { nodes { id name color } } assignee { id name email } delegate { id name email }`
