import { Schema } from 'effect'

import { LinearIssueId, LinearOrganizationId, LinearTeamId, LinearUserId } from './LinearIdentity'

export const LinearWebhookActor = Schema.Struct({
	id: LinearUserId,
	name: Schema.String,
	email: Schema.optionalKey(Schema.NullOr(Schema.String)),
	type: Schema.optionalKey(Schema.String),
})

export const LinearWebhookTeam = Schema.Struct({
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
