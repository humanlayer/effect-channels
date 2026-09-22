import { Schema } from 'effect'

import { LinearIssueId, LinearOrganizationId, LinearTeamId, LinearUserId } from './LinearIdentity'

export const LinearInstallationRef = Schema.Struct({ organizationId: LinearOrganizationId })
export interface LinearInstallationRef extends Schema.Schema.Type<typeof LinearInstallationRef> {}

export const LinearIssueRef = Schema.Struct({
	organizationId: LinearOrganizationId,
	teamId: LinearTeamId,
	issueId: LinearIssueId,
})
export interface LinearIssueRef extends Schema.Schema.Type<typeof LinearIssueRef> {}

export const LinearParticipant = Schema.Struct({
	id: LinearUserId,
	name: Schema.String,
	email: Schema.optionalKey(Schema.NullOr(Schema.String)),
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
	identifier: Schema.NonEmptyString,
	number: Schema.Int,
	title: Schema.String,
	description: Schema.NullOr(Schema.String),
	priority: Schema.Int,
	url: Schema.String,
	team: LinearTeamSnapshot,
	creator: Schema.NullOr(LinearParticipant),
})
export interface LinearIssueSnapshot extends Schema.Schema.Type<typeof LinearIssueSnapshot> {}
