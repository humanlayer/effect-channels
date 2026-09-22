import { Schema } from 'effect'

export const LinearOrganizationId = Schema.NonEmptyString.pipe(Schema.brand('LinearOrganizationId'))
export type LinearOrganizationId = typeof LinearOrganizationId.Type

export const LinearUserId = Schema.NonEmptyString.pipe(Schema.brand('LinearUserId'))
export type LinearUserId = typeof LinearUserId.Type

export const LinearTeamId = Schema.NonEmptyString.pipe(Schema.brand('LinearTeamId'))
export type LinearTeamId = typeof LinearTeamId.Type

export const LinearIssueId = Schema.NonEmptyString.pipe(Schema.brand('LinearIssueId'))
export type LinearIssueId = typeof LinearIssueId.Type

export const LinearWebhookDeliveryId = Schema.NonEmptyString.pipe(Schema.brand('LinearWebhookDeliveryId'))
export type LinearWebhookDeliveryId = typeof LinearWebhookDeliveryId.Type

export const linearIssueResourceId = (issueId: LinearIssueId) => `linear:v1:issue:${encodeURIComponent(issueId)}`
