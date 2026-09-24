import { Schema } from 'effect'

export const LinearOrganizationId = Schema.NonEmptyString.pipe(Schema.brand('LinearOrganizationId'))
export type LinearOrganizationId = typeof LinearOrganizationId.Type

export const LinearUserId = Schema.NonEmptyString.pipe(Schema.brand('LinearUserId'))
export type LinearUserId = typeof LinearUserId.Type

export const LinearTeamId = Schema.NonEmptyString.pipe(Schema.brand('LinearTeamId'))
export type LinearTeamId = typeof LinearTeamId.Type

export const LinearIssueId = Schema.NonEmptyString.pipe(Schema.brand('LinearIssueId'))
export type LinearIssueId = typeof LinearIssueId.Type

export const LinearCommentId = Schema.NonEmptyString.pipe(Schema.brand('LinearCommentId'))
export type LinearCommentId = typeof LinearCommentId.Type

export const LinearNotificationId = Schema.NonEmptyString.pipe(Schema.brand('LinearNotificationId'))
export type LinearNotificationId = typeof LinearNotificationId.Type

export const LinearAgentSessionId = Schema.NonEmptyString.pipe(Schema.brand('LinearAgentSessionId'))
export type LinearAgentSessionId = typeof LinearAgentSessionId.Type

export const LinearAgentActivityId = Schema.NonEmptyString.pipe(Schema.brand('LinearAgentActivityId'))
export type LinearAgentActivityId = typeof LinearAgentActivityId.Type

export const LinearWebhookDeliveryId = Schema.NonEmptyString.pipe(Schema.brand('LinearWebhookDeliveryId'))
export type LinearWebhookDeliveryId = typeof LinearWebhookDeliveryId.Type

export const linearIssueResourceId = (issueId: LinearIssueId) => `linear:v1:issue:${encodeURIComponent(issueId)}`

export const linearAgentSessionResourceId = (sessionId: LinearAgentSessionId) =>
	`linear:v1:agent-session:${encodeURIComponent(sessionId)}`
