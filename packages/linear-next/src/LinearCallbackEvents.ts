import { Schema } from 'effect'

import { LinearWebhookDeliveryId } from './LinearIdentity'
import { LinearParticipant } from './LinearModels'
import { LinearIssue } from './LinearResources'

export const LinearIssueOpened = Schema.TaggedStruct('LinearIssueOpened', {
	eventId: LinearWebhookDeliveryId,
	issue: LinearIssue,
	actor: Schema.NullOr(LinearParticipant),
})
export type LinearIssueOpened = typeof LinearIssueOpened.Type

export const LinearIssueCreated = Schema.TaggedStruct('LinearIssueCreated', {
	issue: LinearIssue,
	trigger: LinearIssueOpened,
	events: Schema.Array(Schema.Never),
})
export type LinearIssueCreated = typeof LinearIssueCreated.Type
