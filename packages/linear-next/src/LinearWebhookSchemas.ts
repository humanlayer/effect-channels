import { Schema } from 'effect'

import { LinearWebhookDeliveryId } from './LinearIdentity'
import { LinearIssueCreateWebhook } from './LinearWebhookEventSchemas'

export const LinearWebhookHeaders = Schema.Struct({
	'linear-delivery': LinearWebhookDeliveryId,
	'linear-event': Schema.NonEmptyString,
	'linear-signature': Schema.NonEmptyString,
	'linear-timestamp': Schema.NonEmptyString,
})

export const LinearWebhookEnvelope = Schema.Struct({
	type: Schema.NonEmptyString,
	action: Schema.NonEmptyString,
	organizationId: Schema.NonEmptyString,
})

export const LinearSupportedWebhook = LinearIssueCreateWebhook
export type LinearSupportedWebhook = typeof LinearSupportedWebhook.Type
