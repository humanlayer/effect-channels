import { Schema } from 'effect'

import { LinearWebhookDeliveryId } from './LinearIdentity'
import {
	LinearAgentSessionEventWebhook,
	LinearAppUserNotificationWebhook,
	LinearLifecycleWebhookEvent,
	LinearResourceWebhookEvent,
} from './LinearWebhookEventSchemas'

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

export const LinearSupportedWebhook = Schema.Union([
	LinearResourceWebhookEvent,
	LinearLifecycleWebhookEvent,
	LinearAppUserNotificationWebhook,
	LinearAgentSessionEventWebhook,
])
export type LinearSupportedWebhook = typeof LinearSupportedWebhook.Type

/** Durable Agent Session payload with authenticated ingress metadata kept separate from provider JSON. */
export const LinearStoredAgentSessionWebhook = Schema.TaggedStruct('LinearStoredAgentSessionWebhook', {
	deliveryId: LinearWebhookDeliveryId,
	webhook: LinearAgentSessionEventWebhook,
})
export type LinearStoredAgentSessionWebhook = typeof LinearStoredAgentSessionWebhook.Type

export const LinearStoredWebhook = Schema.Union([
	LinearResourceWebhookEvent,
	LinearLifecycleWebhookEvent,
	LinearAppUserNotificationWebhook,
	LinearStoredAgentSessionWebhook,
])
export type LinearStoredWebhook = typeof LinearStoredWebhook.Type
