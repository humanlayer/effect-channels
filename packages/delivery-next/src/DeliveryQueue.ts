/**
 * This file defines the delivery queue service
 *
 * It is acceptable for processing parsed provider events and
 * saving or routing them to the appropriate "mailbox"
 */

import { Context, Effect, Schema } from 'effect'

/**
 * Mailbox was unavailable for network/storage reasons, webhook should receive 503
 */
export class DeliveryQueueUnavailable extends Schema.TaggedError<DeliveryQueueUnavailable>()(
	'DeliveryQueueUnavailable',
	{
		reason: Schema.String,
	},
) {}

/**
 * admission cannot be accepted e.g. capacity exhaustion
 */
export class DeliveryQueueRejected extends Schema.TaggedError<DeliveryQueueRejected>()('DeliveryQueueRejected', {
	reason: Schema.String,
}) {}

export type DeliveryQueueError = typeof DeliveryQueueUnavailable.Type | typeof DeliveryQueueRejected.Type

/**
 * DeliveryQueueAdmission - the thing that gets saved to a given mailbox by the delivery system
 *
 * @property namespace - the namespace of the app; exists in case you want multiple apps in the same system
 * @property installationId - e.g. slack Team ID or github organization Id from the webhook that identifies who it's for
 * @property resourceId - for slack the channel + thread TS, for github the issue/pr #, for linear the ticket #
 * @property eventId - the unique event ID for the webhook
 * @property payload - the parsed, provider-specific event encoded as JSON
 *
 */
export const DeliveryAdmission = Schema.TaggedStruct('DeliveryAdmission', {
	namespace: Schema.NonEmptyString,
	provider: Schema.NonEmptyString,
	installationId: Schema.NonEmptyString,
	resourceId: Schema.NonEmptyString,
	eventId: Schema.NonEmptyString,
	payload: Schema.Json,
})
export type DeliveryAdmission = Schema.Schema.Type<typeof DeliveryAdmission>

/**
 * Indicates that an event was delivered to a mailbox correctly
 * AND which mailbox it was accepted to
 *
 * @property mailboxKey - the id of the mailbox the event was put into
 * @property accepted - indicates if accepted. could be false due to already delivered, still return 200 in this case.
 */
export const DeliveryReceipt = Schema.TaggedStruct('DeliveryReceipt', {
	mailboxKey: Schema.NonEmptyString,
	accepted: Schema.Boolean,
})
export type DeliveryReceipt = typeof DeliveryReceipt.Type

export class DeliveryQueue extends Context.Service<
	DeliveryQueue,
	{
		readonly enqueue: (admission: DeliveryAdmission) => Effect.Effect<DeliveryReceipt, DeliveryQueueError>
	}
>()('DeliveryQueue') {}
