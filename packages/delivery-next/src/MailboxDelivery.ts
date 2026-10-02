/**
 * This file defines the mailbox delivery service.
 *
 * It is responsible for processing parsed provider events and
 * saving or routing them to the appropriate "mailbox"
 */

import { Context, Effect, Schema } from 'effect'

/**
 * Mailbox was unavailable for network/storage reasons, webhook should receive 503
 */
export class MailboxDeliveryUnavailable extends Schema.TaggedError<MailboxDeliveryUnavailable>()(
	'MailboxDeliveryUnavailable',
	{
		reason: Schema.String,
	},
) {}

/**
 * admission cannot be accepted e.g. capacity exhaustion
 */
export class MailboxDeliveryRejected extends Schema.TaggedError<MailboxDeliveryRejected>()('MailboxDeliveryRejected', {
	reason: Schema.String,
}) {}

export type MailboxDeliveryError = MailboxDeliveryUnavailable | MailboxDeliveryRejected

/**
 * The provider event delivered to a mailbox.
 *
 * @property namespace - the namespace of the app; exists in case you want multiple apps in the same system
 * @property installationId - e.g. slack Team ID or github organization Id from the webhook that identifies who it's for
 * @property resourceId - for slack the channel + thread TS, for github the issue/pr #, for linear the ticket #
 * @property eventId - the unique event ID for the webhook
 * @property payload - the parsed, provider-specific event encoded as JSON
 * @property interrupt - the event asks the mailbox's current delivery to stop, such as a Linear stop
 * prompt. The store marks the active delivery in the same write; the event itself still waits its turn.
 *
 */
export const DeliveryAdmission = Schema.TaggedStruct('DeliveryAdmission', {
	namespace: Schema.NonEmptyString,
	provider: Schema.NonEmptyString,
	installationId: Schema.NonEmptyString,
	resourceId: Schema.NonEmptyString,
	eventId: Schema.NonEmptyString,
	payload: Schema.Json,
	interrupt: Schema.optionalKey(Schema.Literal(true)),
})
export type DeliveryAdmission = Schema.Schema.Type<typeof DeliveryAdmission>

/** An admission as JSON text, for stores that keep it as a string. */
export const DeliveryAdmissionJson = Schema.fromJsonString(DeliveryAdmission)

/** A collision-free, stable key for the mailbox addressed by an admission. */
export type DeliveryMailboxAddress = Pick<DeliveryAdmission, 'namespace' | 'provider' | 'installationId' | 'resourceId'>

export const deliveryMailboxKey = (admission: DeliveryMailboxAddress) =>
	[admission.namespace, admission.provider, admission.installationId, admission.resourceId]
		.map((segment) => `${segment.length}:${segment}`)
		.join('|')

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

export class MailboxDelivery extends Context.Service<
	MailboxDelivery,
	{
		readonly deliver: (admission: DeliveryAdmission) => Effect.Effect<DeliveryReceipt, MailboxDeliveryError>
	}
>()('@humanlayer/channels-delivery-next/MailboxDelivery') {}
