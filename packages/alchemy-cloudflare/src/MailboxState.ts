import {
	DeliveryAdmission,
	DeliverySlot,
	MailboxSequence,
	Timestamp,
	emptyDeliverySlot,
} from '@humanlayer/channels-delivery'
import { Schema } from 'effect'

/** One event waiting in the mailbox for a later batch. */
export const WaitingAdmission = Schema.Struct({
	sequence: MailboxSequence,
	arrivedAt: Timestamp,
	admission: DeliveryAdmission,
})
export type WaitingAdmission = typeof WaitingAdmission.Type

/**
 * The one mailbox a Durable Object holds.
 *
 * @property waiting - events not yet claimed, oldest first
 * @property deliveries - the active delivery and recently finished ones. Its `readyAt` is when
 * processing should next look at this mailbox; the alarm is set to the same time.
 */
export const DurableMailboxState = Schema.Struct({
	mailboxKey: Schema.NonEmptyString,
	provider: Schema.NonEmptyString,
	nextSequence: MailboxSequence,
	waiting: Schema.Array(WaitingAdmission),
	deliveries: DeliverySlot,
})
export type DurableMailboxState = typeof DurableMailboxState.Type

export const emptyMailboxState = (mailbox: { readonly mailboxKey: string; readonly provider: string }) =>
	DurableMailboxState.make({
		mailboxKey: mailbox.mailboxKey,
		provider: mailbox.provider,
		nextSequence: 0,
		waiting: [],
		deliveries: emptyDeliverySlot,
	})

export const mailboxStateKey = 'mailbox-state'
