import {
	DeliveryAdmission,
	DeliveryAdmissionBatch,
	MailboxProcessingAttemptResult,
	MailboxSequence,
	Timestamp,
} from '@humanlayer/channels-delivery-next'
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
 * @property activeBatch - the frozen batch of the current claim or pending retry
 * @property readyAt - when processing should next look at this mailbox; the alarm is set to the same time
 */
export const DurableMailboxState = Schema.Struct({
	mailboxKey: Schema.NonEmptyString,
	provider: Schema.NonEmptyString,
	status: Schema.Literals(['idle', 'active', 'retry']),
	nextSequence: MailboxSequence,
	waiting: Schema.Array(WaitingAdmission),
	activeBatch: Schema.NullOr(DeliveryAdmissionBatch),
	lastResult: Schema.NullOr(MailboxProcessingAttemptResult),
	claimId: Schema.NullOr(Schema.NonEmptyString),
	attempt: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
	readyAt: Schema.NullOr(Timestamp),
})
export type DurableMailboxState = typeof DurableMailboxState.Type

export const emptyMailboxState = (mailbox: { readonly mailboxKey: string; readonly provider: string }) =>
	DurableMailboxState.make({
		mailboxKey: mailbox.mailboxKey,
		provider: mailbox.provider,
		status: 'idle',
		nextSequence: 0,
		waiting: [],
		activeBatch: null,
		lastResult: null,
		claimId: null,
		attempt: 0,
		readyAt: null,
	})

export const mailboxStateKey = 'mailbox-state'
