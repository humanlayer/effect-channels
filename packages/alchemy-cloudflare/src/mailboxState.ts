import {
	DeliveryAdmission,
	DeliveryAdmissionBatch,
	MailboxProcessingAttemptResult,
	Timestamp,
} from '@humanlayer/channels-delivery-next'
import { Schema } from 'effect'

export const DurableMailboxState = Schema.Struct({
	mailboxKey: Schema.NonEmptyString,
	status: Schema.Literals(['idle', 'active', 'retry']),
	nextSequence: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
	pending: Schema.Array(DeliveryAdmission),
	activeBatch: Schema.NullOr(DeliveryAdmissionBatch),
	lastResult: Schema.NullOr(MailboxProcessingAttemptResult),
	claimId: Schema.NullOr(Schema.NonEmptyString),
	attempt: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
	readyAt: Schema.NullOr(Timestamp),
})
export type DurableMailboxState = typeof DurableMailboxState.Type

export const emptyMailboxState = (mailboxKey: string): DurableMailboxState =>
	DurableMailboxState.make({
		mailboxKey,
		status: 'idle',
		nextSequence: 0,
		pending: [],
		activeBatch: null,
		lastResult: null,
		claimId: null,
		attempt: 0,
		readyAt: null,
	})

export const mailboxStateKey = 'mailbox-state'
