/**
 * This file defines saved output operations: provider output a delivery still owes, such as its final
 * message, kept in the store until a provider has applied it.
 *
 * A request from a remote worker saves the operation and returns. Mailbox processing later claims it
 * under its own lease, gives it to the provider, and settles it. A provider outage retries the
 * operation, never the application callback.
 *
 * ```text
 * Pending ──claim──▶ Delivering ──applied──▶ Delivered
 *    ▲                   │ retryable failure, or its lease runs out
 *    └───────────────────┘
 *                        └── not retryable, or out of attempts ──▶ Failed
 * ```
 */
import { Schema } from 'effect'

import { AddExternalLink } from './DeliveryLink'
import { PresentOutcome } from './DeliveryOutcome'
import { Timestamp } from './MailboxPolicy'

/** Names one output operation within its delivery. Stable across every attempt. */
export const DeliveryOperationId = Schema.NonEmptyString.check(
	Schema.isMaxLength(64),
	Schema.isPattern(/^[A-Za-z0-9_-]+$/),
).pipe(Schema.brand('DeliveryOperationId'))
export type DeliveryOperationId = typeof DeliveryOperationId.Type

/** Provider output a delivery owes. */
export const DeliveryOutputOperation = Schema.Union([PresentOutcome, AddExternalLink])
export type DeliveryOutputOperation = typeof DeliveryOutputOperation.Type

/**
 * Where an operation is.
 *
 * - `Pending`: due at `readyAt`
 * - `Delivering`: claimed by one output attempt until `leaseUntil`
 * - `Delivered`: applied; `receipt` is the provider's own reference to what it made, when it made something
 * - `Failed`: not applied, and never will be
 */
export const DeliveryOperationState = Schema.TaggedUnion({
	Pending: { readyAt: Timestamp },
	Delivering: { claimId: Schema.NonEmptyString, leaseUntil: Timestamp },
	Delivered: { receipt: Schema.optionalKey(Schema.Json) },
	Failed: { safeCode: Schema.NonEmptyString },
})
export type DeliveryOperationState = typeof DeliveryOperationState.Type

/**
 * One saved output operation.
 *
 * @property attempt - how many attempts have claimed it
 * @property hadAmbiguousAttempt - an attempt's lease ran out, so the provider may have applied it already
 */
export const DeliveryOperation = Schema.Struct({
	operationId: DeliveryOperationId,
	operation: DeliveryOutputOperation,
	state: DeliveryOperationState,
	attempt: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
	hadAmbiguousAttempt: Schema.Boolean,
})
export type DeliveryOperation = typeof DeliveryOperation.Type

/** What a remote worker may read about one operation. */
export const DeliveryOutputStatus = Schema.Struct({
	operationId: DeliveryOperationId,
	kind: Schema.Literals(['PresentOutcome', 'AddExternalLink']),
	state: Schema.Literals(['Pending', 'Delivering', 'Delivered', 'Failed']),
	attempts: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
})
export type DeliveryOutputStatus = typeof DeliveryOutputStatus.Type

export const deliveryOutputStatus = (operation: DeliveryOperation) =>
	DeliveryOutputStatus.make({
		operationId: operation.operationId,
		kind: operation.operation._tag,
		state: operation.state._tag,
		attempts: operation.attempt,
	})

/** Whether an operation still needs a provider. */
export const isUnsettledOperation = (operation: DeliveryOperation) =>
	DeliveryOperationState.isAnyOf(['Pending', 'Delivering'])(operation.state)

/**
 * How an output attempt ended, as the store records it.
 *
 * - `Applied`: the provider applied it
 * - `Retry`: try again at `readyAt`
 * - `Failed`: give up
 */
export const DeliveryOutputSettlement = Schema.TaggedUnion({
	Applied: { receipt: Schema.optionalKey(Schema.Json) },
	Retry: { readyAt: Timestamp },
	Failed: { safeCode: Schema.NonEmptyString },
})
export type DeliveryOutputSettlement = typeof DeliveryOutputSettlement.Type
