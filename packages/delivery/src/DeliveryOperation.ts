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
import { Effect, Match, Predicate, Schema } from 'effect'

import { SetActivity } from './DeliveryActivity'
import { AddExternalLink } from './DeliveryLink'
import { CreateMessage, DeleteMessage, MessageId, UpdateMessage } from './DeliveryMessage'
import { PresentOutcome } from './DeliveryOutcome'
import { RenderPlan } from './DeliveryPlan'
import { DeliveryReactionTarget, SetMessageReaction } from './DeliveryReaction'
import { Timestamp } from './MailboxPolicy'

/** Names one output operation within its delivery. Stable across every attempt. */
export const DeliveryOperationId = Schema.NonEmptyString.check(
	Schema.isMaxLength(64),
	Schema.isPattern(/^[A-Za-z0-9_-]+$/),
).pipe(Schema.brand('DeliveryOperationId'))
export type DeliveryOperationId = typeof DeliveryOperationId.Type

/** Provider output a delivery owes. */
export const DeliveryOutputOperation = Schema.Union([
	PresentOutcome,
	AddExternalLink,
	CreateMessage,
	UpdateMessage,
	DeleteMessage,
	SetActivity,
	SetMessageReaction,
	RenderPlan,
])
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
 * @property idempotencyKey - a random UUID given at the first claim and sent on every attempt after it.
 * A provider that takes a client-chosen ID, such as Linear for Agent Activities, sends it so a repeated
 * attempt is refused instead of applied twice. A `SetActivity` replaced before it is sent loses its key,
 * because its next attempt is a new request.
 */
export const DeliveryOperation = Schema.Struct({
	operationId: DeliveryOperationId,
	operation: DeliveryOutputOperation,
	state: DeliveryOperationState,
	attempt: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
	hadAmbiguousAttempt: Schema.Boolean,
	idempotencyKey: Schema.optionalKey(Schema.NonEmptyString),
})
export type DeliveryOperation = typeof DeliveryOperation.Type

/**
 * What a remote worker may read about one operation.
 *
 * @property messageId - the message a message operation, or a reaction on a message, acts on
 * @property hadAmbiguousAttempt - an attempt's lease ran out, so the provider may have applied it more than once
 */
export const DeliveryOutputStatus = Schema.Struct({
	operationId: DeliveryOperationId,
	kind: Schema.Literals([
		'PresentOutcome',
		'AddExternalLink',
		'CreateMessage',
		'UpdateMessage',
		'DeleteMessage',
		'SetActivity',
		'SetMessageReaction',
		'RenderPlan',
	]),
	messageId: Schema.optionalKey(MessageId),
	state: Schema.Literals(['Pending', 'Delivering', 'Delivered', 'Failed']),
	attempts: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
	/** Stores written before this field existed read it as false. */
	hadAmbiguousAttempt: Schema.Boolean.pipe(Schema.withDecodingDefaultKey(Effect.succeed(false))),
})
export type DeliveryOutputStatus = typeof DeliveryOutputStatus.Type

/** The message a saved operation acts on, when it acts on one: a message change, or a reaction on a message. */
export const operationMessageId = (operation: DeliveryOutputOperation): MessageId | undefined =>
	Match.value(operation).pipe(
		Match.tagsExhaustive({
			PresentOutcome: () => undefined,
			AddExternalLink: () => undefined,
			SetActivity: () => undefined,
			RenderPlan: () => undefined,
			CreateMessage: ({ messageId }) => messageId,
			UpdateMessage: ({ messageId }) => messageId,
			DeleteMessage: ({ messageId }) => messageId,
			SetMessageReaction: ({ target }) =>
				DeliveryReactionTarget.match(target, {
					ActivationTarget: () => undefined,
					MessageTarget: ({ messageId }) => messageId,
					PlanTarget: () => undefined,
				}),
		}),
	)

export const deliveryOutputStatus = (operation: DeliveryOperation) => {
	const status = {
		operationId: operation.operationId,
		kind: operation.operation._tag,
		state: operation.state._tag,
		attempts: operation.attempt,
		hadAmbiguousAttempt: operation.hadAmbiguousAttempt,
	}
	const messageId = operationMessageId(operation.operation)
	return DeliveryOutputStatus.make(Predicate.isUndefined(messageId) ? status : { ...status, messageId })
}

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
