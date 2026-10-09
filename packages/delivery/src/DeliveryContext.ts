/**
 * This file defines what an application callback receives about its delivery, and what a provider
 * processor saves before the callback runs.
 *
 * `DeliveryContext` is the callback's second argument. It names the delivery, gives the remote worker's
 * token, and can hand the delivery off so the callback may return while a remote worker finishes it.
 *
 * `ProviderDeliveryExecution` is internal: the provider processor uses it to save which callback a batch
 * runs, and where its output goes, before any application code runs. A retry reuses what was saved.
 */
import { Data, Schema } from 'effect'
import type { Duration, Effect, Option, Redacted } from 'effect'

import type { ExternalLink } from './DeliveryLink'
import { DeliveryReactionTargetKind } from './DeliveryReaction'
import { ConversationId, DeliveryId } from './DeliveryReference'

/** How long a handed-off delivery may go without a request from its remote worker, unless the handoff says otherwise. */
export const DEFAULT_HANDOFF_FAIL_AFTER = '24 hours' satisfies Duration.Input

/**
 * @property links - links to show on the delivery, such as the remote job's page
 * @property failAfter - how long the remote worker may go without a request before the delivery fails on
 * its own, with outcome `Failed` and reason `TimedOut`. Every request it sends, except a status read,
 * starts the time again. Defaults to 24 hours; `'Infinity'` means no limit.
 */
export type HandoffOptions = {
	readonly links?: ReadonlyArray<ExternalLink>
	readonly failAfter?: Duration.Input
}

/** Returned by a callback that handed its delivery off. The saved handoff, not this value, is authoritative. */
export const DeliveryHandoff = Schema.TaggedStruct('DeliveryHandoff', {
	deliveryId: DeliveryId,
})
export type DeliveryHandoff = typeof DeliveryHandoff.Type

/** What an application callback may return. */
export type DeliveryCallbackResult = void | DeliveryHandoff

/**
 * The output operations a destination can show. Every request a remote worker makes needs its
 * operation listed, including `PresentOutcome` for `complete` and `fail`.
 */
export const DeliveryOperationKind = Schema.Literals([
	'PresentOutcome',
	'CreateMessage',
	'UpdateMessage',
	'DeleteMessage',
	'SetMessageReaction',
	'SetActivity',
	'RenderPlan',
	'AddExternalLink',
])
export type DeliveryOperationKind = typeof DeliveryOperationKind.Type

/**
 * What a provider saves before the callback runs. Every attempt of the batch uses it.
 *
 * @property name - which of the provider's callbacks this step runs
 * @property presentationVersion - the provider's version of `destination` and `activationTarget`
 * @property destination - where output goes, encoded by the provider and read only by it
 * @property activationTarget - the message, comment, or issue that started the delivery, when there is one
 * @property supportedOperations - the output operations the destination supports
 * @property reactionTargets - what `SetMessageReaction` can react on here. Absent means nothing, as for
 * a delivery prepared before reactions existed.
 */
export const PreparedDeliveryCallback = Schema.Struct({
	name: Schema.NonEmptyString,
	presentationVersion: Schema.Int.check(Schema.isGreaterThan(0)),
	destination: Schema.Json,
	activationTarget: Schema.optionalKey(Schema.Json),
	supportedOperations: Schema.Array(DeliveryOperationKind),
	reactionTargets: Schema.optionalKey(Schema.Array(DeliveryReactionTargetKind)),
})
export type PreparedDeliveryCallback = typeof PreparedDeliveryCallback.Type

export const PreparedDeliveryInvocation = Schema.Struct({
	callbacks: Schema.NonEmptyArray(PreparedDeliveryCallback),
})
export type PreparedDeliveryInvocation = typeof PreparedDeliveryInvocation.Type

/** A preparation as JSON text, for stores that keep it as a string. */
export const PreparedDeliveryInvocationJson = Schema.fromJsonString(PreparedDeliveryInvocation)

/** Where a delivery is in its life. */
export const DeliveryStage = Schema.Literals([
	'Local',
	'Retry',
	'ExternalCleaning',
	'ExternalWaiting',
	'Finishing',
	'Retired',
])
export type DeliveryStage = typeof DeliveryStage.Type

/** The store cannot hand deliveries off yet. */
export class DeliveryHandoffUnsupported extends Schema.TaggedError<DeliveryHandoffUnsupported>()(
	'DeliveryHandoffUnsupported',
	{},
) {}

/** The delivery can no longer be handed off: its claim was lost, or it already ended. */
export class DeliveryHandoffRejected extends Schema.TaggedError<DeliveryHandoffRejected>()('DeliveryHandoffRejected', {
	deliveryId: DeliveryId,
}) {}

/** The store could not be reached. */
export class DeliveryHandoffUnavailable extends Schema.TaggedError<DeliveryHandoffUnavailable>()(
	'DeliveryHandoffUnavailable',
	{ reason: Schema.String },
) {}

export type DeliveryHandoffError = DeliveryHandoffUnsupported | DeliveryHandoffRejected | DeliveryHandoffUnavailable

/** The batch already runs a different callback, or its claim was lost. The provider must not run anything. */
export class DeliveryPreparationConflict extends Schema.TaggedError<DeliveryPreparationConflict>()(
	'DeliveryPreparationConflict',
	{ deliveryId: DeliveryId },
) {}

/** The store could not be reached while saving the preparation. */
export class DeliveryPreparationUnavailable extends Schema.TaggedError<DeliveryPreparationUnavailable>()(
	'DeliveryPreparationUnavailable',
	{ reason: Schema.String },
) {}

export type DeliveryPreparationError = DeliveryPreparationConflict | DeliveryPreparationUnavailable

/**
 * The callback's view of its delivery.
 *
 * @property deliveryId - stable across retries; use it as the remote job's idempotency key
 * @property conversationId - stable across deliveries in the same thread, issue, or session
 * @property accessToken - pass to the remote worker; never log it
 * @property handoff - hand the delivery to a remote worker; call it after the remote job has started
 */
export class DeliveryContext extends Data.Class<{
	readonly deliveryId: DeliveryId
	readonly conversationId: ConversationId
	readonly accessToken: Redacted.Redacted<string>
	readonly handoff: (options?: HandoffOptions) => Effect.Effect<DeliveryHandoff, DeliveryHandoffError>
}> {}

/**
 * What mailbox processing gives a provider processor for one attempt at one batch.
 *
 * @property idempotencyKey - a UUID made from the delivery ID, the same on every attempt. Send it with
 * output the provider makes before the callback runs, such as Linear's first thought, where the provider
 * takes a client-chosen ID, so a retry cannot make that output twice.
 * @property prepared - what an earlier attempt saved; when present the provider must run that callback
 * @property prepare - save the callback choice and destination; returns what is saved, which may be an earlier identical record
 * @property context - the value to pass to the application callback
 */
export class ProviderDeliveryExecution extends Data.Class<{
	readonly deliveryId: DeliveryId
	readonly callbackIndex: number
	readonly idempotencyKey: string
	readonly prepared: Option.Option<PreparedDeliveryInvocation>
	readonly prepare: (
		invocation: PreparedDeliveryInvocation,
	) => Effect.Effect<PreparedDeliveryInvocation, DeliveryPreparationError>
	readonly context: DeliveryContext
}> {}
