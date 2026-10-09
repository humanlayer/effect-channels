/**
 * This file defines how saved output reaches a provider.
 *
 * A provider's output processor turns one saved operation into its own API call, such as a Slack post,
 * and reports whether it was applied. It gets the destination the provider saved before the callback
 * ran, never storage. `ProviderOutputDispatcher` picks the processor of the provider that owns the
 * delivery.
 */
import { Context, Effect, Layer, Predicate, Schema } from 'effect'

import { SetActivity } from './DeliveryActivity'
import { PreparedDeliveryCallback } from './DeliveryContext'
import { AddExternalLink } from './DeliveryLink'
import { CreateMessage, ProviderDeleteMessage, ProviderUpdateMessage } from './DeliveryMessage'
import { DeliveryOperationId } from './DeliveryOperation'
import { PresentOutcome } from './DeliveryOutcome'
import { ProviderRenderPlan } from './DeliveryPlan'
import { ProviderSetMessageReaction } from './DeliveryReaction'
import { DeliveryId } from './DeliveryReference'

/**
 * `PresentOutcome` as a provider receives it.
 *
 * @property clearActivity - the remote worker's last activity was `Working`, so the provider must clear it
 */
export const ProviderPresentOutcome = Schema.TaggedStruct('PresentOutcome', {
	...PresentOutcome.fields,
	clearActivity: Schema.Boolean,
})
export type ProviderPresentOutcome = typeof ProviderPresentOutcome.Type

/**
 * A saved operation as a provider receives it. An update or deletion, or a reaction on a message,
 * carries the provider's own reference to the message, taken from the receipt of the message's
 * `CreateMessage`. `RenderPlan` carries the plan the provider last showed.
 */
export const ProviderOutputOperation = Schema.Union([
	ProviderPresentOutcome,
	AddExternalLink,
	CreateMessage,
	ProviderUpdateMessage,
	ProviderDeleteMessage,
	SetActivity,
	ProviderSetMessageReaction,
	ProviderRenderPlan,
])
export type ProviderOutputOperation = typeof ProviderOutputOperation.Type

/**
 * One attempt at one operation, as a provider sees it.
 *
 * @property prepared - the callback, destination, and presentation version the provider saved
 * @property hadAmbiguousAttempt - an earlier attempt may already have applied this operation
 * @property idempotencyKey - a UUID v4 that is the same on every attempt at this operation. A provider
 * that takes a client-chosen ID sends it, and treats the provider's "already exists" answer as applied.
 */
export const ProviderOutputAttempt = Schema.Struct({
	deliveryId: DeliveryId,
	operationId: DeliveryOperationId,
	attempt: Schema.Int.check(Schema.isGreaterThan(0)),
	hadAmbiguousAttempt: Schema.Boolean,
	idempotencyKey: Schema.NonEmptyString,
	prepared: PreparedDeliveryCallback,
	operation: ProviderOutputOperation,
})
export type ProviderOutputAttempt = typeof ProviderOutputAttempt.Type

/** The provider applied the operation. `receipt` is its own reference to what it made, read only by it. */
export const DeliveryOutputApplied = Schema.TaggedStruct('DeliveryOutputApplied', {
	receipt: Schema.optionalKey(Schema.Json),
})
export type DeliveryOutputApplied = typeof DeliveryOutputApplied.Type

/** The provider could not apply the operation. A retryable failure runs again later; others give up. */
export class DeliveryOutputFailed extends Schema.TaggedError<DeliveryOutputFailed>()('DeliveryOutputFailed', {
	provider: Schema.NonEmptyString,
	retryable: Schema.Boolean,
	safeCode: Schema.NonEmptyString,
	retryAfterMs: Schema.optionalKey(
		Schema.Int.check(Schema.isGreaterThanOrEqualTo(0), Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER)),
	),
}) {}

/** No provider in this bot sends output for the delivery's provider. */
export class ProviderOutputProcessorNotFound extends Schema.TaggedError<ProviderOutputProcessorNotFound>()(
	'ProviderOutputProcessorNotFound',
	{ namespace: Schema.NonEmptyString, provider: Schema.NonEmptyString },
) {}

/** One provider's output half. Its API client is already supplied. */
export type ProviderOutputProcessor<R = never> = {
	readonly namespace: string
	readonly providerName: string
	readonly process: (attempt: ProviderOutputAttempt) => Effect.Effect<DeliveryOutputApplied, DeliveryOutputFailed, R>
}

/** Hands one output attempt to the processor of the provider that owns the delivery. */
export class ProviderOutputDispatcher extends Context.Service<
	ProviderOutputDispatcher,
	{
		readonly process: (input: {
			readonly namespace: string
			readonly provider: string
			readonly attempt: ProviderOutputAttempt
		}) => Effect.Effect<DeliveryOutputApplied, DeliveryOutputFailed | ProviderOutputProcessorNotFound>
	}
>()('@humanlayer/channels-delivery/ProviderOutputDispatcher') {}

export type ProviderOutputDispatcherOperations<R = never> = {
	readonly process: (input: {
		readonly namespace: string
		readonly provider: string
		readonly attempt: ProviderOutputAttempt
	}) => Effect.Effect<DeliveryOutputApplied, DeliveryOutputFailed | ProviderOutputProcessorNotFound, R>
}

export const makeProviderOutputDispatcher = <R>(
	processors: ReadonlyArray<ProviderOutputProcessor<R>>,
): ProviderOutputDispatcherOperations<R> => ({
	process: Effect.fn('delivery.process_provider_output')(function* ({ namespace, provider, attempt }) {
		const processor = processors.find(
			(candidate) => candidate.namespace === namespace && candidate.providerName === provider,
		)
		if (Predicate.isUndefined(processor)) return yield* new ProviderOutputProcessorNotFound({ namespace, provider })
		return yield* processor.process(attempt)
	}),
})

export const ProviderOutputDispatcherLive = <R = never>(processors: ReadonlyArray<ProviderOutputProcessor<R>>) =>
	Layer.effect(
		ProviderOutputDispatcher,
		Effect.gen(function* () {
			const context = yield* Effect.context<R>()
			const dispatcher = makeProviderOutputDispatcher(processors)
			return ProviderOutputDispatcher.of({
				process: (input) => dispatcher.process(input).pipe(Effect.provide(context)),
			})
		}),
	)
