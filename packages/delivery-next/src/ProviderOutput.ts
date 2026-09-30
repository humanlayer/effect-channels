/**
 * This file defines how saved output reaches a provider.
 *
 * A provider's output processor turns one saved operation into its own API call, such as a Slack post,
 * and reports whether it was applied. It gets the destination the provider saved before the callback
 * ran, never storage. `ProviderOutputDispatcher` picks the processor of the provider that owns the
 * delivery.
 */
import { Context, Effect, Layer, Predicate, Schema } from 'effect'

import { PreparedDeliveryInvocation } from './DeliveryContext'
import { DeliveryOperationId, DeliveryOutputOperation } from './DeliveryOperation'
import { DeliveryId } from './DeliveryReference'

/**
 * One attempt at one operation, as a provider sees it.
 *
 * @property prepared - the callback, destination, and presentation version the provider saved
 * @property hadAmbiguousAttempt - an earlier attempt may already have applied this operation
 */
export const ProviderOutputAttempt = Schema.Struct({
	deliveryId: DeliveryId,
	operationId: DeliveryOperationId,
	attempt: Schema.Int.check(Schema.isGreaterThan(0)),
	hadAmbiguousAttempt: Schema.Boolean,
	prepared: PreparedDeliveryInvocation,
	operation: DeliveryOutputOperation,
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
export type ProviderOutputProcessor = {
	readonly namespace: string
	readonly providerName: string
	readonly process: (attempt: ProviderOutputAttempt) => Effect.Effect<DeliveryOutputApplied, DeliveryOutputFailed>
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
>()('@humanlayer/channels-delivery-next/ProviderOutputDispatcher') {}

export const ProviderOutputDispatcherLive = (processors: ReadonlyArray<ProviderOutputProcessor>) =>
	Layer.succeed(
		ProviderOutputDispatcher,
		ProviderOutputDispatcher.of({
			process: Effect.fn('delivery.process_provider_output')(function* ({ namespace, provider, attempt }) {
				const processor = processors.find(
					(candidate) => candidate.namespace === namespace && candidate.providerName === provider,
				)
				if (Predicate.isUndefined(processor)) {
					return yield* new ProviderOutputProcessorNotFound({ namespace, provider })
				}
				return yield* processor.process(attempt)
			}),
		}),
	)
