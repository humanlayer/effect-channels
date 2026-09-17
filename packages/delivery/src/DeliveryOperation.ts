import { Effect, Encoding, Match, Schema } from 'effect'

import { DeliveryId, DeliveryOperationId, DeliveryTerminalOutcome } from './protocol'

/** Maximum UTF-8 size accepted for a persisted final Markdown message. */
export const FINAL_MESSAGE_MARKDOWN_MAX_BYTES = 65_536
export const FINAL_MESSAGE_MARKDOWN_MAX_LENGTH = FINAL_MESSAGE_MARKDOWN_MAX_BYTES
export const FinalMessageMarkdown = Schema.NonEmptyString.check(
	Schema.isMaxLength(FINAL_MESSAGE_MARKDOWN_MAX_LENGTH),
	Schema.makeFilter((markdown) => new TextEncoder().encode(markdown).byteLength <= FINAL_MESSAGE_MARKDOWN_MAX_BYTES),
)

export const PendingDeliveryOperation = Schema.TaggedStruct('Pending', {
	attempt: Schema.Natural,
	readyAt: Schema.Finite,
	hadAmbiguousAttempt: Schema.Boolean,
})
export const DeliveringDeliveryOperation = Schema.TaggedStruct('Delivering', {
	owner: Schema.Natural,
	attempt: Schema.Int.check(Schema.isGreaterThan(0)),
	leaseUntil: Schema.Finite,
	hadAmbiguousAttempt: Schema.Boolean,
})
export const DeliveredDeliveryOperation = Schema.TaggedStruct('Delivered', {
	attempt: Schema.Int.check(Schema.isGreaterThan(0)),
	deliveredAt: Schema.Finite,
	providerReceipt: Schema.NonEmptyString,
	hadAmbiguousAttempt: Schema.Boolean,
})
export const FailedDeliveryOperation = Schema.TaggedStruct('DeliveryFailed', {
	attempt: Schema.Int.check(Schema.isGreaterThan(0)),
	failedAt: Schema.Finite,
	reason: Schema.Literals(['non_retryable', 'exhausted']),
	safeCode: Schema.optionalKey(Schema.NonEmptyString),
	hadAmbiguousAttempt: Schema.Boolean,
})
export const DeliveryOperationState = Schema.Union([
	PendingDeliveryOperation,
	DeliveringDeliveryOperation,
	DeliveredDeliveryOperation,
	FailedDeliveryOperation,
])
export type DeliveryOperationState = typeof DeliveryOperationState.Type

export const FinalMessageOperation = Schema.TaggedStruct('FinalMessage', {
	operationId: DeliveryOperationId,
	deliveryId: DeliveryId,
	outcome: DeliveryTerminalOutcome,
	markdown: FinalMessageMarkdown,
	provider: Schema.NonEmptyString,
	installation: Schema.NonEmptyString,
	destination: Schema.String,
	presentation: Schema.NonEmptyString,
	presentationVersion: Schema.NonEmptyString,
	state: DeliveryOperationState,
})
export type FinalMessageOperation = typeof FinalMessageOperation.Type
export const DeliveryOperation = FinalMessageOperation
export type DeliveryOperation = typeof DeliveryOperation.Type

export const DeliveryOutputReceipt = Schema.Struct({ providerReceipt: Schema.NonEmptyString })
export interface DeliveryOutputReceipt extends Schema.Schema.Type<typeof DeliveryOutputReceipt> {}

export class DeliveryOutputError extends Schema.TaggedError<DeliveryOutputError>()('DeliveryOutputError', {
	retryable: Schema.Boolean,
	retryAfterMs: Schema.optionalKey(
		Schema.Int.check(Schema.isGreaterThanOrEqualTo(0), Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER)),
	),
	safeCode: Schema.optionalKey(Schema.NonEmptyString),
}) {}

export const finalMessageOperationId = (deliveryId: DeliveryId): DeliveryOperationId =>
	DeliveryOperationId.make(`operation:v1:${Encoding.encodeBase64Url(deliveryId)}:final`)

export const deliveryIdFromOperationId = (
	operationId: DeliveryOperationId,
): Effect.Effect<DeliveryId, Error | Schema.SchemaError> => {
	const encoded = operationId.slice('operation:v1:'.length, -':final'.length)
	return Effect.fromResult(Encoding.decodeBase64UrlString(encoded)).pipe(
		Effect.flatMap(Schema.decodeUnknownEffect(DeliveryId)),
	)
}

export const deliveryOperationStatus = (state: DeliveryOperationState) => {
	return Match.value(state).pipe(
		Match.tagsExhaustive({
			Pending: (pending) => (pending.attempt === 0 ? ('pending' as const) : ('retrying' as const)),
			Delivering: () => 'delivering' as const,
			Delivered: () => 'delivered' as const,
			DeliveryFailed: () => 'delivery_failed' as const,
		}),
	)
}
