import { Schema } from 'effect'

const Positive = Schema.Int.check(Schema.isGreaterThan(0))

export const DeliveryPolicy = Schema.Struct({
	mode: Schema.Literals(['queue', 'serial']),
	maxPayloadBytes: Positive,
	maxEnvelopes: Positive,
	maxOutcomes: Positive,
	retentionMs: Positive,
	maxAttempts: Positive,
	retryBaseMs: Positive,
	retryMaxMs: Positive,
	leaseMs: Positive,
	heartbeatMs: Positive,
	conflictRetries: Schema.Natural,
}).check(Schema.makeFilter((policy) => policy.heartbeatMs < policy.leaseMs && policy.retryBaseMs <= policy.retryMaxMs))
export type DeliveryPolicy = typeof DeliveryPolicy.Type
