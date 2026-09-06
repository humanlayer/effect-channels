import { Schema } from 'effect'

const Positive = Schema.Int.check(Schema.isGreaterThan(0))

const limits = {
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
}

export const DeliveryPolicy = Schema.Union([
	Schema.Struct({ ...limits, mode: Schema.Literals(['queue', 'serial', 'drop']) }),
	Schema.Struct({ ...limits, mode: Schema.Literal('concurrent'), maxConcurrency: Positive }),
	Schema.Struct({ ...limits, mode: Schema.Literal('debounce'), quietPeriodMs: Positive }),
	Schema.Struct({ ...limits, mode: Schema.Literal('burst'), windowMs: Positive }),
]).check(Schema.makeFilter((policy) => policy.heartbeatMs < policy.leaseMs && policy.retryBaseMs <= policy.retryMaxMs))
export type DeliveryPolicy = typeof DeliveryPolicy.Type
