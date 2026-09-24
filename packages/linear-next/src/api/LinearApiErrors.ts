import { Schema } from 'effect'

import { LinearApiError, type LinearApiOperation } from '../LinearApi'

/** Rich transport failure retained inside the package until the orchestration boundary narrows it. */
export class LinearProviderError extends Schema.TaggedError<LinearProviderError>()('LinearProviderError', {
	operation: Schema.String,
	reason: Schema.Literals([
		'transport',
		'authentication',
		'forbidden',
		'not_found',
		'validation',
		'rate_limited',
		'unavailable',
		'graphql',
		'decode',
	]),
	retryable: Schema.Boolean,
	status: Schema.optionalKey(Schema.Int),
	message: Schema.optionalKey(Schema.String),
	retryAfterMs: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))),
}) {
	readonly _operation!: LinearApiOperation
}

export const narrowLinearProviderError = (error: LinearProviderError): LinearApiError =>
	LinearApiError.make({
		operation: error.operation as LinearApiOperation,
		reason:
			error.reason === 'transport' || error.reason === 'unavailable'
				? 'unavailable'
				: error.reason === 'authentication'
					? 'unauthorized'
					: error.reason === 'graphql'
						? 'rejected'
						: error.reason === 'decode'
							? 'invalid_response'
							: error.reason,
		retryable: error.retryable,
		...(error.status === undefined ? {} : { status: error.status }),
		...(error.message === undefined ? {} : { message: error.message }),
		...(error.retryAfterMs === undefined ? {} : { retryAfterMs: error.retryAfterMs }),
	})
