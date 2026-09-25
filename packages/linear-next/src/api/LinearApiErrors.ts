import { Effect, Schema, Stream } from 'effect'

import { LinearApiError, LinearApiOperation } from '../LinearApi'

const RetryAfterMilliseconds = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))
const LinearProviderErrorFields = {
	operation: LinearApiOperation,
	retryable: Schema.Boolean,
	status: Schema.NullOr(Schema.Int),
	message: Schema.NullOr(Schema.String),
	retryAfterMs: Schema.NullOr(RetryAfterMilliseconds),
}

export class LinearTransportError extends Schema.TaggedError<LinearTransportError>()('LinearTransportError', {
	...LinearProviderErrorFields,
}) {}

export class LinearAuthenticationError extends Schema.TaggedError<LinearAuthenticationError>()(
	'LinearAuthenticationError',
	{ ...LinearProviderErrorFields },
) {}

export class LinearForbiddenError extends Schema.TaggedError<LinearForbiddenError>()('LinearForbiddenError', {
	...LinearProviderErrorFields,
}) {}

export class LinearResourceNotFoundError extends Schema.TaggedError<LinearResourceNotFoundError>()(
	'LinearResourceNotFoundError',
	{ ...LinearProviderErrorFields },
) {}

export class LinearValidationError extends Schema.TaggedError<LinearValidationError>()('LinearValidationError', {
	...LinearProviderErrorFields,
}) {}

export class LinearRateLimitedError extends Schema.TaggedError<LinearRateLimitedError>()('LinearRateLimitedError', {
	...LinearProviderErrorFields,
}) {}

export class LinearUnavailableError extends Schema.TaggedError<LinearUnavailableError>()('LinearUnavailableError', {
	...LinearProviderErrorFields,
}) {}

export class LinearGraphqlRequestError extends Schema.TaggedError<LinearGraphqlRequestError>()(
	'LinearGraphqlRequestError',
	{ ...LinearProviderErrorFields },
) {}

export class LinearResponseDecodeError extends Schema.TaggedError<LinearResponseDecodeError>()(
	'LinearResponseDecodeError',
	{ ...LinearProviderErrorFields },
) {}

export class LinearMutationRejectedError extends Schema.TaggedError<LinearMutationRejectedError>()(
	'LinearMutationRejectedError',
	{ ...LinearProviderErrorFields },
) {}

export class LinearFileOriginRejectedError extends Schema.TaggedError<LinearFileOriginRejectedError>()(
	'LinearFileOriginRejectedError',
	{ ...LinearProviderErrorFields },
) {}

export class LinearFileRedirectError extends Schema.TaggedError<LinearFileRedirectError>()('LinearFileRedirectError', {
	...LinearProviderErrorFields,
}) {}

export class LinearFileTransferRejectedError extends Schema.TaggedError<LinearFileTransferRejectedError>()(
	'LinearFileTransferRejectedError',
	{ ...LinearProviderErrorFields },
) {}

export type LinearProviderError =
	| LinearTransportError
	| LinearAuthenticationError
	| LinearForbiddenError
	| LinearResourceNotFoundError
	| LinearValidationError
	| LinearRateLimitedError
	| LinearUnavailableError
	| LinearGraphqlRequestError
	| LinearResponseDecodeError
	| LinearMutationRejectedError
	| LinearFileOriginRejectedError
	| LinearFileRedirectError
	| LinearFileTransferRejectedError

type LinearProviderErrorDetails = {
	readonly operation: LinearApiOperation
	readonly retryable: boolean
	readonly status: number | null
	readonly message: string | null
	readonly retryAfterMs: number | null
}

export const linearProviderErrorDetails = (
	operation: LinearApiOperation,
	overrides: Partial<Omit<LinearProviderErrorDetails, 'operation'>> = {},
): LinearProviderErrorDetails => ({
	operation,
	retryable: overrides.retryable ?? false,
	status: overrides.status ?? null,
	message: overrides.message ?? null,
	retryAfterMs: overrides.retryAfterMs ?? null,
})

const toLinearApiError = (error: LinearProviderError, reason: LinearApiError['reason']): LinearApiError => {
	const details: {
		operation: LinearApiOperation
		reason: LinearApiError['reason']
		retryable: boolean
		status?: number
		message?: string
		retryAfterMs?: number
	} = { operation: error.operation, reason, retryable: error.retryable }
	if (error.status !== null) details.status = error.status
	if (error.message !== null) details.message = error.message
	if (error.retryAfterMs !== null) details.retryAfterMs = error.retryAfterMs
	return LinearApiError.make(details)
}

/** Narrows provider-boundary failures to the stable public LinearApi contract. */
export const narrowLinearProviderErrors = <A, R>(
	effect: Effect.Effect<A, LinearProviderError | LinearApiError, R>,
): Effect.Effect<A, LinearApiError, R> =>
	effect.pipe(
		Effect.catchTags({
			LinearTransportError: (error) => Effect.fail(toLinearApiError(error, 'unavailable')),
			LinearAuthenticationError: (error) => Effect.fail(toLinearApiError(error, 'unauthorized')),
			LinearForbiddenError: (error) => Effect.fail(toLinearApiError(error, 'forbidden')),
			LinearResourceNotFoundError: (error) => Effect.fail(toLinearApiError(error, 'not_found')),
			LinearValidationError: (error) => Effect.fail(toLinearApiError(error, 'validation')),
			LinearRateLimitedError: (error) => Effect.fail(toLinearApiError(error, 'rate_limited')),
			LinearUnavailableError: (error) => Effect.fail(toLinearApiError(error, 'unavailable')),
			LinearGraphqlRequestError: (error) => Effect.fail(toLinearApiError(error, 'rejected')),
			LinearResponseDecodeError: (error) => Effect.fail(toLinearApiError(error, 'invalid_response')),
			LinearMutationRejectedError: (error) => Effect.fail(toLinearApiError(error, 'rejected')),
			LinearFileOriginRejectedError: (error) => Effect.fail(toLinearApiError(error, 'validation')),
			LinearFileRedirectError: (error) => Effect.fail(toLinearApiError(error, 'invalid_response')),
			LinearFileTransferRejectedError: (error) => Effect.fail(toLinearApiError(error, 'rejected')),
		}),
	)

/** Narrows failures raised while a file byte stream is being consumed. */
export const narrowLinearProviderStreamErrors = <A, R>(
	stream: Stream.Stream<A, LinearTransportError, R>,
): Stream.Stream<A, LinearApiError, R> =>
	stream.pipe(Stream.catchTag('LinearTransportError', (error) => Stream.fail(toLinearApiError(error, 'unavailable'))))

export const failLinearMutation = (operation: LinearApiOperation) =>
	Effect.fail(new LinearMutationRejectedError(linearProviderErrorDetails(operation)))
