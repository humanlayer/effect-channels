import { Effect } from 'effect'
import type { HttpClientResponse } from 'effect/unstable/http/HttpClientResponse'

import type { LinearApiOperation } from '../LinearApi'
import {
	LinearAuthenticationError,
	LinearFileTransferRejectedError,
	LinearForbiddenError,
	LinearRateLimitedError,
	LinearResourceNotFoundError,
	LinearUnavailableError,
	linearProviderErrorDetails,
} from './LinearApiErrors'

const parseRetryAfterMs = (value: string | undefined) => {
	if (value === undefined || !/^\d+$/.test(value)) return null
	const milliseconds = Number(value) * 1_000
	return Number.isSafeInteger(milliseconds) ? milliseconds : null
}

/**
 * Classifies a non-redirect file transfer response. Authentication and access statuses are only credential failures
 * when the request carried the workspace credential; for signed or unauthenticated targets they are rejections.
 */
export const inspectLinearFileResponse = (
	operation: LinearApiOperation,
	response: HttpClientResponse,
	credentialed: boolean,
) => {
	const status = response.status
	if (status >= 200 && status < 300) return Effect.succeed(response)
	if (credentialed && status === 401)
		return Effect.fail(new LinearAuthenticationError(linearProviderErrorDetails(operation, { status })))
	if (credentialed && status === 403)
		return Effect.fail(new LinearForbiddenError(linearProviderErrorDetails(operation, { status })))
	if (status === 404)
		return Effect.fail(new LinearResourceNotFoundError(linearProviderErrorDetails(operation, { status })))
	if (status === 429)
		return Effect.fail(
			new LinearRateLimitedError(
				linearProviderErrorDetails(operation, {
					status,
					retryable: true,
					retryAfterMs: parseRetryAfterMs(response.headers['retry-after']),
				}),
			),
		)
	if (status >= 500)
		return Effect.fail(
			new LinearUnavailableError(linearProviderErrorDetails(operation, { status, retryable: true })),
		)
	return Effect.fail(new LinearFileTransferRejectedError(linearProviderErrorDetails(operation, { status })))
}
