import { Predicate, Schema } from 'effect'

import { GitHubApiError, type GitHubApiOperation } from '../GitHubApi'

export class GitHubTransportError extends Schema.TaggedError<GitHubTransportError>()('GitHubTransportError', {
	stage: Schema.Literals(['transport', 'status', 'decode', 'signing', 'token_expiry', 'pagination', 'redirect']),
	status: Schema.optionalKey(Schema.Int),
	message: Schema.optionalKey(Schema.String),
	rateLimited: Schema.optionalKey(Schema.Boolean),
	retryAfterMs: Schema.optionalKey(Schema.Int),
}) {}

export const narrowGitHubTransportError = (
	operation: GitHubApiOperation,
	error: GitHubTransportError,
): GitHubApiError => {
	const details: { status?: number; message?: string; retryAfterMs?: number } = {}
	if (Predicate.isNotUndefined(error.status)) details.status = error.status
	if (Predicate.isNotUndefined(error.message)) details.message = error.message
	if (Predicate.isNotUndefined(error.retryAfterMs)) details.retryAfterMs = error.retryAfterMs
	if (error.rateLimited === true) {
		return GitHubApiError.make({ operation, reason: 'rate_limited', retryable: true, ...details })
	}
	if (error.status === 401 || error.stage === 'signing') {
		return GitHubApiError.make({ operation, reason: 'authentication', retryable: false, ...details })
	}
	if (operation === 'merge_pull_request' && error.status === 409) {
		return GitHubApiError.make({ operation, reason: 'stale_head', retryable: false, ...details })
	}
	if (operation === 'merge_pull_request' && error.status === 405) {
		return GitHubApiError.make({ operation, reason: 'not_mergeable', retryable: false, ...details })
	}
	if (error.status === 403) {
		return GitHubApiError.make({ operation, reason: 'forbidden', retryable: false, ...details })
	}
	if (error.status === 404 || error.status === 410) {
		return GitHubApiError.make({ operation, reason: 'not_found', retryable: false, ...details })
	}
	if (
		error.stage === 'decode' ||
		error.stage === 'token_expiry' ||
		error.stage === 'pagination' ||
		error.stage === 'redirect'
	) {
		return GitHubApiError.make({ operation, reason: 'invalid_response', retryable: false, ...details })
	}
	if (error.status === 409 || error.status === 422) {
		return GitHubApiError.make({ operation, reason: 'validation', retryable: false, ...details })
	}
	if (Predicate.isNotUndefined(error.status) && error.status >= 400 && error.status < 500) {
		return GitHubApiError.make({ operation, reason: 'validation', retryable: false, ...details })
	}
	return GitHubApiError.make({ operation, reason: 'unavailable', retryable: true, ...details })
}
