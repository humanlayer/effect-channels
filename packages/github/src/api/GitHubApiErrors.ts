import { Match, Predicate, Schema, Struct } from 'effect'

import { GitHubApiError, type GitHubApiErrorReason, type GitHubApiOperation } from '../GitHubApi'

export const GitHubTransportErrorFields = Schema.Struct({
	stage: Schema.Literals(['transport', 'status', 'decode', 'signing', 'token_expiry', 'pagination', 'redirect']),
	status: Schema.optionalKey(Schema.Int),
	/** GitHub's `message` from an error response body. Named apart from `Error.message`, which is not an enumerable own property. */
	responseMessage: Schema.optionalKey(Schema.String),
	rateLimited: Schema.optionalKey(Schema.Boolean),
	retryAfterMs: Schema.optionalKey(Schema.Int),
})

export class GitHubTransportError extends Schema.TaggedError<GitHubTransportError>()(
	'GitHubTransportError',
	GitHubTransportErrorFields.fields,
) {}

const classification = (reason: GitHubApiErrorReason, retryable: boolean) => ({ reason, retryable })

const classifyTransportError = (operation: GitHubApiOperation, error: GitHubTransportError) =>
	Match.value({ operation, stage: error.stage, status: error.status, rateLimited: error.rateLimited }).pipe(
		Match.when({ rateLimited: true }, () => classification('rate_limited', true)),
		Match.whenOr({ status: 401 }, { stage: 'signing' }, () => classification('authentication', false)),
		Match.when({ operation: 'merge_pull_request', status: 409 }, () => classification('stale_head', false)),
		Match.when({ operation: 'merge_pull_request', status: 405 }, () => classification('not_mergeable', false)),
		Match.when({ status: 403 }, () => classification('forbidden', false)),
		Match.when({ status: Match.is(404, 410) }, () => classification('not_found', false)),
		Match.when({ stage: Match.is('decode', 'token_expiry', 'pagination', 'redirect') }, () =>
			classification('invalid_response', false),
		),
		Match.when({ status: (status) => Predicate.isNotUndefined(status) && status >= 400 && status < 500 }, () =>
			classification('validation', false),
		),
		Match.orElse(() => classification('unavailable', true)),
	)

export const narrowGitHubTransportError = (operation: GitHubApiOperation, error: GitHubTransportError) =>
	GitHubApiError.make({
		operation,
		...classifyTransportError(operation, error),
		...Struct.renameKeys(Struct.pick(error, ['status', 'responseMessage', 'retryAfterMs']), {
			responseMessage: 'message',
		}),
	})
