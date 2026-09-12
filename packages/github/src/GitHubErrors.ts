import { Schema } from 'effect'

export class GitHubError extends Schema.TaggedError<GitHubError>()('GitHubError', {
	reason: Schema.Literals([
		'configuration',
		'authentication',
		'forbidden',
		'not_found',
		'invalid_input',
		'unavailable',
		'response',
	]),
	retryAfterMs: Schema.optionalKey(
		Schema.Int.check(Schema.isGreaterThanOrEqualTo(0), Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER)),
	),
}) {}

export class GitHubWebhookError extends Schema.TaggedError<GitHubWebhookError>()('GitHubWebhookError', {
	reason: Schema.Literals(['signature', 'decode', 'installation', 'capacity', 'crypto']),
}) {}

export class GitHubIngressError extends Schema.TaggedError<GitHubIngressError>()('GitHubIngressError', {
	operation: Schema.Literals(['configuration', 'admit', 'process', 'run']),
	reason: Schema.optionalKey(Schema.Literals(['unavailable', 'unexpected'])),
}) {}
