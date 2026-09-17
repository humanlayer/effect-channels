import { Match, Schema } from 'effect'

import type { Retryability } from './DomainErrors'
import { SlackTeamId } from './SlackIdentity'

export class CredentialStoreError extends Schema.TaggedError<CredentialStoreError>()('CredentialStoreError', {
	operation: Schema.String,
	teamId: SlackTeamId,
}) {}

export class SlackWebhookError extends Schema.TaggedError<SlackWebhookError>()('SlackWebhookError', {
	reason: Schema.Literals(['missing_headers', 'invalid_timestamp', 'stale', 'invalid_signature', 'decode', 'crypto']),
}) {}

export class SlackTransportError extends Schema.TaggedError<SlackTransportError>()('SlackTransportError', {
	operation: Schema.String,
	status: Schema.optionalKey(Schema.Finite),
	retryAfterMs: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))),
}) {}

export class SlackApiError extends Schema.TaggedError<SlackApiError>()('SlackApiError', {
	operation: Schema.String,
	code: Schema.String,
}) {}

export class InvalidSlackThreadId extends Schema.TaggedError<InvalidSlackThreadId>()('InvalidSlackThreadId', {
	threadId: Schema.String,
}) {}

const transientSlackApiCodes = new Set(['ratelimited', 'internal_error', 'fatal_error', 'service_unavailable'])

/** Classifies Slack transport and API failures without leaking provider codes into core coordination. */
export const slackErrorRetryability = (error: SlackTransportError | SlackApiError): Retryability =>
	Match.value(error).pipe(
		Match.tagsExhaustive({
			SlackTransportError: () => 'retryable' as const,
			SlackApiError: ({ code }) =>
				transientSlackApiCodes.has(code) ? ('retryable' as const) : ('non_retryable' as const),
		}),
	)
