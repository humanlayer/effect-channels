import {
	DeliveryAdmission,
	ProviderWebhookEvent,
	ProviderWebhookIgnored,
	ProviderWebhookResponse,
	WebhookAuthenticationError,
	WebhookPayloadInvalidError,
	type RawWebhookInput,
	type WebhookProvider,
} from '@humanlayer/channels-delivery'
import {
	Array as Arr,
	Crypto,
	Effect,
	JsonPointer,
	Match,
	Predicate,
	Redacted,
	Schema,
	SchemaIssue,
	type StandardSchema,
} from 'effect'
import * as Headers from 'effect/http/Headers'

import {
	linearAgentSessionResourceId,
	linearInstallationResourceId,
	linearIssueResourceId,
	type LinearOrganizationId,
	type LinearUserId,
	type LinearWebhookDeliveryId,
} from './LinearIdentity'
import {
	normalizeLinearAgentSessionWebhook,
	normalizeLinearAppUserNotificationWebhook,
	resourceIssueId,
} from './LinearWebhookParsers'
import {
	LinearStoredAgentSessionWebhook,
	LinearSupportedWebhook,
	LinearWebhookEnvelope,
	LinearWebhookHeaders,
} from './LinearWebhookSchemas'
import { verifyLinearWebhookSignature } from './LinearWebhookSignature'

export type LinearWebhookProviderOptions = {
	readonly namespace: string
	readonly webhookSecret: Redacted.Redacted<string>
	readonly organizationId: LinearOrganizationId
	readonly appUserId: LinearUserId
	readonly oauthClientId?: string
	readonly maxBodyBytes?: number
	readonly maxTimestampAgeMs?: number
}

const LinearWebhookEventType = Schema.Literals([
	'Issue',
	'Comment',
	'Reaction',
	'Attachment',
	'PermissionChange',
	'OAuthApp',
	'AppUserNotification',
	'AgentSessionEvent',
])
type LinearWebhookEventType = typeof LinearWebhookEventType.Type

const isLinearWebhookEventType = Schema.is(LinearWebhookEventType)

const supportedActions = {
	Issue: ['create', 'update', 'remove'],
	Comment: ['create', 'update', 'remove'],
	Reaction: ['create', 'remove'],
	Attachment: ['create', 'update', 'remove'],
	PermissionChange: ['teamAccessChanged'],
	OAuthApp: ['revoked'],
	AppUserNotification: [
		'issueMention',
		'issueCommentMention',
		'issueAssignedToYou',
		'issueUnassignedFromYou',
		'issueNewComment',
		'issueStatusChanged',
		'issueEmojiReaction',
		'issueCommentReaction',
	],
	AgentSessionEvent: ['created', 'prompted'],
} satisfies Record<LinearWebhookEventType, ReadonlyArray<string>>

/**
 * Linear's prompt signal asking the agent to stop. A stop prompt asks the session's current delivery to end;
 * the prompt itself still queues behind it.
 */
const LinearStopSignal = Schema.Literal('stop')
const isStopSignal = Schema.is(LinearStopSignal)

const isSupportedAction = (envelope: typeof LinearWebhookEnvelope.Type) =>
	isLinearWebhookEventType(envelope.type) && supportedActions[envelope.type].includes(envelope.action)

const forbidden = () => ProviderWebhookResponse.make({ status: 403, body: null, headers: {} })

const logInvalidHeaders = (input: RawWebhookInput) =>
	Effect.logWarning('Linear webhook headers did not match the required structure').pipe(
		Effect.annotateLogs({
			provider: 'linear',
			decode_stage: 'headers',
			body_byte_length: input.body.byteLength,
			has_linear_delivery: Headers.has(input.headers, 'linear-delivery'),
			has_linear_event: Headers.has(input.headers, 'linear-event'),
			has_linear_signature: Headers.has(input.headers, 'linear-signature'),
			has_linear_timestamp: Headers.has(input.headers, 'linear-timestamp'),
		}),
	)

const logInvalidJson = (deliveryId: string, bodyByteLength: number) =>
	Effect.logWarning('Linear webhook body was not valid JSON').pipe(
		Effect.annotateLogs({
			provider: 'linear',
			delivery_id: deliveryId,
			decode_stage: 'json',
			body_byte_length: bodyByteLength,
		}),
	)

const formatIssues = SchemaIssue.makeFormatterStandardSchemaV1()

const issuePointer = ({ path = [] }: StandardSchema.StandardSchemaV1.Issue) =>
	path
		.map((segment) => (Predicate.isPropertyKey(segment) ? segment : segment.key))
		.map((key) => `/${JsonPointer.escapeToken(String(key))}`)
		.join('')

/** Logs where a webhook body failed its schema as JSON Pointers, never the values found there. */
const logWebhookDecodeFailure = (
	stage: 'event_envelope' | 'supported_event',
	deliveryId: string,
	error: Schema.SchemaError,
) =>
	Effect.logWarning('Linear webhook body did not match its schema').pipe(
		Effect.annotateLogs({
			provider: 'linear',
			delivery_id: deliveryId,
			decode_stage: stage,
			issue_paths: Arr.dedupe(formatIssues(error.issue).issues.map(issuePointer)),
		}),
	)

const authenticateLinearWebhook = Effect.fn('linear.webhook.authenticate')(function* (
	options: LinearWebhookProviderOptions,
	input: RawWebhookInput,
) {
	const headers = yield* Schema.decodeUnknownEffect(LinearWebhookHeaders)(input.headers).pipe(
		Effect.tapError(() => logInvalidHeaders(input)),
		Effect.mapError(() => WebhookAuthenticationError.make({ reason: 'invalid_signature_headers' })),
	)
	yield* verifyLinearWebhookSignature({
		body: input.body,
		signature: headers['linear-signature'],
		timestamp: headers['linear-timestamp'],
		webhookSecret: options.webhookSecret,
		maxAgeMs: options.maxTimestampAgeMs ?? 60_000,
	})
	return headers
})

const decodeLinearWebhookEnvelope = Effect.fn('linear.webhook.decode_envelope')(function* (
	headers: typeof LinearWebhookHeaders.Type,
	body: Uint8Array,
) {
	const deliveryId = headers['linear-delivery']
	const payload = yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Json))(new TextDecoder().decode(body)).pipe(
		Effect.tapError(() => logInvalidJson(deliveryId, body.byteLength)),
		Effect.mapError(() => WebhookPayloadInvalidError.make({ reason: 'invalid_json' })),
	)
	const envelope = yield* Schema.decodeUnknownEffect(LinearWebhookEnvelope)(payload, { errors: 'all' }).pipe(
		Effect.tapError((error) => logWebhookDecodeFailure('event_envelope', deliveryId, error)),
		Effect.mapError(() => WebhookPayloadInvalidError.make({ reason: 'invalid_event_envelope' })),
	)
	if (headers['linear-event'] !== envelope.type)
		return yield* WebhookPayloadInvalidError.make({ reason: 'event_header_mismatch' })
	return { envelope, payload }
})

const decodeLinearWebhook = Effect.fn('linear.webhook.decode')(function* (deliveryId: string, payload: Schema.Json) {
	return yield* Schema.decodeUnknownEffect(LinearSupportedWebhook)(payload, {
		errors: 'all',
	}).pipe(
		Effect.tapError((error) => logWebhookDecodeFailure('supported_event', deliveryId, error)),
		Effect.mapError(() => WebhookPayloadInvalidError.make({ reason: 'invalid_supported_event' })),
	)
})

const oauthClientMatches = (options: LinearWebhookProviderOptions, oauthClientId: string) =>
	Predicate.isUndefined(options.oauthClientId) || oauthClientId === options.oauthClientId

const appIdentityMatches = (
	options: LinearWebhookProviderOptions,
	webhook: { readonly appUserId: string; readonly oauthClientId: string },
) => webhook.appUserId === options.appUserId && oauthClientMatches(options, webhook.oauthClientId)

const checkLinearWebhookIdentity = (options: LinearWebhookProviderOptions, webhook: LinearSupportedWebhook) =>
	Match.value(webhook).pipe(
		Match.discriminators('type')({
			AppUserNotification: (notification) => appIdentityMatches(options, notification),
			AgentSessionEvent: (session) => appIdentityMatches(options, session),
			PermissionChange: (permission) => appIdentityMatches(options, permission),
			OAuthApp: (installation) => oauthClientMatches(options, installation.oauthClientId),
		}),
		Match.orElse(() => true),
	)

const admitLinearWebhook = Effect.fn('linear.webhook.admit')(function* (
	options: LinearWebhookProviderOptions,
	deliveryId: LinearWebhookDeliveryId,
	webhook: LinearSupportedWebhook,
) {
	if (webhook.type === 'AgentSessionEvent') {
		const normalized = yield* normalizeLinearAgentSessionWebhook(webhook).pipe(
			Effect.mapError(() => WebhookPayloadInvalidError.make({ reason: 'session_identity_mismatch' })),
		)
		const session = normalized.webhook.agentSession
		const admission = {
			namespace: options.namespace,
			provider: 'linear',
			installationId: webhook.organizationId,
			resourceId: linearAgentSessionResourceId(session.id),
			payload: LinearStoredAgentSessionWebhook.make({ deliveryId, webhook: normalized.webhook }),
		}
		return Match.value(normalized).pipe(
			Match.discriminatorsExhaustive('action')({
				created: () =>
					ProviderWebhookEvent.make({
						event: DeliveryAdmission.make({ ...admission, eventId: `agent-session-created:${session.id}` }),
					}),
				prompted: ({ agentActivity }) => {
					const eventId = `agent-session-prompted:${agentActivity.id}`
					return ProviderWebhookEvent.make({
						event: isStopSignal(agentActivity.signal)
							? DeliveryAdmission.make({ ...admission, eventId, interrupt: true })
							: DeliveryAdmission.make({ ...admission, eventId }),
					})
				},
			}),
		)
	}
	if (webhook.type === 'AppUserNotification') {
		const notification = yield* normalizeLinearAppUserNotificationWebhook(webhook).pipe(
			Effect.mapError(() => WebhookPayloadInvalidError.make({ reason: 'notification_identity_mismatch' })),
		)
		return ProviderWebhookEvent.make({
			event: DeliveryAdmission.make({
				namespace: options.namespace,
				provider: 'linear',
				installationId: notification.organizationId,
				resourceId: linearIssueResourceId(notification.notification.issueId),
				eventId: deliveryId,
				payload: notification,
			}),
		})
	}
	if (webhook.type === 'OAuthApp' || webhook.type === 'PermissionChange')
		return ProviderWebhookEvent.make({
			event: DeliveryAdmission.make({
				namespace: options.namespace,
				provider: 'linear',
				installationId: webhook.organizationId,
				resourceId: linearInstallationResourceId(),
				eventId: deliveryId,
				payload: webhook,
			}),
		})
	const issueId = resourceIssueId(webhook)
	if (Predicate.isUndefined(issueId))
		return yield* WebhookPayloadInvalidError.make({ reason: 'resource_issue_identity_missing' })
	return ProviderWebhookEvent.make({
		event: DeliveryAdmission.make({
			namespace: options.namespace,
			provider: 'linear',
			installationId: webhook.organizationId,
			resourceId: linearIssueResourceId(issueId),
			eventId: deliveryId,
			payload: webhook,
		}),
	})
})

export const makeLinearWebhookProvider = (options: LinearWebhookProviderOptions): WebhookProvider<Crypto.Crypto> => ({
	providerName: 'linear',
	maxBodyBytes: options.maxBodyBytes ?? 1024 * 1024,
	handle: (input) =>
		Effect.gen(function* () {
			const headers = yield* authenticateLinearWebhook(options, input)
			const deliveryId = headers['linear-delivery']
			const { envelope, payload } = yield* decodeLinearWebhookEnvelope(headers, input.body)
			if (envelope.organizationId !== options.organizationId) return forbidden()
			if (!isSupportedAction(envelope)) return ProviderWebhookIgnored.make({})
			const webhook = yield* decodeLinearWebhook(deliveryId, payload)
			if (!checkLinearWebhookIdentity(options, webhook)) return forbidden()
			return yield* admitLinearWebhook(options, deliveryId, webhook)
		}),
})
