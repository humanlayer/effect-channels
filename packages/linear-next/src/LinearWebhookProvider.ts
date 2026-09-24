import {
	DeliveryAdmission,
	ProviderWebhookEvent,
	ProviderWebhookIgnored,
	ProviderWebhookResponse,
	WebhookAuthenticationError,
	WebhookPayloadInvalidError,
	type WebhookProvider,
} from '@humanlayer/channels-delivery-next'
import { Crypto, Effect, Predicate, Redacted, Schema } from 'effect'

import {
	linearAgentSessionResourceId,
	linearIssueResourceId,
	type LinearOrganizationId,
	type LinearUserId,
} from './LinearIdentity'
import { normalizeLinearAgentSessionWebhook, normalizeLinearAppUserNotificationWebhook } from './LinearWebhookParsers'
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

const isRecord = (value: unknown): value is Readonly<Record<string, unknown>> =>
	Predicate.isObject(value) && !Array.isArray(value)

const logInvalidHeaders = (input: Parameters<WebhookProvider<Crypto.Crypto>['handle']>[0]) =>
	Effect.logWarning('Linear webhook headers did not match the required structure').pipe(
		Effect.annotateLogs({
			provider: 'linear',
			decode_stage: 'headers',
			body_byte_length: input.body.byteLength,
			has_linear_delivery: 'linear-delivery' in input.headers,
			has_linear_event: 'linear-event' in input.headers,
			has_linear_signature: 'linear-signature' in input.headers,
			has_linear_timestamp: 'linear-timestamp' in input.headers,
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

const logInvalidEnvelope = (deliveryId: string, value: unknown) => {
	const root = isRecord(value) ? value : undefined
	return Effect.logWarning('Linear webhook body did not match the common event envelope').pipe(
		Effect.annotateLogs({
			provider: 'linear',
			delivery_id: deliveryId,
			decode_stage: 'event_envelope',
			json_root_kind: Predicate.isNull(value) ? 'null' : Array.isArray(value) ? 'array' : typeof value,
			has_type: Predicate.isNotUndefined(root) && 'type' in root,
			type_is_string: Predicate.isString(root?.type),
			has_action: Predicate.isNotUndefined(root) && 'action' in root,
			action_is_string: Predicate.isString(root?.action),
			has_organization_id: Predicate.isNotUndefined(root) && 'organizationId' in root,
			organization_id_is_string: Predicate.isString(root?.organizationId),
		}),
	)
}

const logInvalidSupportedShape = (input: {
	readonly deliveryId: string
	readonly envelope: typeof LinearWebhookEnvelope.Type
	readonly payload: unknown
}) => {
	const root = isRecord(input.payload) ? input.payload : {}
	const notification = isRecord(root.notification) ? root.notification : {}
	const agentSession = isRecord(root.agentSession) ? root.agentSession : {}
	const agentActivity = isRecord(root.agentActivity) ? root.agentActivity : {}
	const issue = isRecord(notification.issue) ? notification.issue : {}
	const comment = isRecord(notification.comment) ? notification.comment : {}
	return Effect.logWarning('Linear supported webhook payload did not match its schema').pipe(
		Effect.annotateLogs({
			provider: 'linear',
			delivery_id: input.deliveryId,
			event_type: input.envelope.type,
			action: input.envelope.action,
			has_webhook_id: Predicate.isString(root.webhookId),
			has_webhook_timestamp: Predicate.isNumber(root.webhookTimestamp),
			notification_type: Predicate.isString(notification.type) ? notification.type : 'missing',
			notification_has_issue: isRecord(notification.issue),
			notification_has_comment: isRecord(notification.comment),
			issue_has_team: isRecord(issue.team),
			comment_has_issue_id: Predicate.isString(comment.issueId),
			has_agent_session: isRecord(root.agentSession),
			agent_session_has_issue: isRecord(agentSession.issue),
			has_agent_activity: isRecord(root.agentActivity),
			agent_activity_type:
				isRecord(agentActivity.content) && Predicate.isString(agentActivity.content.type)
					? agentActivity.content.type
					: 'missing',
		}),
	)
}

export const makeLinearWebhookProvider = (options: LinearWebhookProviderOptions): WebhookProvider<Crypto.Crypto> => ({
	providerName: 'linear',
	maxBodyBytes: options.maxBodyBytes ?? 1024 * 1024,
	handle: (input) =>
		Effect.gen(function* () {
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

			const unknownPayload = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown))(
				new TextDecoder().decode(input.body),
			).pipe(
				Effect.tapError(() => logInvalidJson(headers['linear-delivery'], input.body.byteLength)),
				Effect.mapError(() => WebhookPayloadInvalidError.make({ reason: 'invalid_json' })),
			)
			const envelope = yield* Schema.decodeUnknownEffect(LinearWebhookEnvelope)(unknownPayload).pipe(
				Effect.tapError(() => logInvalidEnvelope(headers['linear-delivery'], unknownPayload)),
				Effect.mapError(() => WebhookPayloadInvalidError.make({ reason: 'invalid_event_envelope' })),
			)
			if (headers['linear-event'] !== envelope.type)
				return yield* WebhookPayloadInvalidError.make({ reason: 'event_header_mismatch' })
			if (envelope.organizationId !== options.organizationId)
				return ProviderWebhookResponse.make({ status: 403, body: null, headers: {} })
			const supported =
				(envelope.type === 'Issue' && envelope.action === 'create') ||
				(envelope.type === 'AppUserNotification' &&
					[
						'issueMention',
						'issueCommentMention',
						'issueAssignedToYou',
						'issueUnassignedFromYou',
						'issueNewComment',
						'issueStatusChanged',
						'issueEmojiReaction',
						'issueCommentReaction',
					].includes(envelope.action)) ||
				(envelope.type === 'AgentSessionEvent' && ['created', 'prompted'].includes(envelope.action))
			if (!supported) return ProviderWebhookIgnored.make({})

			const webhook = yield* Schema.decodeUnknownEffect(LinearSupportedWebhook)(unknownPayload, {
				onExcessProperty: 'preserve',
			}).pipe(
				Effect.tapError(() =>
					logInvalidSupportedShape({
						deliveryId: headers['linear-delivery'],
						envelope,
						payload: unknownPayload,
					}),
				),
				Effect.mapError(() => WebhookPayloadInvalidError.make({ reason: 'invalid_supported_event' })),
			)
			if (
				(webhook.type === 'AppUserNotification' || webhook.type === 'AgentSessionEvent') &&
				(webhook.appUserId !== options.appUserId ||
					(Predicate.isNotUndefined(options.oauthClientId) &&
						webhook.oauthClientId !== options.oauthClientId))
			)
				return ProviderWebhookResponse.make({ status: 403, body: null, headers: {} })
			if (webhook.type === 'AgentSessionEvent') {
				const normalized = yield* normalizeLinearAgentSessionWebhook(webhook).pipe(
					Effect.mapError(() => WebhookPayloadInvalidError.make({ reason: 'session_identity_mismatch' })),
				)
				const session = normalized.webhook.agentSession
				const eventId =
					normalized.action === 'created'
						? `agent-session-created:${session.id}`
						: `agent-session-prompted:${normalized.agentActivity.id}`
				return ProviderWebhookEvent.make({
					event: DeliveryAdmission.make({
						namespace: options.namespace,
						provider: 'linear',
						installationId: webhook.organizationId,
						resourceId: linearAgentSessionResourceId(session.id),
						eventId,
						payload: LinearStoredAgentSessionWebhook.make({
							deliveryId: headers['linear-delivery'],
							webhook: normalized.webhook,
						}),
					}),
				})
			}
			if (webhook.type === 'AppUserNotification') {
				const notification = yield* normalizeLinearAppUserNotificationWebhook(webhook).pipe(
					Effect.mapError(() =>
						WebhookPayloadInvalidError.make({ reason: 'notification_identity_mismatch' }),
					),
				)
				return ProviderWebhookEvent.make({
					event: DeliveryAdmission.make({
						namespace: options.namespace,
						provider: 'linear',
						installationId: notification.organizationId,
						resourceId: linearIssueResourceId(notification.notification.issueId),
						eventId: headers['linear-delivery'],
						payload: notification,
					}),
				})
			}
			return ProviderWebhookEvent.make({
				event: DeliveryAdmission.make({
					namespace: options.namespace,
					provider: 'linear',
					installationId: webhook.organizationId,
					resourceId: linearIssueResourceId(webhook.data.id),
					eventId: headers['linear-delivery'],
					payload: webhook,
				}),
			})
		}),
})
