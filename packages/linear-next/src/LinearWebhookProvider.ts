import {
	DeliveryAdmission,
	ProviderWebhookEvent,
	ProviderWebhookIgnored,
	ProviderWebhookResponse,
	WebhookAuthenticationError,
	WebhookPayloadInvalidError,
	type WebhookProvider,
} from '@humanlayer/channels-delivery-next'
import { Crypto, Effect, Redacted, Schema } from 'effect'

import { linearIssueResourceId, type LinearOrganizationId } from './LinearIdentity'
import { LinearIssueCreateWebhook } from './LinearWebhookEventSchemas'
import { LinearWebhookEnvelope, LinearWebhookHeaders } from './LinearWebhookSchemas'
import { verifyLinearWebhookSignature } from './LinearWebhookSignature'

export type LinearWebhookProviderOptions = {
	readonly namespace: string
	readonly webhookSecret: Redacted.Redacted<string>
	readonly organizationId: LinearOrganizationId
	readonly maxBodyBytes?: number
	readonly maxTimestampAgeMs?: number
}

export const makeLinearWebhookProvider = (
	options: LinearWebhookProviderOptions,
): WebhookProvider<Crypto.Crypto> => ({
	providerName: 'linear',
	maxBodyBytes: options.maxBodyBytes ?? 1024 * 1024,
	handle: (input) =>
		Effect.gen(function* () {
			const headers = yield* Schema.decodeUnknownEffect(LinearWebhookHeaders)(input.headers).pipe(
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
			).pipe(Effect.mapError(() => WebhookPayloadInvalidError.make({ reason: 'invalid_json' })))
			const envelope = yield* Schema.decodeUnknownEffect(LinearWebhookEnvelope)(unknownPayload).pipe(
				Effect.mapError(() => WebhookPayloadInvalidError.make({ reason: 'invalid_event_envelope' })),
			)
			if (headers['linear-event'] !== envelope.type)
				return yield* WebhookPayloadInvalidError.make({ reason: 'event_header_mismatch' })
			if (envelope.organizationId !== options.organizationId)
				return ProviderWebhookResponse.make({ status: 403, body: null, headers: {} })
			if (envelope.type !== 'Issue' || envelope.action !== 'create') return ProviderWebhookIgnored.make({})

			const webhook = yield* Schema.decodeUnknownEffect(LinearIssueCreateWebhook)(unknownPayload, {
				onExcessProperty: 'preserve',
			}).pipe(Effect.mapError(() => WebhookPayloadInvalidError.make({ reason: 'invalid_issue_create' })))
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
