import {
	ProviderWebhookIgnored,
	WebhookAuthenticationError,
	type ProviderWebhookError,
	type ProviderWebhookOutcome,
	type RawWebhookInput,
	type WebhookProvider,
} from '@humanlayer/channels-delivery-next'
import { Crypto, Effect, Redacted, Schema } from 'effect'

import { SlackWebhookHeaders } from './SlackWebhookSchemas'
import { verifySlackSignature } from './SlackWebhookSignature'

export type SlackWebhookProviderOptions = {
	readonly namespace: string
	readonly signingSecret: Redacted.Redacted<string>
}

/**
 * The slack webhook handler effect
 */
const slackWebhookHandler =
	(options: SlackWebhookProviderOptions) =>
	(input: RawWebhookInput): Effect.Effect<ProviderWebhookOutcome, ProviderWebhookError, Crypto.Crypto> =>
		Effect.gen(function* () {
			const headers = yield* Schema.decodeUnknownEffect(SlackWebhookHeaders)(input.headers).pipe(
				Effect.mapError(() => WebhookAuthenticationError.make({ reason: 'invalid_signature_headers' })),
			)

			yield* verifySlackSignature({
				body: input.body,
				timestamp: headers['x-slack-request-timestamp'],
				signature: headers['x-slack-signature'],
				signingSecret: options.signingSecret,
			})

			// TODO 1. Parse the verified body as JSON using the top-level Slack webhook envelope schema.
			// TODO 2. Decode URL-verification payloads and return the challenge as ProviderWebhookResponse.
			// TODO 3. Decode event-callback metadata so team_id, event_id, and event.type are known.
			// TODO 4. Return ProviderWebhookIgnored for valid Slack event types that are not supported yet.
			// TODO 5. Decode supported event types with their complete provider-specific schemas.
			// TODO 6. Derive installationId, resourceId, and eventId and return a DeliveryAdmission.
			return ProviderWebhookIgnored.make({})
		})

export const makeSlackWebhookProvider = (options: SlackWebhookProviderOptions): WebhookProvider<Crypto.Crypto> => ({
	key: 'slack',
	handle: slackWebhookHandler(options),
})
