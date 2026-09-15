import {
	DeliveryAdmission,
	ProviderWebhookEvent,
	ProviderWebhookIgnored,
	ProviderWebhookResponse,
	WebhookAuthenticationError,
	WebhookPayloadInvalidError,
	type ProviderWebhookError,
	type ProviderWebhookOutcome,
	type RawWebhookInput,
	type WebhookProvider,
} from '@humanlayer/channels-delivery-next'
import { Crypto, Effect, Match, Redacted, Schema } from 'effect'

import { slackThreadResourceId } from './SlackIdentity'
import { SlackReactionThreadResolver } from './SlackReactionThreadResolver'
import {
	SlackAgentSessionStoppedEnvelope,
	SlackAppMentionEnvelope,
	SlackEventEnvelope,
	SlackMessageDeletedEnvelope,
	SlackMessageEnvelope,
	SlackMessageUpdatedEnvelope,
	SlackReactionAddedEnvelope,
	type SlackReactionAddedEnvelope as SlackReactionAddedEnvelopeType,
	SlackReactionRemovedEnvelope,
	type SlackReactionRemovedEnvelope as SlackReactionRemovedEnvelopeType,
	SlackUrlVerification,
	SlackWebhookEnvelope,
	SlackWebhookHeaders,
} from './SlackWebhookSchemas'
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
	(
		input: RawWebhookInput,
	): Effect.Effect<ProviderWebhookOutcome, ProviderWebhookError, Crypto.Crypto | SlackReactionThreadResolver> =>
		Effect.gen(function* () {
			// make sure that the webhook headers include the necessary fields for signature authentication
			const headers = yield* Schema.decodeUnknownEffect(SlackWebhookHeaders)(input.headers).pipe(
				Effect.mapError(() => WebhookAuthenticationError.make({ reason: 'invalid_signature_headers' })),
			)

			// Authenticate the request
			yield* verifySlackSignature({
				body: input.body,
				timestamp: headers['x-slack-request-timestamp'],
				signature: headers['x-slack-signature'],
				signingSecret: options.signingSecret,
			})

			// Get the body text of the payload and parse to JSON of an unknown shape
			const bodyText = new TextDecoder().decode(input.body)
			const unknownJsonBody = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown))(
				bodyText,
			).pipe(Effect.mapError(() => WebhookPayloadInvalidError.make({ reason: 'invalid_json' })))

			// Decode URL-verification payloads and return the challenge as ProviderWebhookResponse.
			const envelope = yield* Schema.decodeUnknownEffect(SlackWebhookEnvelope)(unknownJsonBody).pipe(
				Effect.mapError(() => WebhookPayloadInvalidError.make({ reason: 'invalid_envelope' })),
			)

			// handle it appropriately based on what it is - if url_verification handle it
			// otherwise handle event callbacks
			return yield* Match.value(envelope.type).pipe(
				// handle URL verification response
				Match.when('url_verification', () => handleUrlVerification(unknownJsonBody)),
				// handle Slack event callbacks separately from top-level Slack protocol messages
				Match.when('event_callback', () => handleSlackEvent(options, unknownJsonBody)),
				Match.orElse(() => Effect.succeed(ProviderWebhookIgnored.make({}))),
			)
		})

// Slack Webhook provider constructor
export const makeSlackWebhookProvider = (
	options: SlackWebhookProviderOptions,
): WebhookProvider<Crypto.Crypto | SlackReactionThreadResolver> => ({
	providerName: 'slack',
	handle: slackWebhookHandler(options),
})

// handle URL verification by parsing the event and creating the response
const handleUrlVerification = (
	verification: unknown,
): Effect.Effect<ProviderWebhookResponse, WebhookPayloadInvalidError> =>
	Schema.decodeUnknownEffect(SlackUrlVerification)(verification)
		.pipe(
			Effect.map((payload) =>
				ProviderWebhookResponse.make({
					status: 200,
					body: new TextEncoder().encode(payload.challenge),
					headers: { 'content-type': 'text/plain; charset=utf-8' },
				}),
			),
		)
		.pipe(Effect.mapError(() => WebhookPayloadInvalidError.make({ reason: 'invalid_url_verification' })))

// handle Slack events by first parsing the shared event metadata, then matching the inner event type
const handleSlackEvent = (
	options: SlackWebhookProviderOptions,
	eventEnvelope: unknown,
): Effect.Effect<ProviderWebhookOutcome, WebhookPayloadInvalidError, SlackReactionThreadResolver> =>
	Schema.decodeUnknownEffect(SlackEventEnvelope)(eventEnvelope).pipe(
		Effect.mapError(() => WebhookPayloadInvalidError.make({ reason: 'invalid_event_envelope' })),
		Effect.flatMap((envelope) =>
			Match.value(envelope.event.type).pipe(
				Match.when('agent_session_stopped', () => handleAgentSessionStopped(options, eventEnvelope)),
				Match.when('app_mention', () => handleAppMention(options, eventEnvelope)),
				Match.when('message', () => handleMessage(options, eventEnvelope)),
				Match.when('reaction_added', () => handleReactionAdded(options, eventEnvelope)),
				Match.when('reaction_removed', () => handleReactionRemoved(options, eventEnvelope)),
				Match.orElse(() => Effect.succeed(ProviderWebhookIgnored.make({}))),
			),
		),
	)

const handleAgentSessionStopped = (
	options: SlackWebhookProviderOptions,
	eventEnvelope: unknown,
): Effect.Effect<ProviderWebhookOutcome, WebhookPayloadInvalidError> =>
	Schema.decodeUnknownEffect(SlackAgentSessionStoppedEnvelope)(eventEnvelope, {
		onExcessProperty: 'preserve',
	}).pipe(
		Effect.mapError(() => WebhookPayloadInvalidError.make({ reason: 'invalid_agent_session_stopped' })),
		Effect.map((envelope) =>
			makeSlackAdmissionOutcome({
				options,
				installationId: envelope.team_id,
				resourceId: slackThreadResourceId({
					teamId: envelope.team_id,
					channelId: envelope.event.channel,
					threadTs: envelope.event.thread_ts,
				}),
				eventId: envelope.event_id,
				payload: envelope,
			}),
		),
	)

const handleMessage = (
	options: SlackWebhookProviderOptions,
	eventEnvelope: unknown,
): Effect.Effect<ProviderWebhookOutcome, WebhookPayloadInvalidError> =>
	Schema.decodeUnknownEffect(SlackMessageEnvelope)(eventEnvelope, { onExcessProperty: 'preserve' }).pipe(
		Effect.mapError(() => WebhookPayloadInvalidError.make({ reason: 'invalid_message' })),
		Effect.flatMap((envelope) =>
			Match.value(envelope.event.subtype).pipe(
				Match.when(undefined, () =>
					Effect.succeed(
						makeSlackAdmissionOutcome({
							options,
							installationId: envelope.team_id,
							resourceId: slackThreadResourceId({
								teamId: envelope.team_id,
								channelId: envelope.event.channel,
								threadTs: envelope.event.thread_ts ?? envelope.event.ts,
							}),
							eventId: envelope.event_id,
							payload: envelope,
						}),
					),
				),
				Match.when('message_changed', () => handleMessageUpdated(options, eventEnvelope)),
				Match.when('message_deleted', () => handleMessageDeleted(options, eventEnvelope)),
				Match.orElse(() => Effect.succeed(ProviderWebhookIgnored.make({}))),
			),
		),
	)

const handleMessageUpdated = (
	options: SlackWebhookProviderOptions,
	eventEnvelope: unknown,
): Effect.Effect<ProviderWebhookOutcome, WebhookPayloadInvalidError> =>
	Schema.decodeUnknownEffect(SlackMessageUpdatedEnvelope)(eventEnvelope, { onExcessProperty: 'preserve' }).pipe(
		Effect.mapError(() => WebhookPayloadInvalidError.make({ reason: 'invalid_message_changed' })),
		Effect.map((envelope) =>
			makeSlackAdmissionOutcome({
				options,
				installationId: envelope.team_id,
				resourceId: slackThreadResourceId({
					teamId: envelope.team_id,
					channelId: envelope.event.channel,
					threadTs: envelope.event.message.thread_ts ?? envelope.event.message.ts,
				}),
				eventId: envelope.event_id,
				payload: envelope,
			}),
		),
	)

const handleMessageDeleted = (
	options: SlackWebhookProviderOptions,
	eventEnvelope: unknown,
): Effect.Effect<ProviderWebhookOutcome, WebhookPayloadInvalidError> =>
	Schema.decodeUnknownEffect(SlackMessageDeletedEnvelope)(eventEnvelope, { onExcessProperty: 'preserve' }).pipe(
		Effect.mapError(() => WebhookPayloadInvalidError.make({ reason: 'invalid_message_deleted' })),
		Effect.map((envelope) => {
			const deletedMessageTs =
				envelope.event.deleted_ts ?? envelope.event.previous_message?.ts ?? envelope.event.ts
			return makeSlackAdmissionOutcome({
				options,
				installationId: envelope.team_id,
				resourceId: slackThreadResourceId({
					teamId: envelope.team_id,
					channelId: envelope.event.channel,
					threadTs: envelope.event.previous_message?.thread_ts ?? deletedMessageTs,
				}),
				eventId: envelope.event_id,
				payload: envelope,
			})
		}),
	)

const makeSlackAdmissionOutcome = (input: {
	readonly options: SlackWebhookProviderOptions
	readonly installationId: string
	readonly resourceId: string
	readonly eventId: string
	readonly payload: DeliveryAdmission['payload']
}): ProviderWebhookOutcome =>
	ProviderWebhookEvent.make({
		event: DeliveryAdmission.make({
			namespace: input.options.namespace,
			provider: 'slack',
			installationId: input.installationId,
			resourceId: input.resourceId,
			eventId: input.eventId,
			payload: input.payload,
		}),
	})

// parse one complete app-mention envelope and prepare it for durable admission
const handleAppMention = (
	options: SlackWebhookProviderOptions,
	eventEnvelope: unknown,
): Effect.Effect<ProviderWebhookOutcome, WebhookPayloadInvalidError> =>
	Schema.decodeUnknownEffect(SlackAppMentionEnvelope)(eventEnvelope, {
		onExcessProperty: 'preserve',
	}).pipe(
		Effect.mapError(() => WebhookPayloadInvalidError.make({ reason: 'invalid_app_mention' })),
		Effect.map((envelope) =>
			makeSlackAdmissionOutcome({
				options,
				installationId: envelope.team_id,
				resourceId: slackThreadResourceId({
					teamId: envelope.team_id,
					channelId: envelope.event.channel,
					threadTs: envelope.event.thread_ts ?? envelope.event.ts,
				}),
				eventId: envelope.event_id,
				payload: envelope,
			}),
		),
	)

const handleReactionAdded = (options: SlackWebhookProviderOptions, eventEnvelope: unknown) =>
	Schema.decodeUnknownEffect(SlackReactionAddedEnvelope)(eventEnvelope, { onExcessProperty: 'preserve' }).pipe(
		Effect.mapError(() => WebhookPayloadInvalidError.make({ reason: 'invalid_reaction_added' })),
		Effect.flatMap((envelope) => admitReaction(options, envelope)),
	)

const handleReactionRemoved = (options: SlackWebhookProviderOptions, eventEnvelope: unknown) =>
	Schema.decodeUnknownEffect(SlackReactionRemovedEnvelope)(eventEnvelope, { onExcessProperty: 'preserve' }).pipe(
		Effect.mapError(() => WebhookPayloadInvalidError.make({ reason: 'invalid_reaction_removed' })),
		Effect.flatMap((envelope) => admitReaction(options, envelope)),
	)

const admitReaction = (
	options: SlackWebhookProviderOptions,
	envelope: SlackReactionAddedEnvelopeType | SlackReactionRemovedEnvelopeType,
): Effect.Effect<ProviderWebhookOutcome, never, SlackReactionThreadResolver> =>
	Effect.gen(function* () {
		const resolver = yield* SlackReactionThreadResolver
		const threadTs = yield* resolver
			.resolve({
				teamId: envelope.team_id,
				channelId: envelope.event.item.channel,
				messageTs: envelope.event.item.ts,
			})
			.pipe(
				Effect.tapError((error) =>
					Effect.logWarning(
						'Slack reaction thread lookup failed; using reacted message timestamp',
						error,
					).pipe(
						Effect.annotateLogs({
							provider: 'slack',
							installation_id: envelope.team_id,
							channel_id: envelope.event.item.channel,
						}),
					),
				),
				Effect.catchTag('SlackReactionThreadResolutionUnavailable', () =>
					Effect.succeed(envelope.event.item.ts),
				),
			)

		return makeSlackAdmissionOutcome({
			options,
			installationId: envelope.team_id,
			resourceId: slackThreadResourceId({
				teamId: envelope.team_id,
				channelId: envelope.event.item.channel,
				threadTs,
			}),
			eventId: envelope.event_id,
			payload: envelope,
		})
	})
