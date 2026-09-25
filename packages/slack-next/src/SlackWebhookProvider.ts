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

import { SlackApi } from './SlackApi'
import { slackThreadResourceId } from './SlackIdentity'
import { SlackMessageRef } from './SlackModels'
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

const slackWebhookHandler =
	(options: SlackWebhookProviderOptions) =>
	(input: RawWebhookInput): Effect.Effect<ProviderWebhookOutcome, ProviderWebhookError, Crypto.Crypto | SlackApi> =>
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

			const bodyText = new TextDecoder().decode(input.body)
			const jsonBody = yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Json))(bodyText).pipe(
				Effect.mapError(() => WebhookPayloadInvalidError.make({ reason: 'invalid_json' })),
			)
			const envelope = yield* Schema.decodeUnknownEffect(SlackWebhookEnvelope)(jsonBody).pipe(
				Effect.mapError(() => WebhookPayloadInvalidError.make({ reason: 'invalid_envelope' })),
			)

			return yield* Match.value(envelope.type).pipe(
				Match.when('url_verification', () => handleUrlVerification(jsonBody)),
				Match.when('event_callback', () => handleSlackEvent(options, jsonBody)),
				Match.orElse(() => Effect.succeed(ProviderWebhookIgnored.make({}))),
			)
		})

/** Verifies Slack Events API requests from their exact bytes and admits supported events to thread mailboxes. */
export const makeSlackWebhookProvider = (
	options: SlackWebhookProviderOptions,
): WebhookProvider<Crypto.Crypto | SlackApi> => ({
	providerName: 'slack',
	handle: slackWebhookHandler(options),
})

const handleUrlVerification = (
	verification: Schema.Json,
): Effect.Effect<ProviderWebhookResponse, WebhookPayloadInvalidError> =>
	Schema.decodeUnknownEffect(SlackUrlVerification)(verification).pipe(
		Effect.map((payload) =>
			ProviderWebhookResponse.make({
				status: 200,
				body: new TextEncoder().encode(payload.challenge),
				headers: { 'content-type': 'text/plain; charset=utf-8' },
			}),
		),
		Effect.mapError(() => WebhookPayloadInvalidError.make({ reason: 'invalid_url_verification' })),
	)

const handleSlackEvent = (
	options: SlackWebhookProviderOptions,
	eventEnvelope: Schema.Json,
): Effect.Effect<ProviderWebhookOutcome, WebhookPayloadInvalidError, SlackApi> =>
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
	eventEnvelope: Schema.Json,
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
	eventEnvelope: Schema.Json,
): Effect.Effect<ProviderWebhookOutcome, WebhookPayloadInvalidError> =>
	Schema.decodeUnknownEffect(SlackMessageEnvelope)(eventEnvelope, { onExcessProperty: 'preserve' }).pipe(
		Effect.mapError(() => WebhookPayloadInvalidError.make({ reason: 'invalid_message' })),
		Effect.flatMap((envelope) =>
			Match.value(envelope.event.subtype).pipe(
				Match.whenOr(undefined, 'file_share', () =>
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
	eventEnvelope: Schema.Json,
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
	eventEnvelope: Schema.Json,
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

const handleAppMention = (
	options: SlackWebhookProviderOptions,
	eventEnvelope: Schema.Json,
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

const handleReactionAdded = (options: SlackWebhookProviderOptions, eventEnvelope: Schema.Json) =>
	Schema.decodeUnknownEffect(SlackReactionAddedEnvelope)(eventEnvelope, { onExcessProperty: 'preserve' }).pipe(
		Effect.mapError(() => WebhookPayloadInvalidError.make({ reason: 'invalid_reaction_added' })),
		Effect.flatMap((envelope) => admitReaction(options, envelope)),
	)

const handleReactionRemoved = (options: SlackWebhookProviderOptions, eventEnvelope: Schema.Json) =>
	Schema.decodeUnknownEffect(SlackReactionRemovedEnvelope)(eventEnvelope, { onExcessProperty: 'preserve' }).pipe(
		Effect.mapError(() => WebhookPayloadInvalidError.make({ reason: 'invalid_reaction_removed' })),
		Effect.flatMap((envelope) => admitReaction(options, envelope)),
	)

const admitReaction = (
	options: SlackWebhookProviderOptions,
	envelope: SlackReactionAddedEnvelopeType | SlackReactionRemovedEnvelopeType,
): Effect.Effect<ProviderWebhookOutcome, never, SlackApi> =>
	Effect.gen(function* () {
		const api = yield* SlackApi
		const threadTs = yield* api
			.resolveReactionThread({
				message: SlackMessageRef.make({
					teamId: envelope.team_id,
					channelId: envelope.event.item.channel,
					messageTs: envelope.event.item.ts,
				}),
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
				Effect.catchTag('SlackApiError', () => Effect.succeed(envelope.event.item.ts)),
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
