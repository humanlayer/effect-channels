import { Ingress } from '@humanlayer/channels'
import { Config, Effect, Layer, Match, Option, Predicate, Schema } from 'effect'
import type { Redacted } from 'effect'
import { HttpRouter, HttpServerResponse } from 'effect/unstable/http'

import { SlackWebhookError } from './Errors.ts'
import type { SlackBotIdentity, SlackEventCallback } from './Schema.ts'
import { SlackEventsRequest, SlackMessageTs } from './Schema.ts'
import { mergeSlackBotIdentity, slackBotIdentity } from './SlackBotIdentity.ts'
import { SlackClient } from './SlackClient.ts'
import {
	normalizeSlackConversationStopped,
	normalizeSlackMessage,
	normalizeSlackMessageDeleted,
	normalizeSlackMessageUpdated,
	normalizeSlackReaction,
} from './SlackNormalize.ts'
import { verifySlackSignature } from './SlackSignature.ts'
import { SlackTenantCredentials } from './SlackTenantCredentials.ts'

type SlackRoutesConfig = {
	readonly signingSecret: Redacted.Redacted<string>
	readonly identity: SlackBotIdentity
	readonly credentials: SlackTenantCredentials['Service']
	readonly client: SlackClient['Service']
}

const webhookErrorResponse = (error: SlackWebhookError) => {
	const status = Match.value(error.reason).pipe(
		Match.when('decode', () => 400),
		Match.when('crypto', () => 500),
		Match.when('missing_headers', () => 401),
		Match.when('invalid_timestamp', () => 401),
		Match.when('stale', () => 401),
		Match.when('invalid_signature', () => 401),
		Match.exhaustive,
	)
	return Effect.succeed(HttpServerResponse.empty({ status }))
}

const resolveBotIdentity = (config: SlackRoutesConfig, callback: SlackEventCallback) =>
	config.credentials.load({ teamId: callback.team_id }).pipe(
		Effect.map((creds) => mergeSlackBotIdentity(config.identity, creds)),
		Effect.tapError((error) =>
			Effect.logWarning('Slack tenant identity lookup failed, using the configured bot identity', error).pipe(
				Effect.annotateLogs({ provider: 'slack', tenant: callback.team_id, event_id: callback.event_id }),
			),
		),
		Effect.catchTag('CredentialStoreError', () => Effect.succeed(config.identity)),
	)

const acceptMessageEvent = (config: SlackRoutesConfig, callback: SlackEventCallback) =>
	Effect.gen(function* () {
		const ingress = yield* Ingress
		const identity = yield* resolveBotIdentity(config, callback)
		const normalized = yield* normalizeSlackMessage({ callback, identity })
		if (Option.isNone(normalized)) {
			yield* Effect.logInfo('acknowledging and dropping ineligible Slack message subtype').pipe(
				Effect.annotateLogs({
					provider: 'slack',
					event_type: callback.event.type,
					event_subtype: 'subtype' in callback.event ? callback.event.subtype : undefined,
					event_id: callback.event_id,
				}),
			)
			return HttpServerResponse.empty({ status: 200 })
		}
		yield* ingress.acceptMessage(normalized.value)
		return HttpServerResponse.empty({ status: 200 })
	})

const acceptLifecycleEvent = (config: SlackRoutesConfig, callback: SlackEventCallback) =>
	Effect.gen(function* () {
		const ingress = yield* Ingress
		const identity = yield* resolveBotIdentity(config, callback)
		const event = callback.event
		if (event.type === 'reaction_added' || event.type === 'reaction_removed') {
			const parentThreadTs = yield* config.client
				.replies({
					teamId: callback.team_id,
					channelId: event.item.channel,
					threadTs: event.item.ts,
					limit: 1,
				})
				.pipe(
					Effect.map((page) => {
						const raw = page.messages[0]?.raw
						return Predicate.hasProperty(raw, 'thread_ts') && Predicate.isString(raw.thread_ts)
							? SlackMessageTs.make(raw.thread_ts)
							: event.item.ts
					}),
					Effect.catch((error) =>
						Effect.logWarning(
							'Slack reaction parent lookup failed; using reacted message as thread root',
							error,
						).pipe(Effect.as(event.item.ts)),
					),
				)
			yield* ingress.acceptReaction(yield* normalizeSlackReaction({ callback, identity, parentThreadTs }))
		} else if (event.type === 'message' && event.subtype === 'message_changed') {
			yield* ingress.acceptMessageUpdated(yield* normalizeSlackMessageUpdated({ callback, identity }))
		} else if (event.type === 'message' && event.subtype === 'message_deleted') {
			yield* ingress.acceptMessageDeleted(yield* normalizeSlackMessageDeleted({ callback, identity }))
		} else {
			return yield* SlackWebhookError.make({ reason: 'decode' })
		}
		return HttpServerResponse.empty({ status: 200 })
	})

const acceptConversationStopped = (callback: SlackEventCallback) =>
	Effect.gen(function* () {
		const ingress = yield* Ingress
		yield* ingress.acceptConversationStopped(yield* normalizeSlackConversationStopped(callback))
		return HttpServerResponse.empty({ status: 200 })
	})

const routes = (config: SlackRoutesConfig) =>
	HttpRouter.add('POST', '/api/v1/integrations/slack/webhook', (request) =>
		Effect.gen(function* () {
			const body = yield* request.text
			const timestamp = request.headers['x-slack-request-timestamp']
			const signature = request.headers['x-slack-signature']
			if (timestamp === undefined || signature === undefined) {
				return yield* SlackWebhookError.make({ reason: 'missing_headers' })
			}
			yield* verifySlackSignature({
				body,
				timestamp,
				signature,
				signingSecret: config.signingSecret,
			})
			const payload = yield* Schema.decodeEffect(Schema.fromJsonString(SlackEventsRequest))(body, {
				onExcessProperty: 'preserve',
			}).pipe(Effect.mapError(() => SlackWebhookError.make({ reason: 'decode' })))
			return yield* Match.value(payload).pipe(
				Match.discriminatorsExhaustive('type')({
					url_verification: (verification) => Effect.succeed(HttpServerResponse.text(verification.challenge)),
					event_callback: (callback) =>
						Match.value(callback.event).pipe(
							Match.discriminatorsExhaustive('type')({
								app_mention: () => acceptMessageEvent(config, callback),
								message: (message) =>
									message.subtype === 'message_changed' || message.subtype === 'message_deleted'
										? acceptLifecycleEvent(config, callback)
										: acceptMessageEvent(config, callback),
								reaction_added: () => acceptLifecycleEvent(config, callback),
								reaction_removed: () => acceptLifecycleEvent(config, callback),
								agent_session_stopped: () => acceptConversationStopped(callback),
							}),
						),
				}),
			)
		}).pipe(
			Effect.catchTags({
				SlackWebhookError: webhookErrorResponse,
				IngressError: (error) =>
					Effect.logError('Slack ingress admission failed', error).pipe(
						Effect.as(HttpServerResponse.empty({ status: 503 })),
					),
			}),
			Effect.withSpan('slack.webhook', { attributes: { provider: 'slack' } }),
		),
	)

export const SlackRoutes = {
	layer: Layer.unwrap(
		Effect.gen(function* () {
			const credentials = yield* SlackTenantCredentials
			const client = yield* SlackClient
			const signingSecret = yield* Config.redacted('SLACK_SIGNING_SECRET')
			const botUserId = yield* Config.string('SLACK_BOT_USER_ID')
			const botId = yield* Config.option(Config.string('SLACK_BOT_ID'))
			const identity = slackBotIdentity({ botUserId, botId: Option.getOrUndefined(botId) })
			return routes({ signingSecret, identity, credentials, client })
		}),
	),
}
