import { Ingress } from '@humanlayer/channels'
import { Config, Effect, Layer, Match, Option, Schema } from 'effect'
import type { Redacted } from 'effect'
import { HttpRouter, HttpServerResponse } from 'effect/unstable/http'

import { SlackWebhookError } from './Errors.ts'
import type { SlackBotIdentity, SlackEventCallback } from './Schema.ts'
import { SlackEventsRequest } from './Schema.ts'
import { mergeSlackBotIdentity, slackBotIdentity } from './SlackBotIdentity.ts'
import { normalizeSlackMessage } from './SlackNormalize.ts'
import { verifySlackSignature } from './SlackSignature.ts'
import { SlackTenantCredentials } from './SlackTenantCredentials.ts'

type SlackRoutesConfig = {
	readonly signingSecret: Redacted.Redacted<string>
	readonly identity: SlackBotIdentity
	readonly credentials: SlackTenantCredentials['Service']
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
					event_id: callback.event_id,
				}),
			)
			return HttpServerResponse.empty({ status: 200 })
		}
		yield* ingress.acceptMessage(normalized.value)
		return HttpServerResponse.empty({ status: 200 })
	})

const acknowledgeUnsupported = (callback: SlackEventCallback) =>
	Effect.logInfo('acknowledging and dropping unsupported Slack event family').pipe(
		Effect.annotateLogs({ provider: 'slack', event_type: callback.event.type, event_id: callback.event_id }),
		Effect.as(HttpServerResponse.empty({ status: 200 })),
	)

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
								message: () => acceptMessageEvent(config, callback),
								reaction_added: () => acknowledgeUnsupported(callback),
								reaction_removed: () => acknowledgeUnsupported(callback),
								agent_session_stopped: () => acknowledgeUnsupported(callback),
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
			const signingSecret = yield* Config.redacted('SLACK_SIGNING_SECRET')
			const botUserId = yield* Config.string('SLACK_BOT_USER_ID')
			const botId = yield* Config.option(Config.string('SLACK_BOT_ID'))
			const identity = slackBotIdentity({ botUserId, botId: Option.getOrUndefined(botId) })
			return routes({ signingSecret, identity, credentials })
		}),
	),
}
