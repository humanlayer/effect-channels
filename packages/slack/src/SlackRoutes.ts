import { Ingress } from '@humanlayer/channels'
import { Config, Effect, Layer, Match, Schema } from 'effect'
import type { Redacted } from 'effect'
import { HttpRouter, HttpServerResponse } from 'effect/unstable/http'

import { SlackWebhookError } from './Errors.ts'
import type { SlackEventCallback } from './Schema.ts'
import { SlackEventsRequest } from './Schema.ts'
import { normalizeSlackMessage } from './SlackNormalize.ts'
import { verifySlackSignature } from './SlackSignature.ts'

type SlackRoutesConfig = {
	readonly signingSecret: Redacted.Redacted<string>
	readonly botUserId: string
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

const acceptMessageEvent = (callback: SlackEventCallback, botUserId: string) =>
	Effect.gen(function* () {
		const ingress = yield* Ingress
		const normalized = yield* normalizeSlackMessage({ callback, botUserId })
		yield* ingress.acceptMessage(normalized)
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
								app_mention: () => acceptMessageEvent(callback, config.botUserId),
								message: () => acceptMessageEvent(callback, config.botUserId),
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
			const signingSecret = yield* Config.redacted('SLACK_SIGNING_SECRET')
			const botUserId = yield* Config.string('SLACK_BOT_USER_ID')
			return routes({ signingSecret, botUserId })
		}),
	),
}
