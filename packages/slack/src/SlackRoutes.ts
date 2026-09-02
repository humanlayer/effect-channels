import { Ingress } from '@humanlayer/channels'
import { Effect, Match, Redacted, Schema } from 'effect'
import { HttpRouter, HttpServerResponse } from 'effect/unstable/http'

import { SlackWebhookError } from './Errors.ts'
import { SlackEventsRequest } from './Schema.ts'
import { normalizeSlackMessage } from './SlackNormalize.ts'
import { verifySlackSignature } from './SlackSignature.ts'

export type SlackRoutesOptions = {
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

export const SlackRoutes = {
	layer: (options: SlackRoutesOptions) =>
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
					signingSecret: options.signingSecret,
				})
				const payload = yield* Schema.decodeEffect(Schema.fromJsonString(SlackEventsRequest))(body, {
					onExcessProperty: 'preserve',
				}).pipe(Effect.mapError(() => SlackWebhookError.make({ reason: 'decode' })))
				return yield* Match.value(payload).pipe(
					Match.discriminatorsExhaustive('type')({
						url_verification: (verification) =>
							Effect.succeed(HttpServerResponse.text(verification.challenge)),
						event_callback: (callback) =>
							Effect.gen(function* () {
								const ingress = yield* Ingress
								const normalized = yield* normalizeSlackMessage({
									callback,
									botUserId: options.botUserId,
								})
								yield* ingress.acceptMessage(normalized)
								return HttpServerResponse.empty({ status: 200 })
							}),
					}),
				)
			}).pipe(
				Effect.catchTags({
					SlackWebhookError: webhookErrorResponse,
					IngressError: () => Effect.succeed(HttpServerResponse.empty({ status: 503 })),
				}),
				Effect.withSpan('slack.webhook', { attributes: { provider: 'slack' } }),
			),
		),
}
