/**
 * This defines the HttpRouter that handles webhooks from providers (slack, github)
 *
 * It receives a list of providers at construction time, and then at execution time when it receives a webhook
 * it looks up the provider, processes the event through the provider, and then hands it off for Delivery
 */
import { Match, Schema } from 'effect'
import * as Effect from 'effect/Effect'
import { HttpRouter, HttpServerResponse } from 'effect/unstable/http'
import type { HttpIncomingMessage } from 'effect/unstable/http/HttpIncomingMessage'

import { DeliveryAdmission, DeliveryQueue } from './DeliveryQueue'

/**
 * The provider failed to authenticate the webhook
 */
export class WebhookAuthenticationError extends Schema.TaggedError<WebhookAuthenticationError>()(
	'WebhookAuthenticationError',
	{ reason: Schema.String },
) {}

/**
 * The payload was invalid
 */
export class WebhookPayloadInvalidError extends Schema.TaggedError<WebhookPayloadInvalidError>()(
	'WebhookPayloadInvalidError',
	{ reason: Schema.String },
) {}

export type ProviderWebhookError = typeof WebhookAuthenticationError.Type | typeof WebhookPayloadInvalidError.Type

/**
 * Platform-agnostic thing that a request body can be translated to regardless of whether it's a node server or a Request object
 */
export type RawWebhookInput = {
	readonly headers: HttpIncomingMessage['headers']
	readonly body: Uint8Array
}

/**
 * The provider webhook outcome is the provider deciding how to respond to the webhook.
 * Event outcomes are admitted to the delivery queue; Response outcomes preserve the provider's status, body, and headers.
 */
export const ProviderWebhookEvent = Schema.TaggedStruct('Event', {
	admission: DeliveryAdmission,
})

/**
 * The webhook is valid provider traffic, but there is nothing for the delivery system to process.
 */
export const ProviderWebhookIgnored = Schema.TaggedStruct('Ignored', {})

/**
 * The provider needs to control the immediate webhook response, including status, body, and headers.
 * Slack URL verification challenges are one example.
 */
export const ProviderWebhookResponse = Schema.TaggedStruct('Response', {
	status: Schema.Number,
	body: Schema.NullOr(Schema.Uint8Array),
	headers: Schema.Record(Schema.String, Schema.String),
})
export type ProviderWebhookResponse = typeof ProviderWebhookResponse.Type

export const ProviderWebhookOutcome = Schema.Union([
	ProviderWebhookEvent,
	ProviderWebhookIgnored,
	ProviderWebhookResponse,
])
export type ProviderWebhookOutcome = typeof ProviderWebhookOutcome.Type

/**
 * Each provider implements this - it is used by the HttpRouter to handle creating the things
 */
export type WebhookProvider<R = never> = {
	readonly key: string
	readonly handle: (input: RawWebhookInput) => Effect.Effect<ProviderWebhookOutcome, ProviderWebhookError, R>
}

/**
 * Http Router
 */
export const webhookRoutes = <R>(providers: ReadonlyArray<WebhookProvider<R>>) =>
	HttpRouter.add('POST', '/integrations/:integration/webhook', (request) =>
		Effect.gen(function* () {
			// Get the delivery Queue service
			const queue = yield* DeliveryQueue
			const { integration } = yield* HttpRouter.schemaPathParams(
				Schema.Struct({ integration: Schema.NonEmptyString }),
			)
			const provider = providers.find((candidate) => candidate.key === integration)
			if (provider === undefined) return HttpServerResponse.empty({ status: 404 })

			const outcome = yield* provider.handle({
				headers: request.headers,
				body: new Uint8Array(yield* request.arrayBuffer),
			})

			return yield* Match.value(outcome).pipe(
				Match.tagsExhaustive({
					Event: ({ admission }) =>
						queue.enqueue(admission).pipe(Effect.as(HttpServerResponse.empty({ status: 200 }))),
					Ignored: () => Effect.succeed(HttpServerResponse.empty({ status: 200 })),
					// Return the provider-indicated response to the webhook - most LIKELY a 200 but depends
					// is provider-specific so there is not a generic case
					Response: ({ body, headers, status }) =>
						Effect.succeed(HttpServerResponse.raw(body, { status, headers })),
				}),
			)
		}).pipe(
			Effect.catchTags({
				SchemaError: (error) =>
					Effect.logError('Webhook route parameters were invalid', error).pipe(
						Effect.as(HttpServerResponse.text('Invalid webhook route', { status: 400 })),
					),
				WebhookAuthenticationError: (error) =>
					Effect.logWarning('Webhook authentication failed', error).pipe(
						Effect.as(HttpServerResponse.text('Unauthorized', { status: 401 })),
					),
				WebhookPayloadInvalidError: (error) =>
					Effect.logWarning('Webhook payload was invalid', error).pipe(
						Effect.as(HttpServerResponse.text('Invalid webhook payload', { status: 400 })),
					),
				DeliveryQueueUnavailable: (error) =>
					Effect.logError('Webhook admission was unavailable', error).pipe(
						Effect.as(HttpServerResponse.text('Webhook admission unavailable', { status: 503 })),
					),
				DeliveryQueueRejected: (error) =>
					Effect.logWarning('Webhook admission was rejected', error).pipe(
						Effect.as(HttpServerResponse.text('Webhook admission rejected', { status: 503 })),
					),
			}),
		),
	)
