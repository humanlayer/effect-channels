/**
 * This defines the HttpRouter that handles webhooks from providers (slack, github)
 *
 * It receives a list of providers at construction time, and then at execution time when it receives a webhook
 * it looks up the provider, processes the event through the provider, and then hands it off for Delivery
 */
import { Array as Arr, Match, Schema, Stream } from 'effect'
import * as Effect from 'effect/Effect'
import { HttpRouter, HttpServerResponse } from 'effect/unstable/http'
import type { HttpIncomingMessage } from 'effect/unstable/http/HttpIncomingMessage'

import { isRoutableMailboxKey } from './DeliveryReference'
import { DeliveryAdmission, MailboxDelivery, deliveryMailboxKey } from './MailboxDelivery'

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

export type ProviderWebhookError = WebhookAuthenticationError | WebhookPayloadInvalidError

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
	event: DeliveryAdmission,
})

/**
 * One provider webhook produced admissions for several independent mailboxes.
 * Delivery remains singular so a retry can safely deduplicate an already accepted prefix.
 */
export const ProviderWebhookEvents = Schema.TaggedStruct('Events', {
	events: Schema.NonEmptyArray(DeliveryAdmission),
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
	status: Schema.Int.check(Schema.isBetween({ minimum: 100, maximum: 599 })),
	body: Schema.NullOr(Schema.Uint8Array),
	headers: Schema.Record(Schema.String, Schema.String),
})
export type ProviderWebhookResponse = typeof ProviderWebhookResponse.Type

export const ProviderWebhookOutcome = Schema.Union([
	ProviderWebhookEvent,
	ProviderWebhookEvents,
	ProviderWebhookIgnored,
	ProviderWebhookResponse,
])
export type ProviderWebhookOutcome = typeof ProviderWebhookOutcome.Type

/**
 * Each provider implements this - it is used by the HttpRouter to handle creating the things
 */
export type WebhookProvider<R = never> = {
	readonly providerName: string
	/** Optional provider-specific limit, enforced while streaming the body. */
	readonly maxBodyBytes?: number
	readonly handle: (input: RawWebhookInput) => Effect.Effect<ProviderWebhookOutcome, ProviderWebhookError, R>
}

/**
 * A list of webhook providers where each entry keeps its own requirements, so providers that need
 * different services can share a list.
 */
export type WebhookProviders<Requirements extends ReadonlyArray<unknown>> = {
	readonly [Index in keyof Requirements]: WebhookProvider<Requirements[Index]>
}

class WebhookBodyTooLarge extends Schema.TaggedError<WebhookBodyTooLarge>()('WebhookBodyTooLarge', {}) {}

const readBoundedBody = <E>(request: HttpIncomingMessage<E>, maxBodyBytes?: number) => {
	if (maxBodyBytes === undefined) return Effect.map(request.arrayBuffer, (body) => new Uint8Array(body))
	const declaredLength = Number(request.headers['content-length'])
	if (Number.isFinite(declaredLength) && declaredLength > maxBodyBytes)
		return Effect.fail(WebhookBodyTooLarge.make({}))
	return request.stream.pipe(
		Stream.runFoldEffect(
			() => ({ chunks: Arr.empty<Uint8Array>(), size: 0 }),
			(state, chunk) => {
				const size = state.size + chunk.byteLength
				return size > maxBodyBytes
					? Effect.fail(WebhookBodyTooLarge.make({}))
					: Effect.succeed({ chunks: [...state.chunks, chunk], size })
			},
		),
		Effect.map(({ chunks, size }) => {
			const body = new Uint8Array(size)
			let offset = 0
			for (const chunk of chunks) {
				body.set(chunk, offset)
				offset += chunk.byteLength
			}
			return body
		}),
	)
}

export type WebhookRoutesOptions = {
	/**
	 * Where the routes are mounted inside a larger API, such as `/api/channels`.
	 * The webhook path becomes `<basePath>/integrations/:integration/webhook`.
	 */
	readonly basePath?: `/${string}`
}

/** The webhook path under an optional base path. Slashes around the base path are ignored. */
export const webhookRoutePath = (options?: WebhookRoutesOptions): `/${string}` => {
	const prefix = (options?.basePath ?? '').replace(/^\/+|\/+$/g, '')
	return prefix === '' ? '/integrations/:integration/webhook' : `/${prefix}/integrations/:integration/webhook`
}

/**
 * Http Router
 */
export const webhookRoutes = <const Requirements extends ReadonlyArray<unknown>>(
	webhookProviders: WebhookProviders<Requirements>,
	options?: WebhookRoutesOptions,
) =>
	HttpRouter.add('POST', webhookRoutePath(options), (request) =>
		Effect.gen(function* () {
			const providers: ReadonlyArray<WebhookProvider<Requirements[number]>> = webhookProviders
			const mailbox = yield* MailboxDelivery
			const { integration } = yield* HttpRouter.schemaPathParams(
				Schema.Struct({ integration: Schema.NonEmptyString }),
			)
			const provider = providers.find((candidate) => candidate.providerName === integration)
			if (provider === undefined) return HttpServerResponse.empty({ status: 404 })

			const outcome = yield* provider.handle({
				headers: request.headers,
				body: yield* readBoundedBody(request, provider.maxBodyBytes),
			})
			/** A mailbox key too long for a delivery ID would become work nothing can route to. */
			const deliverAdmission = (event: DeliveryAdmission) =>
				Effect.gen(function* () {
					if (!isRoutableMailboxKey(deliveryMailboxKey(event))) {
						return yield* new WebhookPayloadInvalidError({ reason: 'mailbox_key_too_long' })
					}
					return yield* mailbox.deliver(event)
				}).pipe(
					Effect.tap((receipt) =>
						Effect.logInfo('Mailbox admission recorded').pipe(
							Effect.annotateLogs({
								provider: event.provider,
								namespace: event.namespace,
								installation_id: event.installationId,
								resource_id: event.resourceId,
								event_id: event.eventId,
								mailbox_key: receipt.mailboxKey,
								accepted: receipt.accepted,
							}),
						),
					),
				)

			return yield* Match.value(outcome).pipe(
				Match.tagsExhaustive({
					Event: ({ event }) =>
						deliverAdmission(event).pipe(Effect.as(HttpServerResponse.empty({ status: 200 }))),
					Events: ({ events }) =>
						Effect.forEach(events, deliverAdmission, { discard: true }).pipe(
							Effect.as(HttpServerResponse.empty({ status: 200 })),
						),
					Ignored: () => Effect.succeed(HttpServerResponse.empty({ status: 200 })),
					Response: ({ body, headers, status }) =>
						Effect.succeed(HttpServerResponse.raw(body, { status, headers })),
				}),
			)
		}).pipe(
			Effect.catchTags({
				WebhookBodyTooLarge: () => Effect.succeed(HttpServerResponse.empty({ status: 413 })),
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
				MailboxDeliveryUnavailable: (error) =>
					Effect.logError('Webhook admission was unavailable', error).pipe(
						Effect.as(HttpServerResponse.text('Webhook admission unavailable', { status: 503 })),
					),
				MailboxDeliveryRejected: (error) =>
					Effect.logWarning('Webhook admission was rejected', error).pipe(
						Effect.as(HttpServerResponse.text('Webhook admission rejected', { status: 503 })),
					),
			}),
		),
	)
