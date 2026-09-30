/**
 * This file serves the delivery API: it reads the bearer token and hands each request to `DeliveryControl`.
 */
import { Effect, FileSystem, Layer, Option, Path, Redacted } from 'effect'
import { Etag, HttpPlatform, type HttpServerRequest } from 'effect/unstable/http'
import { HttpApiBuilder } from 'effect/unstable/httpapi'

import { AddDeliveryLink, CompleteDelivery, DeliveryControl, FailDelivery } from './DeliveryControl'
import { DeliveryCredentialMissing, prefixedDeliveryHttpApi } from './DeliveryHttpApi'
import { ExternalLink } from './DeliveryLink'

const bearerPattern = /^Bearer\s+([A-Za-z0-9_-]+)\s*$/i

/** The bearer token of a request, or a 401 when there is none. */
const bearerToken = (request: HttpServerRequest.HttpServerRequest) =>
	Option.fromNullishOr(request.headers.authorization).pipe(
		Option.flatMap((header) => Option.fromNullishOr(bearerPattern.exec(header)?.[1])),
		Option.match({
			onNone: () => Effect.fail(new DeliveryCredentialMissing()),
			onSome: (token) => Effect.succeed(Redacted.make(token)),
		}),
	)

const handlers = (api: ReturnType<typeof prefixedDeliveryHttpApi>) =>
	HttpApiBuilder.group(api, 'deliveries', (group) =>
		Effect.gen(function* () {
			const control = yield* DeliveryControl
			return group
				.handle('status', ({ params, request }) =>
					Effect.gen(function* () {
						const accessToken = yield* bearerToken(request)
						return yield* control.status({ deliveryId: params.deliveryId, accessToken })
					}).pipe(Effect.withSpan('delivery.api.status')),
				)
				.handle('complete', ({ params, payload, request }) =>
					Effect.gen(function* () {
						const accessToken = yield* bearerToken(request)
						return yield* control.apply({
							deliveryId: params.deliveryId,
							accessToken,
							mutation: CompleteDelivery.make(payload),
						})
					}).pipe(Effect.withSpan('delivery.api.complete')),
				)
				.handle('fail', ({ params, payload, request }) =>
					Effect.gen(function* () {
						const accessToken = yield* bearerToken(request)
						return yield* control.apply({
							deliveryId: params.deliveryId,
							accessToken,
							mutation: FailDelivery.make(payload),
						})
					}).pipe(Effect.withSpan('delivery.api.fail')),
				)
				.handle('addLink', ({ params, payload, request }) =>
					Effect.gen(function* () {
						const accessToken = yield* bearerToken(request)
						return yield* control.apply({
							deliveryId: params.deliveryId,
							accessToken,
							mutation: AddDeliveryLink.make({ link: ExternalLink.make(payload) }),
						})
					}).pipe(Effect.withSpan('delivery.api.add_link')),
				)
		}),
	)

/**
 * `HttpApiBuilder.layer` declares file, path, and ETag services for static and multipart routes.
 * The delivery API serves neither, so it gets inert ones and a host needs no platform layer for it.
 */
const deliveryApiPlatform = Layer.mergeAll(
	HttpPlatform.layer.pipe(Layer.provideMerge(FileSystem.layerNoop({}))),
	Path.layer,
	Etag.layer,
)

/**
 * The delivery API routes under `basePath`. Needs a `DeliveryControl`.
 * Mount it on the same `HttpRouter` as the provider webhooks.
 */
export const deliveryApiRoutes = (options: { readonly basePath?: string | undefined }) => {
	const api = prefixedDeliveryHttpApi(options.basePath)
	return HttpApiBuilder.layer(api).pipe(Layer.provide(handlers(api)), Layer.provide(deliveryApiPlatform))
}
