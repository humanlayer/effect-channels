/**
 * This file defines the client a remote worker uses to call the delivery API.
 *
 * It is generated from the same `HttpApi` the server serves, so the two cannot drift apart. Build it
 * once per application; each call names its delivery and presents that delivery's token.
 */
import { Effect, Redacted } from 'effect'
import { HttpClient, HttpClientRequest } from 'effect/unstable/http'
import { HttpApiClient } from 'effect/unstable/httpapi'

import {
	prefixedDeliveryHttpApi,
	type AddLinkPayload,
	type CompleteDeliveryPayload,
	type FailDeliveryPayload,
} from './DeliveryHttpApi'

/**
 * @property baseUrl - the application's public origin, such as `https://agent.example.com`
 * @property basePath - the same `basePath` given to `Channels.make`
 */
export type DeliveryClientOptions = {
	readonly baseUrl: string | URL
	readonly basePath?: string | undefined
}

/** One delivery, and the token that lets the caller act on it. */
export type DeliveryClientTarget = {
	readonly deliveryId: string
	readonly accessToken: Redacted.Redacted
}

/**
 * A client for one application's delivery API. Takes the `HttpClient` from its environment when it is
 * built, such as `FetchHttpClient.layer`; its calls need nothing further.
 */
export const makeDeliveryClient = Effect.fn('delivery.client.make')(function* (options: DeliveryClientOptions) {
	const httpClient = yield* HttpClient.HttpClient
	const api = prefixedDeliveryHttpApi(options.basePath)

	/** The generated client for one call, with the delivery's token on every request. */
	const forDelivery = (target: DeliveryClientTarget) =>
		HttpApiClient.makeWith(api, {
			baseUrl: options.baseUrl,
			httpClient: httpClient.pipe(
				HttpClient.mapRequest(HttpClientRequest.bearerToken(Redacted.value(target.accessToken))),
			),
		})

	return {
		status: (target: DeliveryClientTarget) =>
			forDelivery(target).pipe(
				Effect.flatMap((client) => client.deliveries.status({ params: { deliveryId: target.deliveryId } })),
				Effect.withSpan('delivery.client.status'),
			),
		complete: (target: DeliveryClientTarget & { readonly payload?: CompleteDeliveryPayload }) =>
			forDelivery(target).pipe(
				Effect.flatMap((client) =>
					client.deliveries.complete({ params: { deliveryId: target.deliveryId }, payload: target.payload ?? {} }),
				),
				Effect.withSpan('delivery.client.complete'),
			),
		fail: (target: DeliveryClientTarget & { readonly payload?: FailDeliveryPayload }) =>
			forDelivery(target).pipe(
				Effect.flatMap((client) =>
					client.deliveries.fail({ params: { deliveryId: target.deliveryId }, payload: target.payload ?? {} }),
				),
				Effect.withSpan('delivery.client.fail'),
			),
		/** Add a link, such as a pull request the remote job opened. A URL already added is a replay. */
		addLink: (target: DeliveryClientTarget & { readonly link: AddLinkPayload }) =>
			forDelivery(target).pipe(
				Effect.flatMap((client) =>
					client.deliveries.addLink({ params: { deliveryId: target.deliveryId }, payload: target.link }),
				),
				Effect.withSpan('delivery.client.add_link'),
			),
	}
})

export type DeliveryClient = Effect.Success<ReturnType<typeof makeDeliveryClient>>
