/**
 * How AgentSession reports on the deliveries handed to it: through the delivery API that `bot.deliveryApi`
 * serves on the Ingress Worker, with the client generated from the same `HttpApi`, as any remote worker does.
 */
import { makeDeliveryClient, type DeliveryClient } from '@humanlayer/channels-delivery'
import * as Cloudflare from 'alchemy/Cloudflare'
import { Context, Effect, Layer } from 'effect'
import { HttpClient } from 'effect/http'

/** The Ingress Worker's service binding to itself, declared on its `env`. */
export const DELIVERY_API_BINDING = 'DELIVERY_API'

/** Requests through a service binding never reach this host; the binding decides where they go. */
const BINDING_BASE_URL = 'http://localhost'

/**
 * An `HttpClient` that sends each request to the Ingress Worker through its self service binding, never over
 * the public internet. The binding exists only on the deployed Worker, so it is read when a request is made.
 */
const selfBindingHttpClient = Layer.effect(
	HttpClient.HttpClient,
	Effect.gen(function* () {
		const env = yield* Cloudflare.Workers.WorkerEnvironment
		return HttpClient.make((request) =>
			Effect.suspend(() =>
				Cloudflare.toHttpClient(Cloudflare.fromCloudflareFetcher(env[DELIVERY_API_BINDING])).execute(request),
			),
		)
	}),
)

export class DeliveryApi extends Context.Service<DeliveryApi, DeliveryClient>()('alchemy-cloudflare/DeliveryApi') {
	/** The client over whichever `HttpClient` its environment supplies. */
	static readonly layer = Layer.effect(DeliveryApi, makeDeliveryClient({ baseUrl: BINDING_BASE_URL }))

	/** The client over the Ingress Worker's self service binding. */
	static readonly layerSelfBinding = DeliveryApi.layer.pipe(Layer.provide(selfBindingHttpClient))
}
