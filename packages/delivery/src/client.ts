import { HttpClient, HttpClientRequest } from 'effect/unstable/http'
import { HttpApiClient } from 'effect/unstable/httpapi'

import { DeliveryContract } from './contract.js'

export const makeDeliveryClient = (input: { readonly baseUrl: string | URL }) =>
	HttpApiClient.make(DeliveryContract, { baseUrl: input.baseUrl })

export const makeMountedDeliveryClient = (input: {
	readonly baseUrl: string | URL
	readonly mountPath: `/${string}`
	readonly headers?: Readonly<Record<string, string>>
}) => {
	const headers = input.headers
	return HttpApiClient.make(DeliveryContract.prefix(input.mountPath), {
		baseUrl: input.baseUrl,
		transformClient:
			headers === undefined
				? undefined
				: HttpClient.mapRequest((request) => HttpClientRequest.setHeaders(request, headers)),
	})
}
