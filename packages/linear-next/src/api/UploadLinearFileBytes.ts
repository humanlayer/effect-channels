import { Effect, Redacted } from 'effect'
import * as FetchHttpClient from 'effect/unstable/http/FetchHttpClient'
import * as HttpClient from 'effect/unstable/http/HttpClient'
import * as HttpClientRequest from 'effect/unstable/http/HttpClientRequest'

import { LinearTransportError, linearProviderErrorDetails } from './LinearApiErrors'
import { inspectLinearFileResponse } from './LinearFileHttp'
import type { LinearUploadTarget } from './RequestLinearFileUpload'

export type UploadLinearFileBytesInput = {
	readonly target: LinearUploadTarget
	readonly contentType: string
	readonly bytes: Uint8Array
}

/**
 * Sends the exact bytes to Linear's signed upload target with only the provider-returned headers and declared
 * content type. It uses the raw HTTP client, so the workspace bearer credential cannot be attached.
 */
export const uploadLinearFileBytes = Effect.fn('linear.api.upload_file_bytes')(function* (
	input: UploadLinearFileBytesInput,
) {
	const client = yield* HttpClient.HttpClient
	const request = HttpClientRequest.put(Redacted.value(input.target.uploadUrl)).pipe(
		HttpClientRequest.bodyUint8Array(input.bytes, input.contentType),
		HttpClientRequest.setHeader('content-type', input.contentType),
		HttpClientRequest.setHeaders(Object.fromEntries(input.target.headers.map(({ key, value }) => [key, value]))),
	)
	const response = yield* client.execute(request).pipe(
		Effect.mapError(
			() => new LinearTransportError(linearProviderErrorDetails('upload_file_bytes', { retryable: true })),
		),
		Effect.provideService(FetchHttpClient.RequestInit, { redirect: 'manual' }),
		Effect.provideService(HttpClient.TracerDisabledWhen, () => true),
	)
	yield* inspectLinearFileResponse('upload_file_bytes', response, false)
})
