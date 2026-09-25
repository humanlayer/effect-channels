import { Duration, Effect, Stream } from 'effect'
import * as HttpClientRequest from 'effect/unstable/http/HttpClientRequest'
import type { HttpClientResponse } from 'effect/unstable/http/HttpClientResponse'

import type { LinearApiError } from '../LinearApi'
import {
	canonicalLinearFileUrl,
	isLinearFileOrigin,
	type LinearDownloadFileRequest,
	LinearFileSizeLimitExceeded,
	type LinearFileTransferPolicy,
} from '../LinearFiles'
import {
	LinearFileOriginRejectedError,
	LinearFileRedirectError,
	LinearTransportError,
	linearProviderErrorDetails,
} from './LinearApiErrors'
import { inspectLinearFileResponse } from './LinearFileHttp'
import { LinearHttpClient } from './LinearHttpClient'

/** An opened file response. The byte stream has not been read. */
export type LinearFileDownload<E> = {
	readonly contentLength: number | null
	readonly stream: Stream.Stream<Uint8Array, E>
}

export type DownloadLinearFileInput = {
	readonly request: LinearDownloadFileRequest
	readonly policy: LinearFileTransferPolicy
}

const isRedirect = (response: HttpClientResponse) => response.status >= 300 && response.status < 400

const redirectError = (message: string) =>
	new LinearFileRedirectError(linearProviderErrorDetails('download_file', { message }))

const redirectTarget = (response: HttpClientResponse, current: string) => {
	const location = response.headers.location
	if (location === undefined || !URL.canParse(location, current))
		return Effect.fail(redirectError('Linear file redirect has no valid location'))
	const target = new URL(location, current)
	if (target.protocol !== 'https:') return Effect.fail(redirectError('Linear file redirect target is not HTTPS'))
	return Effect.succeed(target.toString())
}

const parseContentLength = (value: string | undefined) => {
	if (value === undefined || !/^\d+$/.test(value)) return null
	const length = Number(value)
	return Number.isSafeInteger(length) ? length : null
}

const openHop = (url: string, policy: LinearFileTransferPolicy) =>
	Effect.flatMap(LinearHttpClient, (client) =>
		client.executeFile({ operation: 'download_file', request: HttpClientRequest.get(url) }),
	).pipe(
		Effect.timeoutOrElse({
			duration: Duration.millis(policy.timeoutMs),
			orElse: () =>
				Effect.fail(
					new LinearTransportError(
						linearProviderErrorDetails('download_file', {
							retryable: true,
							message: 'Linear file request timed out',
						}),
					),
				),
		}),
	)

/**
 * Opens an authenticated Linear file download. The canonical URL is validated again before transport; redirects are
 * followed manually within the policy's hop budget, and each hop receives the credential only on the Linear origin.
 */
export const downloadLinearFile = Effect.fn('linear.api.download_file')(function* (input: DownloadLinearFileInput) {
	const canonical = canonicalLinearFileUrl(input.request.url)
	if (canonical === null)
		return yield* new LinearFileOriginRejectedError(
			linearProviderErrorDetails('download_file', { message: 'Not a canonical Linear upload URL' }),
		)
	let url = canonical
	let response = yield* openHop(url, input.policy)
	for (let hops = 0; isRedirect(response); hops += 1) {
		if (hops >= input.policy.maxRedirects) return yield* redirectError('Linear file redirect limit exceeded')
		url = yield* redirectTarget(response, url)
		response = yield* openHop(url, input.policy)
	}
	const opened = yield* inspectLinearFileResponse('download_file', response, isLinearFileOrigin(url))
	const download: LinearFileDownload<LinearTransportError> = {
		contentLength: parseContentLength(opened.headers['content-length']),
		stream: opened.stream.pipe(
			Stream.mapError(
				() =>
					new LinearTransportError(
						linearProviderErrorDetails('download_file', {
							retryable: true,
							message: 'Linear file stream failed',
						}),
					),
			),
		),
	}
	return download
})

type BufferedFileBytes = {
	readonly size: number
	readonly chunks: Array<Uint8Array>
}

const concatenate = (chunks: ReadonlyArray<Uint8Array>, size: number) => {
	const bytes = new Uint8Array(size)
	let offset = 0
	for (const chunk of chunks) {
		bytes.set(chunk, offset)
		offset += chunk.byteLength
	}
	return bytes
}

/** Buffers an opened download, rejecting from `Content-Length` or the running byte count before exceeding the bound. */
export const readBoundedLinearFileBytes = (
	download: LinearFileDownload<LinearApiError>,
	maxBytes: number,
): Effect.Effect<Uint8Array, LinearApiError | LinearFileSizeLimitExceeded> => {
	if (download.contentLength !== null && download.contentLength > maxBytes)
		return Effect.fail(
			LinearFileSizeLimitExceeded.make({
				maxBytes,
				observedBytes: download.contentLength,
				source: 'content_length',
			}),
		)
	return download.stream.pipe(
		Stream.runFoldEffect(
			(): BufferedFileBytes => ({ size: 0, chunks: [] }),
			(state: BufferedFileBytes, chunk): Effect.Effect<BufferedFileBytes, LinearFileSizeLimitExceeded> => {
				const size = state.size + chunk.byteLength
				if (size > maxBytes)
					return Effect.fail(
						LinearFileSizeLimitExceeded.make({ maxBytes, observedBytes: size, source: 'received_bytes' }),
					)
				state.chunks.push(chunk)
				return Effect.succeed({ size, chunks: state.chunks })
			},
		),
		Effect.map(({ chunks, size }) => concatenate(chunks, size)),
	)
}
