import { Duration, Effect, Stream } from 'effect'
import type { HttpClientResponse } from 'effect/unstable/http/HttpClientResponse'

import {
	SlackApiError,
	type SlackDownloadFileRequest,
	SlackFileAuthorizationError,
	SlackFileSizeLimitExceeded,
} from '../SlackApi'
import { isSlackFileUrl, SlackHttpClient } from './SlackHttpClient'

/** An opened file response. The byte stream has not been read. */
export type SlackFileDownload = {
	readonly contentLength: number | null
	readonly stream: Stream.Stream<Uint8Array, SlackApiError>
}

const maxRedirects = 3
const hopTimeout = Duration.seconds(60)

const downloadFailure = (message: string) => SlackApiError.make({ operation: 'download_file', message })

const missingFilesRead = () =>
	SlackFileAuthorizationError.make({ operation: 'download_file', requiredScope: 'files:read', retryable: false })

const isRedirect = (response: HttpClientResponse) => response.status >= 300 && response.status < 400

const isSlackSignInHost = (hostname: string) =>
	['slack.com', 'slack-gov.com'].some((domain) => hostname === domain || hostname.endsWith(`.${domain}`))

/**
 * Accepts redirects only within the approved Slack file origins. Slack answers an unauthorized file request by
 * redirecting to its sign-in page, so a redirect to another Slack host means the token cannot read the file.
 */
const redirectTarget = (response: HttpClientResponse, current: string) => {
	const location = response.headers.location
	if (location === undefined || !URL.canParse(location, current))
		return Effect.fail(downloadFailure('Slack file redirect has no valid location'))
	const target = new URL(location, current)
	if (isSlackFileUrl(target.toString())) return Effect.succeed(target.toString())
	if (target.protocol === 'https:' && isSlackSignInHost(target.hostname)) return Effect.fail(missingFilesRead())
	return Effect.fail(downloadFailure('Slack file redirect target is not an approved Slack file origin'))
}

const isHtml = (value: string | null | undefined) => value?.toLowerCase().startsWith('text/html') === true

/**
 * Classifies a non-redirect file response. Slack serves its HTML sign-in page with status 200 when the token cannot
 * read the file, so an HTML body for a file Slack did not describe as HTML is an authorization failure.
 */
const inspectResponse = (response: HttpClientResponse, request: SlackDownloadFileRequest) => {
	const status = response.status
	if (status >= 200 && status < 300) {
		if (isHtml(response.headers['content-type']) && !isHtml(request.contentType))
			return Effect.fail(missingFilesRead())
		return Effect.succeed(response)
	}
	if (status === 401 || status === 403) return Effect.fail(missingFilesRead())
	if (status === 404) return Effect.fail(downloadFailure('Slack file was not found'))
	return Effect.fail(downloadFailure(`Slack file download returned HTTP ${status}`))
}

const parseContentLength = (value: string | undefined) => {
	if (value === undefined || !/^\d+$/.test(value)) return null
	const length = Number(value)
	return Number.isSafeInteger(length) ? length : null
}

const openHop = (url: string) =>
	Effect.flatMap(SlackHttpClient, (client) => client.executeFile({ operation: 'download_file', url })).pipe(
		Effect.timeoutOrElse({
			duration: hopTimeout,
			orElse: () => Effect.fail(downloadFailure('Slack file request timed out')),
		}),
	)

/**
 * Opens a private Slack file download with the bot token. The URL is validated against the approved Slack file
 * origins before transport, and redirects are followed manually within a small hop budget on those origins only.
 */
export const downloadSlackFile = Effect.fn('slack.api.download_file')(function* (request: SlackDownloadFileRequest) {
	if (request.downloadUrl === null)
		return yield* downloadFailure('Slack did not provide a private download URL for this file')
	let url = request.downloadUrl
	let response = yield* openHop(url)
	for (let hops = 0; isRedirect(response); hops += 1) {
		if (hops >= maxRedirects) return yield* downloadFailure('Slack file redirect limit exceeded')
		url = yield* redirectTarget(response, url)
		response = yield* openHop(url)
	}
	const opened = yield* inspectResponse(response, request)
	const download: SlackFileDownload = {
		contentLength: parseContentLength(opened.headers['content-length']),
		stream: opened.stream.pipe(Stream.mapError(() => downloadFailure('Slack file stream failed'))),
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
export const readBoundedSlackFileBytes = (
	download: SlackFileDownload,
	maxBytes: number,
): Effect.Effect<Uint8Array, SlackApiError | SlackFileSizeLimitExceeded> => {
	if (download.contentLength !== null && download.contentLength > maxBytes)
		return Effect.fail(
			SlackFileSizeLimitExceeded.make({
				maxBytes,
				observedBytes: download.contentLength,
				source: 'content_length',
			}),
		)
	return download.stream.pipe(
		Stream.runFoldEffect(
			(): BufferedFileBytes => ({ size: 0, chunks: [] }),
			(state: BufferedFileBytes, chunk): Effect.Effect<BufferedFileBytes, SlackFileSizeLimitExceeded> => {
				const size = state.size + chunk.byteLength
				if (size > maxBytes)
					return Effect.fail(
						SlackFileSizeLimitExceeded.make({ maxBytes, observedBytes: size, source: 'received_bytes' }),
					)
				state.chunks.push(chunk)
				return Effect.succeed({ size, chunks: state.chunks })
			},
		),
		Effect.map(({ chunks, size }) => concatenate(chunks, size)),
	)
}
