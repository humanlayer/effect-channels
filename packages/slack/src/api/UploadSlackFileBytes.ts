import { Effect, Redacted } from 'effect'
import * as FetchHttpClient from 'effect/http/FetchHttpClient'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientRequest from 'effect/http/HttpClientRequest'

import { SlackApiError } from '../SlackApi'
import type { SlackUploadTarget } from './GetSlackFileUploadUrl'

export type UploadSlackFileBytesInput = {
	readonly target: SlackUploadTarget
	readonly bytes: Uint8Array
}

/**
 * Posts the exact bytes to Slack's upload URL. It uses the raw HTTP client, so the bot token cannot be attached; the
 * upload URL itself authorizes the transfer.
 */
export const uploadSlackFileBytes = Effect.fn('slack.api.upload_file_bytes')(function* (
	input: UploadSlackFileBytesInput,
) {
	const client = yield* HttpClient.HttpClient
	const request = HttpClientRequest.post(Redacted.value(input.target.uploadUrl)).pipe(
		HttpClientRequest.bodyUint8Array(input.bytes, 'application/octet-stream'),
	)
	const response = yield* client.execute(request).pipe(
		Effect.mapError(() =>
			SlackApiError.make({ operation: 'upload_file_bytes', message: 'Could not reach the Slack upload URL' }),
		),
		Effect.provideService(FetchHttpClient.RequestInit, { redirect: 'manual' }),
		Effect.provideService(HttpClient.TracerDisabledWhen, () => true),
	)
	if (response.status < 200 || response.status >= 300)
		return yield* SlackApiError.make({
			operation: 'upload_file_bytes',
			message: `Slack upload URL returned HTTP ${response.status}`,
		})
})
