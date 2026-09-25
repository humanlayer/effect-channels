import { Effect, Schema } from 'effect'

import { SlackFileId, type SlackUploadFileInput } from '../SlackModels'
import { isSlackFileUrl, SlackHttpClient } from './SlackHttpClient'

const SlackFileUploadUrl = Schema.String.check(
	Schema.makeFilter((value: string) =>
		isSlackFileUrl(value) ? undefined : 'expected an HTTPS URL on an approved Slack file origin',
	),
)

/** Package-private upload target. The upload URL is a bearer capability and never leaves the upload sequence. */
export const SlackUploadTarget = Schema.Struct({
	fileId: SlackFileId,
	uploadUrl: Schema.RedactedFromValue(SlackFileUploadUrl, { disallowEncode: true }),
})
export type SlackUploadTarget = typeof SlackUploadTarget.Type

const GetUploadUrlResponse = Schema.Struct({
	upload_url: Schema.RedactedFromValue(SlackFileUploadUrl, { disallowEncode: true }),
	file_id: SlackFileId,
})

/** Calls `files.getUploadURLExternal` with the caller's filename and exact byte length. */
export const getSlackFileUploadUrl = Effect.fn('slack.api.get_file_upload_url')(function* (
	input: SlackUploadFileInput,
) {
	const client = yield* SlackHttpClient
	const response = yield* client.postForm({
		operation: 'get_file_upload_url',
		method: 'files.getUploadURLExternal',
		params: { filename: input.filename, length: input.bytes.byteLength },
		response: GetUploadUrlResponse,
	})
	return SlackUploadTarget.make({ fileId: response.file_id, uploadUrl: response.upload_url })
})
