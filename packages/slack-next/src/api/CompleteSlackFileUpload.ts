import { Effect, Predicate, Schema } from 'effect'

import { SlackApiError } from '../SlackApi'
import type { SlackMessageTs } from '../SlackIdentity'
import type { SlackChannelRef, SlackUploadFileInput } from '../SlackModels'
import { SlackFileMetadata } from '../SlackWebhookEventSchemas'
import type { SlackUploadTarget } from './GetSlackFileUploadUrl'
import { SlackHttpClient } from './SlackHttpClient'

/** Where a completed upload is shared. A null `threadTs` shares at the channel root. */
export type SlackFileUploadDestination = {
	readonly channel: SlackChannelRef
	readonly threadTs: SlackMessageTs | null
}

export type CompleteSlackFileUploadInput = {
	readonly target: SlackUploadTarget
	readonly destination: SlackFileUploadDestination
	readonly input: SlackUploadFileInput
}

const CompleteUploadFile = Schema.Struct({ id: Schema.String, title: Schema.optionalKey(Schema.String) })
const CompleteUploadFiles = Schema.fromJsonString(Schema.Array(CompleteUploadFile))

const CompleteUploadResponse = Schema.Struct({ files: Schema.Array(SlackFileMetadata) })

/**
 * Calls `files.completeUploadExternal`, sharing the uploaded file to the destination channel or thread with the
 * optional title and initial comment. Returns Slack's metadata for the uploaded file.
 */
export const completeSlackFileUpload = Effect.fn('slack.api.complete_file_upload')(function* (
	upload: CompleteSlackFileUploadInput,
) {
	const client = yield* SlackHttpClient
	const title = upload.input.title
	const uploaded = Predicate.isUndefined(title)
		? CompleteUploadFile.make({ id: upload.target.fileId })
		: CompleteUploadFile.make({ id: upload.target.fileId, title })
	const files = yield* Schema.encodeEffect(CompleteUploadFiles)([uploaded]).pipe(
		Effect.mapError(() =>
			SlackApiError.make({ operation: 'complete_file_upload', message: 'Could not encode Slack request' }),
		),
	)
	const response = yield* client.postForm({
		operation: 'complete_file_upload',
		method: 'files.completeUploadExternal',
		params: {
			files,
			channel_id: upload.destination.channel.channelId,
			thread_ts: upload.destination.threadTs ?? undefined,
			initial_comment: upload.input.initialComment,
		},
		response: CompleteUploadResponse,
	})
	const file = response.files.find((candidate) => candidate.id === upload.target.fileId)
	if (file === undefined)
		return yield* SlackApiError.make({
			operation: 'complete_file_upload',
			message: 'Slack did not return the uploaded file',
		})
	return file
})
