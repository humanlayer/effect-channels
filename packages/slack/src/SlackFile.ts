import { Effect, Schema, type Stream } from 'effect'

import { SlackApi } from './SlackApi'
import type { SlackApiError, SlackFileDownloadError } from './SlackApiContract'
import { SlackDownloadFileBytesRequest, SlackDownloadFileRequest } from './SlackApiContract'
import type { SlackTeamId } from './SlackIdentity'
import { type SlackDownloadBytesOptions, SlackFileRecord, slackFileRecordFromMetadata } from './SlackModels'
import type { SlackFileMetadata } from './SlackWebhookEventSchemas'

/** A Slack file you can download. `SlackApi` returns its fields as a `SlackFileRecord`. */
export class SlackFile extends Schema.TaggedClass<SlackFile>()('SlackFile', SlackFileRecord.fields) {
	/** Streams the file with the bot token. The stream is not buffered. */
	download(): Effect.Effect<Stream.Stream<Uint8Array, SlackApiError>, SlackFileDownloadError, SlackApi> {
		return Effect.flatMap(SlackApi, (api) =>
			api.downloadFile(
				SlackDownloadFileRequest.make({
					file: this.ref,
					downloadUrl: this.downloadUrl,
					contentType: this.contentType,
					size: this.size,
				}),
			),
		).pipe(
			Effect.withSpan('slack.file.download', {
				attributes: { 'slack.team_id': this.ref.teamId, 'slack.file_id': this.ref.fileId },
			}),
		)
	}

	/** Buffers the file, failing before `maxBytes` is exceeded. */
	downloadBytes(options: SlackDownloadBytesOptions): Effect.Effect<Uint8Array, SlackFileDownloadError, SlackApi> {
		return Effect.flatMap(SlackApi, (api) =>
			api.downloadFileBytes(
				SlackDownloadFileBytesRequest.make({
					file: this.ref,
					downloadUrl: this.downloadUrl,
					contentType: this.contentType,
					size: this.size,
					maxBytes: options.maxBytes,
				}),
			),
		).pipe(
			Effect.withSpan('slack.file.download_bytes', {
				attributes: {
					'slack.team_id': this.ref.teamId,
					'slack.file_id': this.ref.fileId,
					'slack.max_bytes': options.maxBytes,
				},
			}),
		)
	}
}

/** Builds a `SlackFile` from webhook or Web API file metadata; see `slackFileRecordFromMetadata`. */
export const slackFileFromMetadata = (teamId: SlackTeamId, metadata: SlackFileMetadata): SlackFile =>
	SlackFile.make(slackFileRecordFromMetadata(teamId, metadata))
