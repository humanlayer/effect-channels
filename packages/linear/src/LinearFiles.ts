import { Effect, Schema, type Stream } from 'effect'

import { LinearApi, type LinearApiError, type LinearFileError } from './LinearApi'
import {
	discoverLinearFileRecords,
	type LinearDownloadBytesOptions,
	LinearDownloadFileBytesRequest,
	LinearDownloadFileRequest,
	LinearFileRecord,
	type LinearFileRef,
} from './LinearFileModels'

/** A Linear-hosted file whose methods download it with the workspace credential. */
export class LinearFile extends Schema.TaggedClass<LinearFile>()('LinearFile', LinearFileRecord.fields) {
	/** Streams the file with the workspace credential. The stream is not buffered. */
	download(): Effect.Effect<Stream.Stream<Uint8Array, LinearApiError>, LinearApiError, LinearApi> {
		return Effect.flatMap(LinearApi, (api) =>
			api.downloadFile(LinearDownloadFileRequest.make({ file: this.ref, url: this.url, size: this.size })),
		).pipe(Effect.withSpan('linear.file.download', { attributes: { 'linear.issue_id': this.ref.issueId } }))
	}

	/** Buffers the file, failing before `maxBytes` is exceeded. */
	downloadBytes(options: LinearDownloadBytesOptions): Effect.Effect<Uint8Array, LinearFileError, LinearApi> {
		return Effect.flatMap(LinearApi, (api) =>
			api.downloadFileBytes(
				LinearDownloadFileBytesRequest.make({
					file: this.ref,
					url: this.url,
					size: this.size,
					maxBytes: options.maxBytes,
				}),
			),
		).pipe(
			Effect.withSpan('linear.file.download_bytes', {
				attributes: { 'linear.issue_id': this.ref.issueId, 'linear.max_bytes': options.maxBytes },
			}),
		)
	}
}

/**
 * Finds Linear-hosted files linked or embedded in Markdown, as `LinearFile`s. See `discoverLinearFileRecords`.
 */
export const discoverLinearFiles = (ref: LinearFileRef, markdown: string | null): ReadonlyArray<LinearFile> =>
	discoverLinearFileRecords(ref, markdown).map((record) => LinearFile.make(record))
