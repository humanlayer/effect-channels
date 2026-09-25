import { Effect, Schema } from 'effect'

import { LinearFileUrl, type LinearUploadFileRequest } from '../LinearFiles'
import { failLinearMutation } from './LinearApiErrors'
import { linearGraphql } from './LinearGraphql'

const document = `mutation LinearFileUpload($contentType: String!, $filename: String!, $size: Int!) { fileUpload(contentType: $contentType, filename: $filename, size: $size) { success uploadFile { assetUrl uploadUrl filename contentType size headers { key value } } } }`

const HttpsUrl = Schema.String.check(
	Schema.makeFilter((value: string) =>
		URL.canParse(value) && new URL(value).protocol === 'https:' ? undefined : 'expected an HTTPS URL',
	),
)

/** Package-private signed upload target. Only the asset fields may cross into public values. */
export const LinearUploadTarget = Schema.Struct({
	assetUrl: LinearFileUrl,
	uploadUrl: Schema.RedactedFromValue(HttpsUrl, { disallowEncode: true }),
	filename: Schema.String,
	contentType: Schema.String,
	size: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
	headers: Schema.Array(Schema.Struct({ key: Schema.NonEmptyString, value: Schema.String })),
})
export type LinearUploadTarget = typeof LinearUploadTarget.Type

const RequestLinearFileUploadResponse = Schema.Struct({
	fileUpload: Schema.Struct({ success: Schema.Boolean, uploadFile: Schema.NullOr(LinearUploadTarget) }),
})

export const requestLinearFileUpload = Effect.fn('linear.api.request_file_upload')((input: LinearUploadFileRequest) =>
	linearGraphql({
		operation: 'request_file_upload',
		query: document,
		variables: {
			contentType: input.input.contentType,
			filename: input.input.filename,
			size: input.input.bytes.byteLength,
		},
		response: RequestLinearFileUploadResponse,
	}).pipe(
		Effect.flatMap(({ fileUpload }) => {
			if (!fileUpload.success || fileUpload.uploadFile === null) return failLinearMutation('request_file_upload')
			return Effect.succeed(fileUpload.uploadFile)
		}),
	),
)
