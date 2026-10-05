import { describe, it } from '@effect/vitest'
import { Effect, Match, Queue, Schema } from 'effect'

import { SlackApi, SlackApiError, SlackFileAuthorizationError, type SlackFileUploadError } from '../src/SlackApi'
import { SlackFile, SlackFileId, SlackFileRef, type SlackUploadFileInput } from '../src/SlackModels'
import {
	botToken,
	channelRef,
	fileId,
	formParams,
	makeRecordingSlackHttp,
	privateDownloadUrl,
	type RecordedSlackRequest,
	rootTs,
	slackApiLayer,
	slackFileObject,
	slackMethod,
	teamId,
	threadRef,
} from './slack-file-fixtures'

const uploadUrl = `https://files.slack.com/upload/v1/CwABAAAAXwoAAZnVN2YaCwACAAAAC1UwNjFGN0FVUgsAAwAAAAtGMFM0M1BaREYA`
const bytes = new TextEncoder().encode('channels-live file body\n')
const input: SlackUploadFileInput = { filename: 'notes.txt', bytes, title: 'Notes', initialComment: 'Here it is' }

type UploadScript = {
	readonly uploadUrlResponse?: unknown
	readonly uploadStatus?: number
	readonly completeResponse?: unknown
}

const respond = (script: UploadScript) => (request: RecordedSlackRequest) => {
	if (request.url === uploadUrl)
		return new Response(`OK - ${bytes.byteLength}`, { status: script.uploadStatus ?? 200 })
	return Match.value(slackMethod(request)).pipe(
		Match.when('files.getUploadURLExternal', () =>
			Response.json(script.uploadUrlResponse ?? { ok: true, upload_url: uploadUrl, file_id: fileId }),
		),
		Match.when('files.completeUploadExternal', () =>
			Response.json(script.completeResponse ?? { ok: true, files: [slackFileObject] }),
		),
		Match.orElse(() => Response.json({ ok: false, error: 'unknown_method' }, { status: 404 })),
	)
}

type UploadTarget = 'channel' | 'thread'

const runUpload = <A, E>(
	script: UploadScript,
	target: UploadTarget,
	uploadInput: SlackUploadFileInput,
	finish: (effect: Effect.Effect<SlackFile, SlackFileUploadError, SlackApi>) => Effect.Effect<A, E, SlackApi>,
) =>
	Effect.gen(function* () {
		const requests = yield* Queue.unbounded<RecordedSlackRequest>()
		const outcome = yield* Effect.flatMap(SlackApi, (api) =>
			target === 'channel'
				? api.uploadFileToChannel({ channel: channelRef, input: uploadInput })
				: api.uploadFileToThread({ thread: threadRef, input: uploadInput }),
		).pipe(finish, Effect.provide(slackApiLayer(makeRecordingSlackHttp(requests, respond(script)))))
		return { outcome, requests: Array.from(yield* Queue.takeAll(requests)) }
	})

const uploadSucceeds = (script: UploadScript, target: UploadTarget, uploadInput = input) =>
	runUpload(script, target, uploadInput, (effect) => effect)

const uploadFails = (script: UploadScript, target: UploadTarget, uploadInput = input) =>
	runUpload(script, target, uploadInput, Effect.flip)

const CompletedFiles = Schema.fromJsonString(
	Schema.Array(Schema.Struct({ id: Schema.String, title: Schema.optionalKey(Schema.String) })),
)

const completionParams = (request: RecordedSlackRequest | undefined) =>
	Effect.gen(function* () {
		if (request === undefined) return yield* Effect.die('expected a completion request')
		const { files, ...rest } = formParams(request)
		const decoded = files === undefined ? null : yield* Schema.decodeEffect(CompletedFiles)(files)
		return { files: decoded, ...rest }
	})

const steps = (requests: ReadonlyArray<RecordedSlackRequest>) =>
	requests.map((request) => slackMethod(request) ?? `${request.method} ${new URL(request.url).pathname}`)

describe('Slack file upload', () => {
	it.effect('uploads to a thread through the external upload sequence', ({ expect }) =>
		Effect.gen(function* () {
			const { outcome: file, requests } = yield* uploadSucceeds({}, 'thread')
			expect(steps(requests)).toEqual([
				'files.getUploadURLExternal',
				`POST ${new URL(uploadUrl).pathname}`,
				'files.completeUploadExternal',
			])
			const [setup, transfer, completion] = requests
			expect(setup?.authorization).toBe(`Bearer ${botToken}`)
			expect(setup === undefined ? null : formParams(setup)).toEqual({
				filename: 'notes.txt',
				length: String(bytes.byteLength),
			})
			expect(transfer?.authorization).toBeNull()
			expect(transfer?.body).toEqual(bytes)
			expect(completion?.authorization).toBe(`Bearer ${botToken}`)
			expect(yield* completionParams(completion)).toEqual({
				files: [{ id: fileId, title: 'Notes' }],
				channel_id: channelRef.channelId,
				thread_ts: rootTs,
				initial_comment: 'Here it is',
			})
			expect(file).toEqual(
				SlackFile.make({
					ref: SlackFileRef.make({ teamId, fileId: SlackFileId.make(fileId) }),
					name: 'notes.txt',
					contentType: 'text/plain',
					size: bytes.byteLength,
					downloadUrl: privateDownloadUrl,
				}),
			)
			expect(Object.values(file)).not.toContain(uploadUrl)
		}),
	)

	it.effect('shares a channel upload at the channel root and omits absent options', ({ expect }) =>
		Effect.gen(function* () {
			const { outcome: file, requests } = yield* uploadSucceeds({}, 'channel', { filename: 'notes.txt', bytes })
			expect(file.ref.fileId).toBe(fileId)
			expect(yield* completionParams(requests[2])).toEqual({
				files: [{ id: fileId }],
				channel_id: channelRef.channelId,
			})
			expect(requests.some((request) => slackMethod(request) === 'files.upload')).toBe(false)
		}),
	)

	it.effect('names files:write when Slack reports a missing scope at either Web API stage', ({ expect }) =>
		Effect.gen(function* () {
			const missingScope = { ok: false, error: 'missing_scope', needed: 'files:write', provided: 'chat:write' }
			const setup = yield* uploadFails({ uploadUrlResponse: missingScope }, 'thread')
			expect(setup.outcome).toEqual(
				SlackFileAuthorizationError.make({
					operation: 'get_file_upload_url',
					requiredScope: 'files:write',
					retryable: false,
				}),
			)
			expect(steps(setup.requests)).toEqual(['files.getUploadURLExternal'])

			const completion = yield* uploadFails({ completeResponse: missingScope }, 'channel')
			expect(completion.outcome).toEqual(
				SlackFileAuthorizationError.make({
					operation: 'complete_file_upload',
					requiredScope: 'files:write',
					retryable: false,
				}),
			)
			expect(completion.requests).toHaveLength(3)
		}),
	)

	it.effect('reports setup, transfer, and completion failures as separate stages without retrying', ({ expect }) =>
		Effect.gen(function* () {
			const lookalike = yield* uploadFails(
				{
					uploadUrlResponse: {
						ok: true,
						upload_url: 'https://files.slack.com.example.com/upload/v1/abc',
						file_id: fileId,
					},
				},
				'thread',
			)
			expect(lookalike.outcome).toEqual(
				SlackApiError.make({
					operation: 'get_file_upload_url',
					message: 'Slack returned an invalid response',
				}),
			)
			expect(lookalike.requests).toHaveLength(1)

			const transfer = yield* uploadFails({ uploadStatus: 500 }, 'thread')
			expect(transfer.outcome).toEqual(
				SlackApiError.make({
					operation: 'upload_file_bytes',
					message: 'Slack upload URL returned HTTP 500',
				}),
			)
			expect(transfer.requests).toHaveLength(2)

			const completion = yield* uploadFails(
				{ completeResponse: { ok: false, error: 'invalid_channel' } },
				'thread',
			)
			expect(completion.outcome).toEqual(
				SlackApiError.make({ operation: 'complete_file_upload', message: 'invalid_channel' }),
			)
			expect(completion.requests).toHaveLength(3)

			const missingFile = yield* uploadFails({ completeResponse: { ok: true, files: [] } }, 'thread')
			expect(missingFile.outcome).toEqual(
				SlackApiError.make({
					operation: 'complete_file_upload',
					message: 'Slack did not return the uploaded file',
				}),
			)
		}),
	)
})
