import { NodeCrypto, NodeRuntime } from '@effect/platform-node'
import { Array as Arr, Config, Crypto, Effect, Encoding, Layer, Ref, Schema, Stream } from 'effect'

import {
	discoverLinearFiles,
	LinearApi,
	type LinearApiError,
	LinearApiLive,
	type LinearFile,
	LinearFileRef,
	LinearIssue,
	LinearIssueId,
	LinearIssueRef,
	LinearOrganizationId,
	LinearTeamId,
} from '../src/index'

const SmokeMarker = Schema.String.check(Schema.isPattern(/^channels-live-p5-\d+$/))

const onePixelPng = 'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=='

const sha256 = Effect.fn('linear.files_smoke.sha256')(function* (bytes: Uint8Array) {
	const crypto = yield* Crypto.Crypto
	return Encoding.encodeHex(yield* crypto.digest('SHA-256', bytes))
})

const concatenate = (chunks: ReadonlyArray<Uint8Array>) => {
	const bytes = new Uint8Array(chunks.reduce((size, chunk) => size + chunk.byteLength, 0))
	let offset = 0
	for (const chunk of chunks) {
		bytes.set(chunk, offset)
		offset += chunk.byteLength
	}
	return bytes
}

const verifyDownload = Effect.fn('linear.files_smoke.verify_download')(function* (
	file: LinearFile,
	original: Uint8Array,
) {
	const streamed = concatenate(yield* Stream.runCollect(yield* file.download()))
	const buffered = yield* file.downloadBytes({ maxBytes: original.byteLength })
	const expected = yield* sha256(original)
	const streamedHash = yield* sha256(streamed)
	const bufferedHash = yield* sha256(buffered)
	if (streamedHash !== expected || bufferedHash !== expected)
		return yield* Effect.die(`Downloaded bytes for ${file.name ?? 'file'} do not match the upload`)
	const limited = yield* file.downloadBytes({ maxBytes: original.byteLength - 1 }).pipe(Effect.flip)
	yield* Effect.logInfo('Linear file verified').pipe(
		Effect.annotateLogs({
			filename: file.name ?? 'unnamed',
			bytes: original.byteLength,
			sha256: expected,
			limit_error: limited._tag,
		}),
	)
})

const program = Effect.gen(function* () {
	const marker = yield* Config.schema(SmokeMarker, 'LINEAR_FILES_SMOKE_CONFIRM')
	const organizationId = yield* Config.schema(LinearOrganizationId, 'LINEAR_ORGANIZATION_ID')
	const issueId = yield* Config.schema(LinearIssueId, 'LINEAR_SMOKE_ISSUE_ID')
	const keep = yield* Config.boolean('LINEAR_FILES_SMOKE_KEEP').pipe(Config.withDefault(false))
	const pngBytes = yield* Effect.fromResult(Encoding.decodeBase64(onePixelPng))
	const api = yield* LinearApi
	const info = yield* api.getIssue({
		issue: LinearIssueRef.make({ organizationId, teamId: LinearTeamId.make('resolved-by-get-issue'), issueId }),
	})
	const issue = LinearIssue.make({
		ref: info.ref,
		mailboxKey: 'linear-files-live-smoke',
		identifier: info.identifier,
		number: null,
		title: info.title,
		description: info.description,
		priority: info.priority,
		url: info.url,
		team: null,
		creator: null,
		files: [],
	})
	const cleanup = yield* Ref.make<ReadonlyArray<Effect.Effect<void>>>([])
	const remember = (effect: Effect.Effect<void, LinearApiError, LinearApi>) =>
		Ref.update(cleanup, (effects) => [
			...effects,
			effect.pipe(Effect.provideService(LinearApi, api), Effect.ignore),
		])

	yield* Effect.gen(function* () {
		const textBytes = new TextEncoder().encode(`${marker} text asset\n`)
		const text = yield* issue.uploadFile({
			filename: `${marker}.txt`,
			contentType: 'text/plain',
			bytes: textBytes,
		})
		const image = yield* issue.uploadFile({ filename: `${marker}.png`, contentType: 'image/png', bytes: pngBytes })
		const attachment = yield* issue.uploadAttachment({
			filename: `${marker}-attachment.txt`,
			contentType: 'text/plain',
			bytes: textBytes,
		})
		yield* remember(attachment.remove())
		const card = yield* issue.createAttachment({ url: 'https://example.com', title: `${marker} external card` })
		yield* remember(card.remove())
		const comment = yield* issue.postComment({
			markdown: [
				`${marker} files`,
				`![${image.name ?? 'image'}](${image.url})`,
				`[${text.name ?? 'text'}](${text.url})`,
				'[external](https://example.com/not-a-linear-file.png)',
			].join('\n\n'),
		})
		yield* remember(comment.remove())
		const lookalikes = discoverLinearFiles(
			LinearFileRef.make({ organizationId, issueId }),
			`[x](https://uploads.linear.app.example.com/${organizationId}/a/b)`,
		)
		yield* Effect.logInfo('Linear files uploaded').pipe(
			Effect.annotateLogs({
				issue: info.identifier,
				comment_files: comment.files.length,
				attachment_id: attachment.ref.attachmentId,
				external_card_id: card.ref.attachmentId,
				lookalike_files: lookalikes.length,
			}),
		)
		if (comment.files.length !== 2 || !Arr.isReadonlyArrayEmpty(lookalikes))
			return yield* Effect.die('Markdown discovery did not return exactly the uploaded Linear files')
		yield* verifyDownload(text, textBytes)
		yield* verifyDownload(image, pngBytes)
	}).pipe(
		Effect.ensuring(
			keep
				? Effect.logInfo('Keeping smoke comment and attachments for manual inspection')
				: Effect.flatMap(Ref.get(cleanup), (effects) => Effect.all([...effects].reverse(), { discard: true })),
		),
	)
})

NodeRuntime.runMain(program.pipe(Effect.provide(Layer.mergeAll(LinearApiLive, NodeCrypto.layer))))
