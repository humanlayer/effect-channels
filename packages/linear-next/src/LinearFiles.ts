import { Effect, Predicate, Schema, type Stream } from 'effect'
import type { Nodes } from 'mdast'
import { fromMarkdown } from 'mdast-util-from-markdown'
import { gfmFromMarkdown } from 'mdast-util-gfm'
import { gfm } from 'micromark-extension-gfm'

import { LinearApi, LinearApiError } from './LinearApi'
import { LinearIssueId, LinearOrganizationId } from './LinearIdentity'
import { LinearIssueRef } from './LinearModels'

/** The only origin that receives the workspace credential for file downloads. */
export const linearFileOrigin = 'https://uploads.linear.app'

/** Upper bound for one in-memory upload; Linear's `fileUpload` requires the exact byte size up front. */
export const linearMaxUploadBytes = 50 * 1024 * 1024

const linearFilePathSegment = /^[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?$/

const isLinearFilePathSegment = (segment: string) => linearFilePathSegment.test(segment) && !segment.includes('..')

/**
 * Returns the canonical `https://uploads.linear.app/<workspace>/<asset>/<file>` form of a Linear upload URL, or
 * null for every other URL, including lookalike hosts, other schemes, credentials, ports, and non-canonical paths.
 */
export const canonicalLinearFileUrl = (value: string): string | null => {
	if (!URL.canParse(value)) return null
	const url = new URL(value)
	if (url.origin !== linearFileOrigin || url.username !== '' || url.password !== '') return null
	const segments = url.pathname.split('/').slice(1)
	if (segments.length !== 3 || !segments.every(isLinearFilePathSegment)) return null
	return `${url.origin}${url.pathname}`
}

/** True only for HTTPS URLs on the approved Linear upload origin. */
export const isLinearFileOrigin = (value: string) => URL.canParse(value) && new URL(value).origin === linearFileOrigin

export const LinearFileUrl = Schema.String.check(
	Schema.makeFilter((value: string) =>
		canonicalLinearFileUrl(value) === value ? undefined : 'expected a canonical uploads.linear.app file URL',
	),
).pipe(Schema.brand('LinearFileUrl'))
export type LinearFileUrl = typeof LinearFileUrl.Type

export const LinearFileRef = Schema.Struct({
	organizationId: LinearOrganizationId,
	issueId: LinearIssueId,
})
export type LinearFileRef = typeof LinearFileRef.Type

export const LinearByteCount = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))

export const LinearDownloadBytesOptions = Schema.Struct({
	maxBytes: Schema.Int.check(Schema.isGreaterThan(0)),
})
export type LinearDownloadBytesOptions = typeof LinearDownloadBytesOptions.Type

export const LinearDownloadFileRequest = Schema.Struct({
	file: LinearFileRef,
	url: LinearFileUrl,
	size: Schema.NullOr(LinearByteCount),
})
export type LinearDownloadFileRequest = typeof LinearDownloadFileRequest.Type

export const LinearDownloadFileBytesRequest = Schema.Struct({
	...LinearDownloadFileRequest.fields,
	...LinearDownloadBytesOptions.fields,
})
export type LinearDownloadFileBytesRequest = typeof LinearDownloadFileBytesRequest.Type

export const LinearUploadBytes = Schema.Uint8Array.check(
	Schema.makeFilter((bytes: Uint8Array) =>
		bytes.byteLength <= linearMaxUploadBytes ? undefined : `expected at most ${linearMaxUploadBytes} bytes`,
	),
)

export const LinearUploadFileInput = Schema.Struct({
	filename: Schema.NonEmptyString,
	contentType: Schema.NonEmptyString,
	bytes: LinearUploadBytes,
})
export type LinearUploadFileInput = typeof LinearUploadFileInput.Type

export const LinearUploadFileRequest = Schema.Struct({ issue: LinearIssueRef, input: LinearUploadFileInput })
export type LinearUploadFileRequest = typeof LinearUploadFileRequest.Type

export const LinearUploadAttachmentInput = Schema.Struct({
	...LinearUploadFileInput.fields,
	title: Schema.optionalKey(Schema.String),
	subtitle: Schema.optionalKey(Schema.String),
	metadata: Schema.optionalKey(Schema.Record(Schema.String, Schema.Json)),
})
export type LinearUploadAttachmentInput = typeof LinearUploadAttachmentInput.Type

export const LinearUploadAttachmentRequest = Schema.Struct({
	issue: LinearIssueRef,
	input: LinearUploadAttachmentInput,
})
export type LinearUploadAttachmentRequest = typeof LinearUploadAttachmentRequest.Type

/** Transport limits for authenticated downloads. Redirects are followed manually and never inherit credentials. */
export const LinearFileTransferPolicy = Schema.Struct({
	maxRedirects: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0), Schema.isLessThanOrEqualTo(10)),
	timeoutMs: Schema.Int.check(Schema.isGreaterThan(0)),
})
export type LinearFileTransferPolicy = typeof LinearFileTransferPolicy.Type

export const defaultLinearFileTransferPolicy = LinearFileTransferPolicy.make({ maxRedirects: 3, timeoutMs: 60_000 })

export class LinearFileSizeLimitExceeded extends Schema.TaggedError<LinearFileSizeLimitExceeded>()(
	'LinearFileSizeLimitExceeded',
	{
		maxBytes: Schema.Int,
		observedBytes: LinearByteCount,
		source: Schema.Literals(['declared_size', 'content_length', 'received_bytes']),
	},
) {}

export const LinearFileError = Schema.Union([LinearApiError, LinearFileSizeLimitExceeded])
export type LinearFileError = typeof LinearFileError.Type

const contentTypeByExtension = new Map([
	['csv', 'text/csv'],
	['gif', 'image/gif'],
	['gz', 'application/gzip'],
	['html', 'text/html'],
	['jpeg', 'image/jpeg'],
	['jpg', 'image/jpeg'],
	['json', 'application/json'],
	['log', 'text/plain'],
	['md', 'text/markdown'],
	['mov', 'video/quicktime'],
	['mp3', 'audio/mpeg'],
	['mp4', 'video/mp4'],
	['pdf', 'application/pdf'],
	['png', 'image/png'],
	['svg', 'image/svg+xml'],
	['txt', 'text/plain'],
	['wav', 'audio/wav'],
	['webm', 'video/webm'],
	['webp', 'image/webp'],
	['xml', 'application/xml'],
	['yaml', 'application/yaml'],
	['yml', 'application/yaml'],
	['zip', 'application/zip'],
])

const contentTypeFromName = (name: string | null) => {
	if (name === null) return null
	const extension = /\.([A-Za-z0-9]+)$/.exec(name)?.[1]?.toLowerCase()
	if (Predicate.isUndefined(extension)) return null
	return contentTypeByExtension.get(extension) ?? null
}

type LinearFileCandidate = {
	readonly url: string
	readonly label: string | null
}

const textOf = (node: Nodes): string => {
	switch (node.type) {
		case 'text':
		case 'inlineCode':
			return node.value
		case 'image':
			return node.alt ?? ''
		default:
			return 'children' in node ? node.children.map(textOf).join('') : ''
	}
}

const safeLabel = (label: string | null | undefined, url: string) => {
	const trimmed = label?.trim() ?? ''
	if (trimmed.length === 0 || trimmed === url) return null
	return trimmed.slice(0, 255)
}

const collectDefinitions = (node: Nodes, definitions: Map<string, string>) => {
	if (node.type === 'definition' && !definitions.has(node.identifier)) definitions.set(node.identifier, node.url)
	if ('children' in node) for (const child of node.children) collectDefinitions(child, definitions)
}

const collectCandidates = (
	node: Nodes,
	definitions: ReadonlyMap<string, string>,
	candidates: Array<LinearFileCandidate>,
) => {
	switch (node.type) {
		case 'link':
			candidates.push({ url: node.url, label: safeLabel(textOf(node), node.url) })
			break
		case 'image':
			candidates.push({ url: node.url, label: safeLabel(node.alt, node.url) })
			break
		case 'linkReference':
		case 'imageReference': {
			const url = definitions.get(node.identifier)
			if (Predicate.isNotUndefined(url)) candidates.push({ url, label: safeLabel(textOf(node), url) })
			break
		}
		default:
			break
	}
	if ('children' in node) for (const child of node.children) collectCandidates(child, definitions, candidates)
}

export class LinearFile extends Schema.TaggedClass<LinearFile>()('LinearFile', {
	ref: LinearFileRef,
	url: LinearFileUrl,
	name: Schema.NullOr(Schema.String),
	contentType: Schema.NullOr(Schema.String),
	size: Schema.NullOr(LinearByteCount),
}) {
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
 * Finds Linear-hosted files linked or embedded in Markdown. Links, images, reference links, autolinks, and GFM bare
 * URLs are parsed structurally; only canonical `uploads.linear.app` URLs become files, in first-occurrence order.
 */
export const discoverLinearFiles = (ref: LinearFileRef, markdown: string | null): ReadonlyArray<LinearFile> => {
	if (markdown === null || !markdown.toLowerCase().includes('uploads.linear.app')) return []
	const tree = fromMarkdown(markdown, { extensions: [gfm()], mdastExtensions: [gfmFromMarkdown()] })
	const definitions = new Map<string, string>()
	collectDefinitions(tree, definitions)
	const candidates: Array<LinearFileCandidate> = []
	collectCandidates(tree, definitions, candidates)
	const files = new Map<string, LinearFile>()
	for (const candidate of candidates) {
		const url = canonicalLinearFileUrl(candidate.url)
		if (url === null || files.has(url)) continue
		files.set(
			url,
			LinearFile.make({
				ref,
				url: LinearFileUrl.make(url),
				name: candidate.label,
				contentType: contentTypeFromName(candidate.label),
				size: null,
			}),
		)
	}
	return Array.from(files.values())
}
