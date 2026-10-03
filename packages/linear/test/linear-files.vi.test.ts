import { describe, it } from '@effect/vitest'
import { DeliveryAdmission, MailboxSubscriptions, ProviderEventHandled } from '@humanlayer/channels-delivery'
import { Effect, Layer, Queue, Schema, Stream } from 'effect'

import { LinearApi } from '../src/LinearApi'
import type { LinearIssueCreated, LinearSubscribedEvents } from '../src/LinearCallbackEvents'
import { LinearCallbacks } from '../src/LinearCallbacks'
import { makeLinearEventProcessor } from '../src/LinearEventProcessor'
import {
	canonicalLinearFileUrl,
	discoverLinearFiles,
	LinearFile,
	LinearFileRef,
	LinearFileUrl,
	LinearUploadFileInput,
} from '../src/LinearFiles'
import { LinearAttachmentId } from '../src/LinearIdentity'
import { LinearIssueAttachment, LinearIssue } from '../src/LinearResources'
import { issue } from './api-test-fixtures'
import { firstAttempt, issueCreatePayload, linearAppUserId, linearOrganizationId } from './fixtures'

const workspace = '8f1d3c4e-1111-4222-8333-944455556666'
const upload = (asset: string) =>
	`https://uploads.linear.app/${workspace}/${asset}/0b1c2d3e-aaaa-4bbb-8ccc-ddddeeeeffff`
const imageUrl = upload('11111111-1111-4111-8111-111111111111')
const textUrl = upload('22222222-2222-4222-8222-222222222222')
const autolinkUrl = upload('33333333-3333-4333-8333-333333333333')
const bareUrl = upload('44444444-4444-4444-8444-444444444444')
const referenceUrl = upload('55555555-5555-4555-8555-555555555555')
const fileRef = LinearFileRef.make({ organizationId: issue.organizationId, issueId: issue.issueId })

const markdown = [
	`Intro ![screenshot.png](${imageUrl}) and [notes.txt](${textUrl}).`,
	`Autolink <${autolinkUrl}> and bare ${bareUrl} here.`,
	`Reference [spec][spec] and a duplicate [again](${imageUrl}).`,
	'Ordinary [site](https://example.com/file.png) stays content.',
	`Lookalike [x](https://uploads.linear.app.example.com/${workspace}/a/b) and [y](http://uploads.linear.app/${workspace}/a/b).`,
	`Wrong shape [z](https://uploads.linear.app/${workspace}/only-two).`,
	`Code \`${upload('66666666-6666-4666-8666-666666666666')}\` is not a link.`,
	'',
	`[spec]: ${referenceUrl}`,
].join('\n')

describe('Linear file discovery', () => {
	it.effect('parses Markdown structurally and keeps only canonical Linear upload URLs in order', ({ expect }) =>
		Effect.sync(() => {
			const files = discoverLinearFiles(fileRef, markdown)
			expect(files.map((file) => file.url)).toEqual([imageUrl, textUrl, autolinkUrl, bareUrl, referenceUrl])
			expect(files.map((file) => file.name)).toEqual(['screenshot.png', 'notes.txt', null, null, 'spec'])
			expect(files.map((file) => file.contentType)).toEqual(['image/png', 'text/plain', null, null, null])
			expect(files.map((file) => file.size)).toEqual([null, null, null, null, null])
			expect(files.map((file) => file.ref)).toEqual(files.map(() => fileRef))
			expect(discoverLinearFiles(fileRef, null)).toEqual([])
			expect(discoverLinearFiles(fileRef, 'no files')).toEqual([])
		}),
	)

	it.effect('rejects lookalike hosts, other schemes, credentials, ports, and non-canonical paths', ({ expect }) =>
		Effect.gen(function* () {
			expect(canonicalLinearFileUrl(`${imageUrl}?signature=abc#frag`)).toBe(imageUrl)
			for (const value of [
				`https://uploads.linear.app.example.com/${workspace}/a/b`,
				`https://evil.example/uploads.linear.app/${workspace}/a/b`,
				`http://uploads.linear.app/${workspace}/a/b`,
				`https://user:pass@uploads.linear.app/${workspace}/a/b`,
				`https://uploads.linear.app:8443/${workspace}/a/b`,
				`https://uploads.linear.app/${workspace}/a`,
				`https://uploads.linear.app/${workspace}/../b`,
				`https://uploads.linear.app/${workspace}/a%2Fb/c`,
				'not a url',
			])
				expect(canonicalLinearFileUrl(value)).toBeNull()
			yield* Schema.decodeEffect(LinearFileUrl)(`https://uploads.linear.app.example.com/${workspace}/a/b`).pipe(
				Effect.flip,
			)
			yield* Schema.decodeEffect(LinearUploadFileInput)({
				filename: 'a.txt',
				contentType: 'text/plain',
				bytes: new Uint8Array(50 * 1024 * 1024 + 1),
			}).pipe(Effect.flip)
		}),
	)

	it.effect('discovers files while normalizing issue descriptions and comment bodies', ({ expect }) =>
		Effect.gen(function* () {
			const namespace = 'linear-files-test'
			const created = issueCreatePayload
			const description = `Spec ![diagram.png](${imageUrl}) and [site](https://example.com)`
			const createdEvents = yield* Queue.unbounded<LinearIssueCreated>()
			const subscribedEvents = yield* Queue.unbounded<LinearSubscribedEvents>()
			const services = Layer.mergeAll(
				LinearCallbacks.layer({
					onIssueCreated: (event) => Effect.asVoid(Queue.offer(createdEvents, event)),
					onSubscribedEvent: (event) => Effect.asVoid(Queue.offer(subscribedEvents, event)),
				}),
				Layer.mock(LinearApi, {}),
				Layer.mock(MailboxSubscriptions, {
					subscribe: () => Effect.die('not used'),
					isSubscribed: () => Effect.succeed(true),
					unsubscribe: () => Effect.die('not used'),
				}),
			)
			const processor = makeLinearEventProcessor({
				namespace,
				bot: { organizationId: linearOrganizationId, appUserId: linearAppUserId },
			})
			const admission = (eventId: string, payload: Schema.Json) =>
				DeliveryAdmission.make({
					namespace,
					provider: 'linear',
					installationId: created.organizationId,
					resourceId: `linear:v1:issue:${created.data.id}`,
					eventId,
					payload,
				})
			const issueCreate = { ...created, data: { ...created.data, description } }
			const opened = yield* processor
				.process([admission('issue-create', issueCreate)], yield* firstAttempt())
				.pipe(Effect.provide(services))
			expect(opened).toEqual(ProviderEventHandled.make({}))
			const createdEvent = yield* Queue.take(createdEvents)
			expect(createdEvent.issue.description).toBe(description)
			expect(createdEvent.issue.files.map((file) => [file.url, file.name, file.ref.issueId])).toEqual([
				[imageUrl, 'diagram.png', created.data.id],
			])

			const commentBody = `See [notes.txt](${textUrl}) and https://example.com/x.png`
			const commentCreate = {
				createdAt: '2026-01-01T00:00:00.000Z',
				webhookId: 'comment-webhook',
				webhookTimestamp: 1,
				type: 'Comment',
				action: 'create',
				organizationId: created.organizationId,
				data: {
					id: 'comment-1',
					body: commentBody,
					issueId: created.data.id,
					createdAt: '2026-01-01T00:00:00.000Z',
					updatedAt: '2026-01-01T00:00:00.000Z',
					reactionData: {},
				},
			}
			yield* processor
				.process([admission('comment-create', commentCreate)], yield* firstAttempt())
				.pipe(Effect.provide(services))
			const subscribed = yield* Queue.take(subscribedEvents)
			const [event] = subscribed.events
			if (event?._tag !== 'LinearCommentCreated') return yield* Effect.die('expected LinearCommentCreated')
			expect(event.comment.content.markdown).toBe(commentBody)
			expect(event.comment.files.map((file) => file.url)).toEqual([textUrl])
		}),
	)
})

describe('Linear file resources', () => {
	it.effect('delegate uploads and downloads through LinearApi with named requests', ({ expect }) =>
		Effect.gen(function* () {
			const calls = yield* Queue.unbounded<{ readonly method: string; readonly input: unknown }>()
			const file = LinearFile.make({
				ref: fileRef,
				url: LinearFileUrl.make(textUrl),
				name: 'notes.txt',
				contentType: 'text/plain',
				size: 5,
			})
			const attachment = LinearIssueAttachment.make({
				id: LinearAttachmentId.make('attachment-1'),
				issueId: issue.issueId,
				title: 'notes.txt',
				subtitle: null,
				url: textUrl,
				ref: { issue, attachmentId: LinearAttachmentId.make('attachment-1') },
			})
			const bytes = new TextEncoder().encode('hello')
			const layer = Layer.mock(LinearApi, {
				uploadFile: (input) => Queue.offer(calls, { method: 'uploadFile', input }).pipe(Effect.as(file)),
				uploadAttachment: (input) =>
					Queue.offer(calls, { method: 'uploadAttachment', input }).pipe(Effect.as(attachment)),
				downloadFile: (input) =>
					Queue.offer(calls, { method: 'downloadFile', input }).pipe(Effect.as(Stream.make(bytes))),
				downloadFileBytes: (input) =>
					Queue.offer(calls, { method: 'downloadFileBytes', input }).pipe(Effect.as(bytes)),
			})
			const resource = LinearIssue.make({
				ref: issue,
				mailboxKey: 'mailbox',
				identifier: 'CORE-1',
				number: 1,
				title: 'Issue',
				description: null,
				priority: 1,
				url: '',
				team: null,
				creator: null,
				files: [],
			})
			const input = { filename: 'notes.txt', contentType: 'text/plain', bytes }
			const uploaded = yield* resource.uploadFile(input).pipe(Effect.provide(layer))
			const attached = yield* resource.uploadAttachment({ ...input, title: 'Notes' }).pipe(Effect.provide(layer))
			const stream = yield* file.download().pipe(Effect.provide(layer))
			const streamed = yield* Stream.runCollect(stream)
			const buffered = yield* file.downloadBytes({ maxBytes: 10 }).pipe(Effect.provide(layer))

			expect(uploaded).toBe(file)
			expect(attached).toBe(attachment)
			expect(streamed).toEqual([bytes])
			expect(buffered).toBe(bytes)
			expect(Array.from(yield* Queue.takeAll(calls))).toEqual([
				{ method: 'uploadFile', input: { issue, input } },
				{ method: 'uploadAttachment', input: { issue, input: { ...input, title: 'Notes' } } },
				{ method: 'downloadFile', input: { file: fileRef, url: textUrl, size: 5 } },
				{ method: 'downloadFileBytes', input: { file: fileRef, url: textUrl, size: 5, maxBytes: 10 } },
			])
		}),
	)
})
