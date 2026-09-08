import { assert, it } from '@effect/vitest'
import { Effect, Layer, Logger, Predicate, Queue, Schema } from 'effect'

import { SlackApiError, type SlackTransportError } from '../src/Errors.js'
import { AttachmentRef, FileUpload, TenantId, type UnknownTenant } from '../src/index.js'
import { SlackFileDownloadInput, SlackFileUploadInput } from '../src/Schema.js'
import { SlackClient, slackFileLimits } from '../src/SlackClient.js'
import { makeSlackClientHarness, slackJsonResponse, testChannelId, testTeamId } from './support.js'

const attachment = AttachmentRef.make({
	provider: 'slack',
	tenant: TenantId.make(testTeamId),
	id: 'F_SECURE',
	kind: 'file',
	name: 'secure.bin',
	providerLocator: { id: 'F_SECURE' },
})

const fileInfo = (url: string) =>
	slackJsonResponse(
		JSON.stringify({
			ok: true,
			file: {
				id: 'F_SECURE',
				name: 'secure.bin',
				mimetype: 'application/octet-stream',
				url_private_download: url,
			},
		}),
	)

const assertSlackApiCode = (error: SlackApiError | SlackTransportError | UnknownTenant, code: string) => {
	if (!Schema.is(SlackApiError)(error)) {
		throw new Error(`Expected SlackApiError(${code})`)
	}
	assert.strictEqual(error.code, code)
}

it.effect('downloads from a recognized Slack origin with authentication and a bounded byte body', () =>
	Effect.gen(function* () {
		const expected = new Uint8Array([0, 1, 2, 250, 255])
		const harness = yield* makeSlackClientHarness((request) => {
			if (request.url.pathname.endsWith('/files.info')) {
				return fileInfo('https://files.slack.com/files-pri/F_SECURE/secure.bin')
			}
			return new Response(expected, { headers: { 'content-type': 'application/octet-stream' } })
		})
		const program = Effect.gen(function* () {
			const client = yield* SlackClient
			const downloaded = yield* client.downloadFile(
				SlackFileDownloadInput.make({ teamId: testTeamId, attachment }),
			)
			assert.deepStrictEqual([...downloaded], [...expected])
			const requests = yield* Queue.takeAll(harness.requests)
			assert.strictEqual(requests.length, 2)
			assert.strictEqual(requests[1]?.url.origin, 'https://files.slack.com')
			assert.ok(requests[1]?.authorization?.startsWith('Bearer '))
		})
		yield* program.pipe(Effect.provide(harness.layer))
	}),
)

it.effect('rejects an untrusted redirect before the bot token can leave Slack', () =>
	Effect.gen(function* () {
		const harness = yield* makeSlackClientHarness((request) => {
			if (request.url.pathname.endsWith('/files.info')) {
				return fileInfo('https://files.slack.com/files-pri/F_SECURE/secure.bin')
			}
			return new Response(null, {
				status: 302,
				headers: { location: 'https://attacker.example/steal' },
			})
		})
		const program = Effect.gen(function* () {
			const client = yield* SlackClient
			const error = yield* Effect.flip(
				client.downloadFile(SlackFileDownloadInput.make({ teamId: testTeamId, attachment })),
			)
			assertSlackApiCode(error, 'untrusted_redirect')
			const requests = yield* Queue.takeAll(harness.requests)
			assert.strictEqual(requests.length, 2)
			assert.ok(requests.every((request) => request.url.hostname !== 'attacker.example'))
		})
		yield* program.pipe(Effect.provide(harness.layer))
	}),
)

it.effect('rejects download bodies above the configured limit before buffering them', () =>
	Effect.gen(function* () {
		const harness = yield* makeSlackClientHarness((request) =>
			request.url.pathname.endsWith('/files.info')
				? fileInfo('https://files.slack.com/files-pri/F_SECURE/secure.bin')
				: new Response(new Uint8Array([1]), {
						headers: {
							'content-length': String(slackFileLimits.maxDownloadBytes + 1),
							'content-type': 'application/octet-stream',
						},
					}),
		)
		const program = Effect.gen(function* () {
			const client = yield* SlackClient
			const error = yield* Effect.flip(
				client.downloadFile(SlackFileDownloadInput.make({ teamId: testTeamId, attachment })),
			)
			assertSlackApiCode(error, 'file_too_large')
		})
		yield* program.pipe(Effect.provide(harness.layer))
	}),
)

it.effect('rejects oversized uploads before making an HTTP request', () =>
	Effect.gen(function* () {
		const harness = yield* makeSlackClientHarness(() =>
			slackJsonResponse(JSON.stringify({ ok: false, error: 'unexpected_request' })),
		)
		const program = Effect.gen(function* () {
			const client = yield* SlackClient
			const oversized = FileUpload.make({
				filename: 'too-large.bin',
				data: new Uint8Array(slackFileLimits.maxFileBytes + 1),
			})
			const error = yield* Effect.flip(
				client.uploadFiles(
					SlackFileUploadInput.make({
						teamId: testTeamId,
						channelId: testChannelId,
						files: [oversized],
					}),
				),
			)
			assertSlackApiCode(error, 'file_too_large')
			assert.strictEqual(yield* Queue.size(harness.requests), 0)
		})
		yield* program.pipe(Effect.provide(harness.layer))
	}),
)

it.effect('rejects HTML login responses instead of returning them as file bytes', () =>
	Effect.gen(function* () {
		const harness = yield* makeSlackClientHarness((request) =>
			request.url.pathname.endsWith('/files.info')
				? fileInfo('https://files.slack.com/files-pri/F_SECURE/secure.bin')
				: new Response('<html>sign in</html>', { headers: { 'content-type': 'text/html' } }),
		)
		const program = Effect.gen(function* () {
			const client = yield* SlackClient
			const error = yield* Effect.flip(
				client.downloadFile(SlackFileDownloadInput.make({ teamId: testTeamId, attachment })),
			)
			assertSlackApiCode(error, 'unexpected_html')
		})
		yield* program.pipe(Effect.provide(harness.layer))
	}),
)

it.effect('rejects attachment references from another tenant before resolving a file URL', () =>
	Effect.gen(function* () {
		const harness = yield* makeSlackClientHarness(() =>
			slackJsonResponse(JSON.stringify({ ok: false, error: 'unexpected_request' })),
		)
		const wrongTenant = AttachmentRef.make({
			...attachment,
			tenant: TenantId.make('T_OTHER'),
		})
		const program = Effect.gen(function* () {
			const client = yield* SlackClient
			const error = yield* Effect.flip(
				client.downloadFile(SlackFileDownloadInput.make({ teamId: testTeamId, attachment: wrongTenant })),
			)
			assertSlackApiCode(error, 'invalid_attachment_origin')
			assert.strictEqual(yield* Queue.size(harness.requests), 0)
		})
		yield* program.pipe(Effect.provide(harness.layer))
	}),
)

it.effect('keeps tokens and binary content out of file failure logs', () =>
	Effect.gen(function* () {
		const records: Array<string> = []
		const recorder = Logger.make((entry) => {
			const messages = Array.isArray(entry.message) ? entry.message : [entry.message]
			records.push(messages.filter(Predicate.isString).join(' '))
		})
		const harness = yield* makeSlackClientHarness(() =>
			slackJsonResponse(JSON.stringify({ ok: false, error: 'request_rejected' })),
		)
		const marker = 'BINARY-CONTENT-MUST-NOT-LEAK'
		const program = Effect.gen(function* () {
			const client = yield* SlackClient
			yield* Effect.exit(
				client.uploadFiles({
					teamId: testTeamId,
					channelId: testChannelId,
					files: [FileUpload.make({ filename: 'secret.bin', data: new TextEncoder().encode(marker) })],
				}),
			)
		})
		yield* program.pipe(Effect.provide(Layer.merge(harness.layer, Logger.layer([recorder]))))
		const logText = records.join('\n')
		assert.ok(!logText.includes(marker))
		assert.ok(!logText.includes('xoxb-test-token'))
	}),
)
