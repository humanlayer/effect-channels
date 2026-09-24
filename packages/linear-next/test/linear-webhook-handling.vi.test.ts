import { describe, it } from '@effect/vitest'
import {
	ProviderWebhookEvent,
	ProviderWebhookIgnored,
	WebhookAuthenticationError,
	WebhookPayloadInvalidError,
} from '@humanlayer/channels-delivery-next'
import { Effect, Logger, Schema } from 'effect'
import { Headers } from 'effect/unstable/http'

import { issueCreatePayload, makeLinearTestProvider, signedLinearBody, signedLinearInput } from './fixtures'

describe('Linear webhook handling', () => {
	it.effect('verifies exact bytes and accepts signed Issue.create', ({ expect }) =>
		Effect.gen(function* () {
			const outcome = yield* makeLinearTestProvider().handle(signedLinearInput())
			expect(Schema.is(ProviderWebhookEvent)(outcome)).toBe(true)
		}),
	)

	it.effect('rejects a modified body with the old signature', ({ expect }) =>
		Effect.gen(function* () {
			const signed = signedLinearInput()
			const changed = new TextEncoder().encode(`${new TextDecoder().decode(signed.body)} `)
			const error = yield* makeLinearTestProvider()
				.handle({ ...signed, body: changed })
				.pipe(Effect.flip)
			expect(Schema.is(WebhookAuthenticationError)(error)).toBe(true)
		}),
	)

	it.effect('acknowledges authenticated unsupported actions', ({ expect }) =>
		Effect.gen(function* () {
			const payload = { ...(issueCreatePayload as Record<string, unknown>), action: 'unsupported' }
			const outcome = yield* makeLinearTestProvider().handle(signedLinearInput(payload))
			expect(outcome).toEqual(ProviderWebhookIgnored.make({}))
		}),
	)

	it.effect('keeps Document callbacks as authenticated unsupported ignores', ({ expect }) =>
		Effect.gen(function* () {
			const payload = { ...(issueCreatePayload as Record<string, unknown>), type: 'Document' }
			const outcome = yield* makeLinearTestProvider().handle(signedLinearInput(payload))
			expect(outcome).toEqual(ProviderWebhookIgnored.make({}))
		}),
	)

	it.effect('rejects stale timestamps', ({ expect }) =>
		Effect.gen(function* () {
			const body = new TextEncoder().encode(JSON.stringify(issueCreatePayload))
			const error = yield* makeLinearTestProvider()
				.handle(signedLinearBody(body, undefined, 100_000))
				.pipe(Effect.flip)
			expect(error).toEqual(WebhookAuthenticationError.make({ reason: 'stale_timestamp' }))
		}),
	)

	it.effect('logs safe structural diagnostics at each webhook decode boundary', ({ expect }) =>
		Effect.gen(function* () {
			const logs: Array<string> = []
			const logger = Logger.layer([
				Logger.make((entry) => logs.push(JSON.stringify(Logger.formatStructured.log(entry)))),
			])
			const provider = makeLinearTestProvider()

			const invalidHeaders = yield* provider
				.handle({
					headers: Headers.fromInput({}),
					body: new TextEncoder().encode('private-header-boundary-sentinel'),
				})
				.pipe(Effect.flip, Effect.provide(logger))
			expect(invalidHeaders).toEqual(WebhookAuthenticationError.make({ reason: 'invalid_signature_headers' }))

			const invalidJson = new TextEncoder().encode('private-json-boundary-sentinel')
			const invalidJsonError = yield* provider
				.handle(signedLinearBody(invalidJson))
				.pipe(Effect.flip, Effect.provide(logger))
			expect(invalidJsonError).toEqual(WebhookPayloadInvalidError.make({ reason: 'invalid_json' }))

			const invalidEnvelope = {
				type: 'AppUserNotification',
				privatePayload: 'private-envelope-boundary-sentinel',
			}
			const invalidEnvelopeError = yield* provider
				.handle(signedLinearInput(invalidEnvelope))
				.pipe(Effect.flip, Effect.provide(logger))
			expect(invalidEnvelopeError).toEqual(WebhookPayloadInvalidError.make({ reason: 'invalid_event_envelope' }))

			const output = logs.join('\n')
			expect(output).toContain('"decode_stage":"headers"')
			expect(output).toContain('"has_linear_signature":false')
			expect(output).toContain('"decode_stage":"json"')
			expect(output).toContain(`"body_byte_length":${invalidJson.byteLength}`)
			expect(output).toContain('"decode_stage":"event_envelope"')
			expect(output).toContain('"has_type":true')
			expect(output).toContain('"has_action":false')
			expect(output).toContain('"has_organization_id":false')
			expect(output).not.toContain('private-header-boundary-sentinel')
			expect(output).not.toContain('private-json-boundary-sentinel')
			expect(output).not.toContain('private-envelope-boundary-sentinel')
		}),
	)
})
