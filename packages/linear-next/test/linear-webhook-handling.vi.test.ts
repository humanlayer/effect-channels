import { describe, it } from '@effect/vitest'
import {
	ProviderWebhookEvent,
	ProviderWebhookIgnored,
	WebhookAuthenticationError,
} from '@humanlayer/channels-delivery-next'
import { Effect, Schema } from 'effect'

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
			const error = yield* makeLinearTestProvider().handle({ ...signed, body: changed }).pipe(Effect.flip)
			expect(Schema.is(WebhookAuthenticationError)(error)).toBe(true)
		}),
	)

	it.effect('acknowledges authenticated unsupported actions', ({ expect }) =>
		Effect.gen(function* () {
			const payload = { ...(issueCreatePayload as Record<string, unknown>), action: 'update' }
			const outcome = yield* makeLinearTestProvider().handle(signedLinearInput(payload))
			expect(outcome).toEqual(ProviderWebhookIgnored.make({}))
		}),
	)

	it.effect('rejects stale timestamps', ({ expect }) =>
		Effect.gen(function* () {
			const body = new TextEncoder().encode(JSON.stringify(issueCreatePayload))
			const error = yield* makeLinearTestProvider().handle(signedLinearBody(body, undefined, 100_000)).pipe(Effect.flip)
			expect(error).toEqual(WebhookAuthenticationError.make({ reason: 'stale_timestamp' }))
		}),
	)
})
