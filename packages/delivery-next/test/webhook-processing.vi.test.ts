import { describe, it } from '@effect/vitest'
import { Effect } from 'effect'

import {
	DeliveryAdmission,
	DeliveryQueueRejected,
	DeliveryQueueUnavailable,
	ProviderWebhookEvent,
	ProviderWebhookIgnored,
	ProviderWebhookResponse,
	WebhookAuthenticationError,
	WebhookPayloadInvalidError,
} from '../src'
import { makeWebhookTestApp } from './mocks'

const admission = DeliveryAdmission.make({
	namespace: 'test',
	provider: 'example',
	installationId: 'installation',
	resourceId: 'resource',
	eventId: 'event',
	payload: { type: 'example' },
})

describe('ProviderWebhook routing tests', () => {
	it.effect('Provider-ignored messages return a 200', ({ expect }) =>
		Effect.gen(function* () {
			const app = yield* makeWebhookTestApp(() => Effect.succeed(ProviderWebhookIgnored.make({})))

			const response = yield* app.post()

			expect(response.status).toBe(200)
		}),
	)

	it.effect('Provider-controlled responses preserve the status, body, and headers', ({ expect }) =>
		Effect.gen(function* () {
			const app = yield* makeWebhookTestApp(() =>
				Effect.succeed(
					ProviderWebhookResponse.make({
						status: 202,
						body: new TextEncoder().encode('challenge'),
						headers: { 'x-provider-response': 'yes' },
					}),
				),
			)
			const response = yield* app.post()

			expect(response.status).toBe(202)
			expect(response.headers.get('x-provider-response')).toBe('yes')
			expect(yield* Effect.promise(() => response.text())).toBe('challenge')
		}),
	)

	it.effect('Admitted events return a 200 after the queue accepts them', ({ expect }) =>
		Effect.gen(function* () {
			const app = yield* makeWebhookTestApp(() => Effect.succeed(ProviderWebhookEvent.make({ admission })))

			const response = yield* app.post()

			expect(response.status).toBe(200)
		}),
	)

	it.effect('Authentication errors return a 401', ({ expect }) =>
		Effect.gen(function* () {
			const app = yield* makeWebhookTestApp(() =>
				Effect.fail(new WebhookAuthenticationError({ reason: 'test authentication failure' })),
			)
			const response = yield* app.post()

			expect(response.status).toBe(401)
			expect(yield* Effect.promise(() => response.text())).toBe('Unauthorized')
		}),
	)

	it.effect('Invalid payload errors return a 400', ({ expect }) =>
		Effect.gen(function* () {
			const app = yield* makeWebhookTestApp(() =>
				Effect.fail(new WebhookPayloadInvalidError({ reason: 'test payload failure' })),
			)
			const response = yield* app.post()

			expect(response.status).toBe(400)
			expect(yield* Effect.promise(() => response.text())).toBe('Invalid webhook payload')
		}),
	)

	it.effect('Unavailable delivery queues return a 503', ({ expect }) =>
		Effect.gen(function* () {
			const app = yield* makeWebhookTestApp(() => Effect.succeed(ProviderWebhookEvent.make({ admission })), {
				enqueue: () => Effect.fail(new DeliveryQueueUnavailable({ reason: 'test queue failure' })),
			})
			const response = yield* app.post()

			expect(response.status).toBe(503)
			expect(yield* Effect.promise(() => response.text())).toBe('Webhook admission unavailable')
		}),
	)

	it.effect('Rejected delivery admissions return a 503', ({ expect }) =>
		Effect.gen(function* () {
			const app = yield* makeWebhookTestApp(() => Effect.succeed(ProviderWebhookEvent.make({ admission })), {
				enqueue: () => Effect.fail(new DeliveryQueueRejected({ reason: 'test queue rejection' })),
			})
			const response = yield* app.post()

			expect(response.status).toBe(503)
			expect(yield* Effect.promise(() => response.text())).toBe('Webhook admission rejected')
		}),
	)

	it.effect('Unknown providers return a 404', ({ expect }) =>
		Effect.gen(function* () {
			const app = yield* makeWebhookTestApp(() => Effect.succeed(ProviderWebhookIgnored.make({})))

			const response = yield* app.post('unknown')

			expect(response.status).toBe(404)
		}),
	)
})
