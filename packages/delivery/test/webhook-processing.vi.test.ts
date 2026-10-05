import { describe, it } from '@effect/vitest'
import { Effect, Ref } from 'effect'

import {
	DeliveryAdmission,
	DeliveryReceipt,
	MailboxDeliveryRejected,
	MailboxDeliveryUnavailable,
	ProviderWebhookEvent,
	ProviderWebhookEvents,
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
			const app = yield* makeWebhookTestApp(() => Effect.succeed(ProviderWebhookEvent.make({ event: admission })))

			const response = yield* app.post()

			expect(response.status).toBe(200)
		}),
	)

	it.effect('Plural admissions are delivered in order before returning 200', ({ expect }) =>
		Effect.gen(function* () {
			const delivered = yield* Ref.make<ReadonlyArray<string>>([])
			const second = DeliveryAdmission.make({ ...admission, resourceId: 'resource-2', eventId: 'event-2' })
			const app = yield* makeWebhookTestApp(
				() => Effect.succeed(ProviderWebhookEvents.make({ events: [admission, second] })),
				{
					deliver: (event) =>
						Ref.update(delivered, (eventIds) => [...eventIds, event.eventId]).pipe(
							Effect.as(DeliveryReceipt.make({ mailboxKey: event.resourceId, accepted: true })),
						),
				},
			)

			const response = yield* app.post()

			expect(response.status).toBe(200)
			expect(yield* Ref.get(delivered)).toEqual(['event', 'event-2'])
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

	it.effect('Unavailable mailbox delivery returns a 503', ({ expect }) =>
		Effect.gen(function* () {
			const app = yield* makeWebhookTestApp(
				() => Effect.succeed(ProviderWebhookEvent.make({ event: admission })),
				{
					deliver: () => Effect.fail(new MailboxDeliveryUnavailable({ reason: 'test delivery failure' })),
				},
			)
			const response = yield* app.post()

			expect(response.status).toBe(503)
			expect(yield* Effect.promise(() => response.text())).toBe('Webhook admission unavailable')
		}),
	)

	it.effect('Rejected delivery admissions return a 503', ({ expect }) =>
		Effect.gen(function* () {
			const app = yield* makeWebhookTestApp(
				() => Effect.succeed(ProviderWebhookEvent.make({ event: admission })),
				{
					deliver: () => Effect.fail(new MailboxDeliveryRejected({ reason: 'test queue rejection' })),
				},
			)
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

	it.effect('Provider body limits return 413 before invoking the provider', ({ expect }) =>
		Effect.gen(function* () {
			const called = yield* Ref.make(false)
			const app = yield* makeWebhookTestApp(
				() => Ref.set(called, true).pipe(Effect.as(ProviderWebhookIgnored.make({}))),
				undefined,
				4,
			)
			const response = yield* app.post('example', '12345')
			expect(response.status).toBe(413)
			expect(yield* Ref.get(called)).toBe(false)
		}),
	)
})
