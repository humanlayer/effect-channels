import { assert, it } from '@effect/vitest'
import { Context, Effect, FileSystem, Layer, Path, Schema } from 'effect'
import { Etag, FetchHttpClient, HttpPlatform, HttpRouter } from 'effect/unstable/http'
import { HttpApiBuilder, HttpApiTest } from 'effect/unstable/httpapi'

import { makeMountedDeliveryClient } from '../src/client.js'
import { DeliveryContract, Forbidden, Unauthorized, Unavailable } from '../src/contract.js'
import { bind } from '../src/Delivery.js'
import { DeliveryControl } from '../src/DeliveryControl.js'
import { DeliveryPolicy } from '../src/DeliveryPolicy.js'
import { resolveDeliveryFor } from '../src/DeliveryResolution.js'
import type { EventDefinition } from '../src/EventDefinition.js'
import { MailboxStore } from '../src/MailboxStore.js'
import { layer as memoryLayer } from '../src/memory.js'
import { deliveryNotFound, DeliveryNotFound, DeliveryOutcomeConflict } from '../src/protocol.js'
import { deliveryApiServerLayer } from '../src/server.js'

class Calls extends Context.Service<Calls, Array<string>>()('test/Calls') {}

const Event = Schema.Struct({ id: Schema.String, installation: Schema.String, resource: Schema.String })
const definition: EventDefinition<typeof Event, typeof Schema.String> = {
	name: 'test.http',
	version: '1',
	provider: 'test',
	event: Event,
	resource: Schema.String,
	identify: (event) => ({ eventId: event.id, installation: event.installation, resource: event.resource }),
	resourceKey: (resource) => resource,
}
const policy = DeliveryPolicy.make({
	mode: 'serial',
	maxPayloadBytes: 4096,
	maxEnvelopes: 16,
	maxOutcomes: 64,
	retentionMs: 60_000,
	maxAttempts: 3,
	retryBaseMs: 100,
	retryMaxMs: 1000,
	leaseMs: 1000,
	heartbeatMs: 100,
	conflictRetries: 8,
})
const base = Layer.merge(
	DeliveryControl.layer.pipe(Layer.provideMerge(memoryLayer({ maxMailboxes: 20 }))),
	Layer.sync(Calls, () => []),
)
const testPlatform = Layer.mergeAll(
	HttpPlatform.layer.pipe(Layer.provideMerge(FileSystem.layerNoop({}))),
	Path.layer,
	Etag.layer,
)

const makeHandoff = Effect.fn('test.delivery.handoff')(function* (id: string) {
	const delivery = bind({
		namespace: 'h',
		handlerId: 'h',
		definition,
		policy,
		handler: (_event, context) => context.handoff(),
	})
	const receipt = yield* delivery.admit({
		event: Event.make({ id: id.slice(0, 1), installation: 'i', resource: 'r' }),
		organizationId: 'org-one',
	})
	yield* delivery.processMailbox(receipt)
	const stored = yield* (yield* MailboxStore).loadMailbox(receipt)
	assert(stored?.state.active?.deliveryId !== undefined)
	return stored.state.active.deliveryId
})

it.effect('runs with neither context nor middleware', () =>
	Effect.gen(function* () {
		const deliveryId = yield* makeHandoff('neither')
		const client = yield* HttpApiTest.groups(DeliveryContract.prefix('/hooks'), ['deliveries'])
		assert.strictEqual(
			(yield* client.deliveries.complete({ params: { deliveryId }, payload: {} })).status,
			'accepted',
		)
	}).pipe(
		Effect.provide(
			Layer.merge(deliveryApiServerLayer({ mountPath: '/hooks' }).pipe(Layer.provideMerge(base)), testPlatform),
		),
	),
)

it.effect('runs delivery-aware context without middleware', () =>
	Effect.gen(function* () {
		const deliveryId = yield* makeHandoff('context-only')
		const client = yield* HttpApiTest.groups(DeliveryContract.prefix('/hooks'), ['deliveries'])
		assert.strictEqual(
			(yield* client.deliveries.complete({ params: { deliveryId }, payload: {} })).status,
			'accepted',
		)
		assert.deepStrictEqual(yield* Calls, ['context-only:i'])
	}).pipe(
		Effect.provide(
			Layer.merge(
				deliveryApiServerLayer({
					mountPath: '/hooks',
					context: ({ delivery }) =>
						Effect.gen(function* () {
							assert.strictEqual(delivery.organizationId, 'org-one')
							const calls = yield* Calls
							calls.push(`context-only:${delivery.installation}`)
						}),
				}).pipe(Layer.provideMerge(base)),
				testPlatform,
			),
		),
	),
)

it.effect('lets context-only authorization hide the wrong organization before mutation', () =>
	Effect.gen(function* () {
		const deliveryId = yield* makeHandoff('context-hidden')
		const client = yield* HttpApiTest.groups(DeliveryContract.prefix('/hooks'), ['deliveries'])
		const hidden = yield* Effect.flip(client.deliveries.complete({ params: { deliveryId }, payload: {} }))
		assert(Schema.is(DeliveryNotFound)(hidden))
		const unknown = yield* Effect.flip(
			client.deliveries.complete({ params: { deliveryId: 'delivery:v1:bm90LWZvdW5k:eA' }, payload: {} }),
		)
		assert(Schema.is(DeliveryNotFound)(unknown))
		assert.deepStrictEqual(yield* Calls, ['org-one'])
		const resolved = yield* (yield* DeliveryControl).resolve({ deliveryId })
		assert.strictEqual(resolved.organizationId, 'org-one')
	}).pipe(
		Effect.provide(
			Layer.merge(
				deliveryApiServerLayer({
					mountPath: '/hooks',
					context: ({ delivery }) =>
						Calls.pipe(
							Effect.tap((calls) => Effect.sync(() => calls.push(delivery.organizationId))),
							Effect.andThen(Effect.fail(deliveryNotFound(delivery.deliveryId))),
						),
				}).pipe(Layer.provideMerge(base)),
				testPlatform,
			),
		),
	),
)

it.effect('runs middleware without context and supplies null', () =>
	Effect.gen(function* () {
		const deliveryId = yield* makeHandoff('middleware-only')
		const client = yield* HttpApiTest.groups(DeliveryContract.prefix('/hooks'), ['deliveries'])
		assert.strictEqual((yield* client.deliveries.fail({ params: { deliveryId }, payload: {} })).outcome, 'failed')
		assert.deepStrictEqual(yield* Calls, ['middleware-only'])
	}).pipe(
		Effect.provide(
			Layer.merge(
				deliveryApiServerLayer({
					mountPath: '/hooks',
					middleware: {
						fail: ({ context, next }) =>
							Effect.gen(function* () {
								assert.strictEqual(context, null)
								const calls = yield* Calls
								calls.push('middleware-only')
								return yield* next()
							}),
					},
				}).pipe(Layer.provideMerge(base)),
				testPlatform,
			),
		),
	),
)

it.effect('runs context then endpoint middleware through the generated client', () =>
	Effect.gen(function* () {
		const calls = yield* Calls
		const delivery = bind({
			namespace: 'http',
			handlerId: 'handler',
			definition,
			policy,
			handler: (_event, context) => context.handoff(),
		})
		const receipt = yield* delivery.admit({
			event: Event.make({ id: 'A', installation: 'one', resource: 'thread' }),
			organizationId: 'org-one',
		})
		yield* delivery.processMailbox(receipt)
		const stored = yield* (yield* MailboxStore).loadMailbox(receipt)
		assert(stored?.state.active?.deliveryId !== undefined)
		const snapshot = yield* (yield* DeliveryControl).resolve({ deliveryId: stored.state.active.deliveryId })
		const api = DeliveryContract.prefix('/control')
		const runtime = Layer.mergeAll(
			Layer.succeed(DeliveryControl, yield* DeliveryControl),
			Layer.succeed(MailboxStore, yield* MailboxStore),
			Layer.succeed(Calls, calls),
		)
		const routes = HttpApiBuilder.layer(api).pipe(
			Layer.provide(
				deliveryApiServerLayer({
					mountPath: '/control',
					context: ({ request, delivery }) =>
						Effect.gen(function* () {
							const recorded = yield* Calls
							const authorization = request.headers.authorization
							if (authorization === undefined)
								return yield* Unauthorized.make({ message: 'Authentication required.' })
							if (authorization === 'forbidden') return yield* Forbidden.make({ message: 'Denied.' })
							if (authorization === 'unavailable')
								return yield* Unavailable.make({ message: 'Authentication unavailable.' })
							const native = yield* resolveDeliveryFor(delivery, definition).pipe(
								Effect.mapError(() =>
									Unavailable.make({ message: 'Delivery metadata is unavailable.' }),
								),
							)
							assert.strictEqual(native.event.installation, delivery.installation)
							if (authorization === 'wrong-organization' && native.organizationId !== 'org-two')
								return yield* deliveryNotFound(delivery.deliveryId)
							recorded.push('context')
							return { actor: authorization }
						}),
					middleware: {
						complete: ({ context, delivery, input, next }) =>
							context.actor === 'allowed'
								? Effect.gen(function* () {
										const recorded = yield* Calls
										const accepted = yield* next()
										const replay = yield* next()
										assert.deepStrictEqual(replay, accepted)
										recorded.push(`middleware:${delivery.organizationId}`)
										recorded.push('after')
										return accepted
									})
								: Effect.fail(
										DeliveryNotFound.make({
											deliveryId: input.deliveryId,
											message: 'The requested delivery was not found.',
										}),
									),
						fail: ({ request, next }) =>
							request.headers['x-fail'] === 'unavailable'
								? Effect.fail(Unavailable.make({ message: 'Fail policy unavailable.' }))
								: next(),
					},
				}),
			),
			Layer.provide(testPlatform),
			Layer.provide(runtime),
		)
		const web = HttpRouter.toWebHandler(routes, { disableLogger: true })
		const fetch: typeof globalThis.fetch = (input, init) => web.handler(new Request(input, init))
		const unprefixed = yield* Effect.promise(() =>
			web.handler(
				new Request(`http://delivery.test/deliveries/${snapshot.deliveryId}/complete`, {
					method: 'POST',
					headers: { 'content-type': 'application/json' },
					body: '{}',
				}),
			),
		)
		assert.strictEqual(unprefixed.status, 404)
		const results = yield* Effect.gen(function* () {
			const anonymous = yield* makeMountedDeliveryClient({
				baseUrl: 'http://delivery.test',
				mountPath: '/control',
			})
			const unauthorized = yield* Effect.flip(
				anonymous.deliveries.complete({ params: { deliveryId: snapshot.deliveryId }, payload: {} }),
			)
			assert(Schema.is(Unauthorized)(unauthorized))
			const untouched = yield* (yield* MailboxStore).loadMailbox(receipt)
			assert.strictEqual(untouched?.state.active?.stage?.terminalOutcome, undefined)
			const denied = yield* makeMountedDeliveryClient({
				baseUrl: 'http://delivery.test',
				mountPath: '/control',
				headers: { authorization: 'forbidden' },
			})
			const forbidden = yield* Effect.flip(
				denied.deliveries.complete({ params: { deliveryId: snapshot.deliveryId }, payload: {} }),
			)
			const wrongOrganization = yield* makeMountedDeliveryClient({
				baseUrl: 'http://delivery.test',
				mountPath: '/control',
				headers: { authorization: 'wrong-organization' },
			})
			const hidden = yield* Effect.flip(
				wrongOrganization.deliveries.complete({ params: { deliveryId: snapshot.deliveryId }, payload: {} }),
			)
			assert(Schema.is(DeliveryNotFound)(hidden))
			const stillUntouched = yield* (yield* MailboxStore).loadMailbox(receipt)
			assert.strictEqual(stillUntouched?.state.active?.stage?.terminalOutcome, undefined)
			const client = yield* makeMountedDeliveryClient({
				baseUrl: 'http://delivery.test',
				mountPath: '/control',
				headers: { authorization: 'allowed' },
			})
			const accepted = yield* client.deliveries.complete({
				params: { deliveryId: snapshot.deliveryId },
				payload: {},
			})
			const replayed = yield* client.deliveries.complete({
				params: { deliveryId: snapshot.deliveryId },
				payload: {},
			})
			const conflict = yield* Effect.flip(
				client.deliveries.fail({ params: { deliveryId: snapshot.deliveryId }, payload: {} }),
			)
			const missing = yield* Effect.flip(
				client.deliveries.complete({
					params: { deliveryId: 'delivery:v1:bm90LWZvdW5k:eA' },
					payload: {},
				}),
			)
			const brokenAuth = yield* makeMountedDeliveryClient({
				baseUrl: 'http://delivery.test',
				mountPath: '/control',
				headers: { authorization: 'unavailable' },
			})
			const unavailable = yield* Effect.flip(
				brokenAuth.deliveries.complete({ params: { deliveryId: snapshot.deliveryId }, payload: {} }),
			)
			const brokenFail = yield* makeMountedDeliveryClient({
				baseUrl: 'http://delivery.test',
				mountPath: '/control',
				headers: { authorization: 'allowed', 'x-fail': 'unavailable' },
			})
			const failRejected = yield* Effect.flip(
				brokenFail.deliveries.fail({ params: { deliveryId: snapshot.deliveryId }, payload: {} }),
			)
			return { accepted, replayed, conflict, missing, unauthorized, forbidden, hidden, unavailable, failRejected }
		}).pipe(Effect.provide(FetchHttpClient.layer), Effect.provideService(FetchHttpClient.Fetch, fetch))
		assert.strictEqual(results.accepted.outcome, 'completed')
		assert.strictEqual(results.accepted.status, 'accepted')
		assert.strictEqual(results.replayed.status, 'already_recorded')
		assert(Schema.is(DeliveryOutcomeConflict)(results.conflict))
		assert.match(results.conflict.message, /completed.*failed/)
		assert(Schema.is(DeliveryNotFound)(results.missing))
		assert(Schema.is(Unauthorized)(results.unauthorized))
		assert(Schema.is(Forbidden)(results.forbidden))
		assert(Schema.is(DeliveryNotFound)(results.hidden))
		assert(Schema.is(Unavailable)(results.unavailable))
		assert(Schema.is(Unavailable)(results.failRejected))
		assert.deepStrictEqual(calls, [
			'context',
			'middleware:org-one',
			'after',
			'context',
			'middleware:org-one',
			'after',
			'context',
			'context',
		])
		yield* Effect.promise(() => web.dispose())
	}).pipe(Effect.provide(Layer.merge(base, testPlatform))),
)
