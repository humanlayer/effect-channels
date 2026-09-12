import { assert, it } from '@effect/vitest'
import { Deferred, Effect, Exit, Fiber, Layer, Match, Queue, Schema } from 'effect'
import { TestClock } from 'effect/testing'

import { bind, DeliveryError, HandlerFailure } from '../src/Delivery.js'
import { DeliveryControl, DeliveryNotFound, DeliveryOutcomeConflict } from '../src/DeliveryControl.js'
import { DeliveryPolicy } from '../src/DeliveryPolicy.js'
import { resolveDeliveryFor } from '../src/DeliveryResolution.js'
import type { EventDefinition } from '../src/EventDefinition.js'
import { activeBatches, MailboxState } from '../src/Mailbox.js'
import { MailboxStore } from '../src/MailboxStore.js'
import { layer as memoryLayer } from '../src/memory.js'
import { DELIVERY_ID_MAX_LENGTH, DeliveryId } from '../src/protocol.js'

const Event = Schema.Struct({ id: Schema.String, installation: Schema.String, resource: Schema.String })
const definition: EventDefinition<typeof Event, typeof Schema.String> = {
	name: 'test.remote',
	version: '1',
	provider: 'test',
	event: Event,
	resource: Schema.String,
	identify: (event) => ({ eventId: event.id, installation: event.installation, resource: event.resource }),
	resourceKey: (resource) => resource,
}
const policy = DeliveryPolicy.make({
	mode: 'queue',
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
const event = (id: string) => Event.make({ id, installation: 'one', resource: 'thread' })
const memory = memoryLayer({ maxMailboxes: 20 })
const services = DeliveryControl.layer.pipe(Layer.provideMerge(memory))

it.effect('keeps a handed-off delivery in flight across lease expiry and advances queued work on completion', () =>
	Effect.gen(function* () {
		const starts = yield* Queue.unbounded<string>()
		const ids = yield* Queue.unbounded<string>()
		const delivery = bind({
			namespace: 'remote',
			handlerId: 'investigate',
			definition,
			policy,
			handler: (input, context) =>
				Queue.offer(starts, input.id).pipe(
					Effect.andThen(
						input.id === 'A'
							? Queue.offer(ids, context.deliveryId).pipe(Effect.andThen(context.handoff()))
							: Effect.void,
					),
				),
		})
		const receipt = yield* delivery.admit({ event: event('A'), organizationId: 'org-one' })
		assert.strictEqual(yield* delivery.processMailbox(receipt), true)
		const deliveryId = yield* Queue.take(ids)
		yield* delivery.admit({ event: event('B'), organizationId: 'org-one' })
		assert.strictEqual(yield* delivery.processMailbox(receipt), false)
		yield* TestClock.adjust(2000)
		assert.strictEqual(yield* delivery.processMailbox(receipt), false)
		assert.deepStrictEqual(yield* Queue.takeAll(starts), ['A'])

		const control = yield* DeliveryControl
		const resolved = yield* control.resolve({ deliveryId })
		assert.strictEqual(resolved.installation, 'one')
		assert.strictEqual(resolved.eventId, 'A')
		const native = yield* resolveDeliveryFor(resolved, definition)
		assert.deepStrictEqual(native.event, event('A'))
		assert.strictEqual(native.resource, 'thread')
		const accepted = yield* control.finish({ deliveryId, outcome: 'completed' })
		assert.strictEqual(accepted.status, 'accepted')
		assert.strictEqual((yield* control.finish({ deliveryId, outcome: 'completed' })).status, 'already_recorded')
		const conflict = yield* Effect.flip(control.finish({ deliveryId, outcome: 'failed' }))
		assert.strictEqual(Schema.is(DeliveryOutcomeConflict)(conflict), true)
		assert.strictEqual(yield* delivery.processMailbox(receipt), true)
		assert.strictEqual(yield* delivery.processMailbox(receipt), true)
		assert.deepStrictEqual(yield* Queue.takeAll(starts), ['B'])
		assert.strictEqual((yield* control.finish({ deliveryId, outcome: 'completed' })).status, 'already_recorded')
		assert.strictEqual(
			Schema.is(DeliveryOutcomeConflict)(yield* Effect.flip(control.finish({ deliveryId, outcome: 'failed' }))),
			true,
		)
		const snapshot = yield* (yield* MailboxStore).loadMailbox(receipt)
		assert(snapshot !== undefined)
		assert.strictEqual(activeBatches(snapshot.state).length, 0)
		const retainedNative = yield* resolveDeliveryFor(yield* control.resolve({ deliveryId }), definition)
		assert.strictEqual(retainedNative.event.id, 'A')
	}).pipe(Effect.provide(services)),
)

for (const mode of ['queue', 'debounce', 'burst'] as const) {
	it.effect(`${mode} handoff resolves the canonical event rather than its skipped envelope`, () =>
		Effect.gen(function* () {
			const ids = yield* Queue.unbounded<string>()
			const selected = yield* Queue.unbounded<ReadonlyArray<string>>()
			const modePolicy: DeliveryPolicy = Match.value(mode).pipe(
				Match.when('queue', () => policy),
				Match.when('debounce', (selectedMode) =>
					DeliveryPolicy.make({ ...policy, mode: selectedMode, quietPeriodMs: 100 }),
				),
				Match.when('burst', (selectedMode) =>
					DeliveryPolicy.make({ ...policy, mode: selectedMode, windowMs: 100 }),
				),
				Match.exhaustive,
			)
			const delivery = bind({
				namespace: `canonical-${mode}`,
				handlerId: 'investigate',
				definition,
				policy: modePolicy,
				handler: (input, context) =>
					Queue.offer(selected, [...context.skipped.map((entry) => entry.id), input.id]).pipe(
						Effect.andThen(Queue.offer(ids, context.deliveryId)),
						Effect.andThen(context.handoff()),
					),
			})
			const first = Event.make({ id: 'A', installation: 'one', resource: 'thread' })
			const second = Event.make({ id: 'B', installation: 'one', resource: 'thread' })
			const receipt = yield* delivery.admit({ event: first, organizationId: 'org-one' })
			yield* delivery.admit({ event: second, organizationId: 'org-one' })
			if (mode !== 'queue') yield* TestClock.adjust(100)
			assert.strictEqual(yield* delivery.processMailbox(receipt), true)
			assert.deepStrictEqual(yield* Queue.take(selected), ['A', 'B'])
			const deliveryId = yield* Queue.take(ids)
			const control = yield* DeliveryControl
			const active = yield* resolveDeliveryFor(yield* control.resolve({ deliveryId }), definition)
			assert.deepStrictEqual(active.event, second)
			assert.strictEqual(active.resource, 'thread')
			yield* control.finish({ deliveryId, outcome: 'completed' })
			assert.strictEqual(yield* delivery.processMailbox(receipt), true)
			const retained = yield* resolveDeliveryFor(yield* control.resolve({ deliveryId }), definition)
			assert.deepStrictEqual(retained.event, second)
			assert.strictEqual(retained.resource, 'thread')
			const state = yield* (yield* MailboxStore).loadMailbox(receipt)
			assert.deepStrictEqual(
				state?.state.outcomes.map((outcome) => [outcome.eventId, outcome.deliveryId]),
				[
					[undefined, undefined],
					['B', deliveryId],
				],
			)
		}).pipe(Effect.provide(services)),
	)
}

it.effect('rejects oversized delivery identifiers before locator decoding', () =>
	Effect.gen(function* () {
		const oversized = `delivery:v1:${'a'.repeat(DELIVERY_ID_MAX_LENGTH)}:eA`
		const result = yield* Effect.exit(Schema.decodeEffect(DeliveryId)(oversized))
		assert(Exit.isFailure(result))
	}),
)

it.effect('rejects an oversized generated locator during admission without mutating the mailbox', () =>
	Effect.gen(function* () {
		let invoked = false
		const delivery = bind({
			namespace: 'n'.repeat(DELIVERY_ID_MAX_LENGTH),
			handlerId: 'oversized-locator',
			definition,
			policy: DeliveryPolicy.make({ ...policy, maxPayloadBytes: 100_000 }),
			handler: () =>
				Effect.sync(() => {
					invoked = true
				}),
		})
		const input = { event: event('A'), organizationId: 'org-one' }
		const key = yield* delivery.keyFor({ event: input.event })
		const failure = yield* Effect.flip(delivery.admit(input))
		assert.deepStrictEqual(failure, DeliveryError.make({ reason: 'capacity' }))
		assert.strictEqual(yield* (yield* MailboxStore).loadMailbox({ key }), undefined)
		assert.strictEqual(invoked, false)
	}).pipe(Effect.provide(memory)),
)

it.effect('claims v1-v3 work with oversized legacy keys through bounded atomic locators', () =>
	Effect.gen(function* () {
		const ids = yield* Queue.unbounded<string>()
		const delivery = bind({
			namespace: 'legacy'.repeat(400),
			handlerId: 'oversized-reconstruction',
			definition,
			policy: DeliveryPolicy.make({ ...policy, maxPayloadBytes: 100_000 }),
			handler: (_input, context) => Queue.offer(ids, context.deliveryId).pipe(Effect.andThen(context.handoff())),
		})
		const store = yield* MailboxStore
		const control = yield* DeliveryControl
		for (const legacy of [
			{ version: 1, retryable: false },
			{ version: 1, retryable: true },
			{ version: 2, retryable: false },
			{ version: 2, retryable: true },
			{ version: 3, retryable: false },
			{ version: 3, retryable: true },
		] as const) {
			const kind = legacy.retryable ? 'retryable' : 'pending'
			const value = { ...event(`legacy-${legacy.version}-${kind}`), resource: `thread-${legacy.version}-${kind}` }
			const key = yield* delivery.keyFor({ event: value })
			const envelope = {
				definition: definition.name,
				version: definition.version,
				eventId: value.id,
				resource: `"${value.resource}"`,
				payload: `{"id":"${value.id}","installation":"${value.installation}","resource":"${value.resource}"}`,
				acceptedAt: 0,
				organizationId: 'org-one',
			}
			const batch = {
				envelopes: [envelope],
				attempt: 1,
				owner: null,
				leaseUntil: 0,
				cancelled: false,
			}
			const common = {
				pending: legacy.retryable ? [] : [envelope],
				active: legacy.retryable ? batch : null,
				failed: [],
				outcomes: [],
				readyAt: 0,
			}
			const encodedState =
				legacy.version === 1
					? { ...common, version: 1 }
					: {
							...common,
							version: legacy.version,
							additionalActive: [],
							pendingReadyAt: legacy.retryable ? null : 0,
							burstDraining: false,
						}
			const state = yield* Schema.decodeUnknownEffect(MailboxState)(encodedState)
			assert.strictEqual(
				yield* store.commitMailbox({ key, expectedRevision: null, nextState: state }),
				'committed',
			)
			assert.strictEqual(yield* delivery.processMailbox({ key }), true)
			const deliveryId = yield* Queue.take(ids)
			assert.match(deliveryId, /^delivery:v2:/)
			assert(deliveryId.length <= DELIVERY_ID_MAX_LENGTH)
			assert.strictEqual((yield* control.resolve({ deliveryId })).eventId, value.id)
		}
	}).pipe(Effect.provide(services)),
)

it.effect('rejects noncanonical delivery IDs at persisted active and outcome boundaries', () =>
	Effect.gen(function* () {
		const invalidState = {
			version: 4,
			pending: [],
			active: {
				envelopes: [
					{
						definition: 'test.remote',
						version: '1',
						eventId: 'A',
						resource: '"thread"',
						payload: '{}',
						acceptedAt: 0,
					},
				],
				attempt: 1,
				owner: null,
				leaseUntil: 0,
				cancelled: false,
				deliveryId: 'not-a-delivery-id',
			},
			additionalActive: [],
			pendingReadyAt: null,
			burstDraining: false,
			failed: [],
			outcomes: [{ identity: 'A', kind: 'completed', expiresAt: 1, deliveryId: 'not-a-delivery-id' }],
			readyAt: null,
		}
		assert(Exit.isFailure(yield* Effect.exit(Schema.decodeUnknownEffect(MailboxState)(invalidState))))
	}),
)

it.effect('preserves an early terminal result through handoff and delayed local cleanup', () =>
	Effect.gen(function* () {
		const started = yield* Deferred.make<string>()
		const release = yield* Deferred.make<void>()
		const delivery = bind({
			namespace: 'early',
			handlerId: 'investigate',
			definition,
			policy,
			handler: (_input, context) =>
				Deferred.succeed(started, context.deliveryId).pipe(
					Effect.andThen(Deferred.await(release)),
					Effect.andThen(context.handoff()),
				),
		})
		const receipt = yield* delivery.admit({ event: event('A'), organizationId: 'org-one' })
		const fiber = yield* delivery.processMailbox(receipt).pipe(Effect.forkChild)
		const deliveryId = yield* Deferred.await(started)
		const control = yield* DeliveryControl
		yield* control.finish({ deliveryId, outcome: 'failed' })
		const whileRunning = yield* (yield* MailboxStore).loadMailbox(receipt)
		assert(whileRunning !== undefined)
		assert.strictEqual(activeBatches(whileRunning.state).length, 1)
		yield* Deferred.succeed(release, undefined)
		yield* Fiber.join(fiber)
		const completed = yield* (yield* MailboxStore).loadMailbox(receipt)
		assert.strictEqual(completed?.state.active, null)
		assert.strictEqual(completed?.state.outcomes[0]?.kind, 'failed')
	}).pipe(Effect.provide(services)),
)

it.effect('keeps the durable handoff when local code fails afterward', () =>
	Effect.gen(function* () {
		const ids = yield* Queue.unbounded<string>()
		const delivery = bind({
			namespace: 'post-handoff-failure',
			handlerId: 'investigate',
			definition,
			policy,
			handler: (_input, context) =>
				context.handoff().pipe(
					Effect.tap(() => Queue.offer(ids, context.deliveryId)),
					Effect.andThen(Effect.fail(HandlerFailure.make({ retryable: true }))),
				),
		})
		const receipt = yield* delivery.admit({ event: event('A'), organizationId: 'org-one' })
		assert.strictEqual(yield* delivery.processMailbox(receipt), true)
		const deliveryId = yield* Queue.take(ids)
		yield* TestClock.adjust(2000)
		assert.strictEqual(yield* delivery.processMailbox(receipt), false)
		assert.strictEqual((yield* (yield* MailboxStore).loadMailbox(receipt))?.state.active?.attempt, 1)
		yield* (yield* DeliveryControl).finish({ deliveryId, outcome: 'completed' })
		assert.strictEqual(yield* delivery.processMailbox(receipt), true)
	}).pipe(Effect.provide(services)),
)

it.effect('treats physically retained terminal outcomes as absent after expiry', () =>
	Effect.gen(function* () {
		const ids = yield* Queue.unbounded<string>()
		const delivery = bind({
			namespace: 'expired-outcome',
			handlerId: 'investigate',
			definition,
			policy,
			handler: (_input, context) => Queue.offer(ids, context.deliveryId).pipe(Effect.andThen(context.handoff())),
		})
		const receipt = yield* delivery.admit({ event: event('A'), organizationId: 'org-one' })
		assert.strictEqual(yield* delivery.processMailbox(receipt), true)
		const deliveryId = yield* Queue.take(ids)
		const control = yield* DeliveryControl
		yield* control.finish({ deliveryId, outcome: 'completed' })
		assert.strictEqual(yield* delivery.processMailbox(receipt), true)
		const retained = yield* (yield* MailboxStore).loadMailbox(receipt)
		assert.strictEqual(
			retained?.state.outcomes.some((outcome) => outcome.deliveryId === deliveryId),
			true,
		)
		yield* TestClock.adjust(policy.retentionMs)
		assert(Schema.is(DeliveryNotFound)(yield* Effect.flip(control.resolve({ deliveryId }))))
		assert(Schema.is(DeliveryNotFound)(yield* Effect.flip(control.finish({ deliveryId, outcome: 'completed' }))))
		const unpruned = yield* (yield* MailboxStore).loadMailbox(receipt)
		assert.strictEqual(
			unpruned?.state.outcomes.some((outcome) => outcome.deliveryId === deliveryId),
			true,
		)
	}).pipe(Effect.provide(services)),
)
