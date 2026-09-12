import { assert, it } from '@effect/vitest'
import { Clock, Deferred, Effect, Fiber, Layer, Match, Queue, Schema } from 'effect'
import { TestClock } from 'effect/testing'

import { bind, DeliveryError, HandlerFailure } from '../src/Delivery.js'
import { DeliveryPolicy } from '../src/DeliveryPolicy.js'
import { activeBatches, currentMailbox, MailboxSnapshot } from '../src/Mailbox.js'
import { MailboxReadiness, MailboxStore } from '../src/MailboxStore.js'
import { layer } from '../src/memory.js'

const Event = Schema.Struct({ id: Schema.String })
const definition = {
	name: 'mode.event',
	version: '1',
	provider: 'test',
	event: Event,
	resource: Schema.String,
	identify: (event: typeof Event.Type) => ({ eventId: event.id, installation: 'T1', resource: 'thread' }),
	resourceKey: (resource: string) => resource,
}
const limits = {
	maxPayloadBytes: 4096,
	maxEnvelopes: 16,
	maxOutcomes: 64,
	retentionMs: 60_000,
	maxAttempts: 3,
	retryBaseMs: 100,
	retryMaxMs: 1000,
	leaseMs: 1000,
	heartbeatMs: 100,
	conflictRetries: 16,
}
const policies: ReadonlyArray<DeliveryPolicy> = [
	{ ...limits, mode: 'queue' },
	{ ...limits, mode: 'burst', windowMs: 1500 },
	{ ...limits, mode: 'debounce', quietPeriodMs: 1500 },
]
const memory = layer({ maxMailboxes: 20 })
const registration = { namespace: 'modes', handlerId: 'handler', definition }
const arrival = (id: string) => ({ event: { id } })
const load = (key: string) =>
	Effect.flatMap(MailboxStore, (store) => store.loadMailbox({ key })).pipe(
		Effect.map((snapshot) => {
			assert.ok(snapshot !== undefined)
			return currentMailbox(snapshot.state)
		}),
	)

for (const policy of policies) {
	it.effect(`${policy.mode}: persists its deadline, ignores duplicates and drains with skipped context`, () =>
		Effect.gen(function* () {
			const calls = yield* Queue.unbounded<ReadonlyArray<string>>()
			const gate = yield* Deferred.make<void>()
			const handler = (
				event: typeof Event.Type,
				context: { readonly skipped: ReadonlyArray<typeof Event.Type> },
			) =>
				Queue.offer(calls, [...context.skipped.map((entry) => entry.id), event.id]).pipe(
					Effect.andThen(Deferred.await(gate)),
				)
			const delivery = bind({ ...registration, policy, handler })
			const { key } = yield* delivery.admit(arrival('A'))
			const deadlineAfterB = Match.value(policy.mode).pipe(
				Match.when('debounce', () => 2500),
				Match.when('burst', () => 1500),
				Match.orElse(() => 0),
			)
			assert.strictEqual((yield* load(key)).readyAt, policy.mode === 'queue' ? 0 : 1500)
			yield* TestClock.adjust(1000)
			yield* delivery.admit(arrival('B'))
			assert.strictEqual((yield* load(key)).readyAt, deadlineAfterB)
			yield* TestClock.adjust(400)
			assert.strictEqual((yield* delivery.admit(arrival('B'))).accepted, false)
			assert.strictEqual((yield* load(key)).readyAt, deadlineAfterB)
			const replacement = bind({ ...registration, policy, handler })
			if (policy.mode !== 'queue') {
				assert.strictEqual(yield* replacement.processMailbox({ key }), false)
				yield* TestClock.adjust(policy.mode === 'burst' ? 100 : 1100)
			}
			const active = yield* replacement.processMailbox({ key }).pipe(Effect.forkChild)
			assert.deepStrictEqual(yield* Queue.take(calls), ['A', 'B'])
			yield* replacement.admit(arrival('C'))
			yield* TestClock.adjust(500)
			yield* replacement.admit(arrival('D'))
			yield* Deferred.succeed(gate, undefined)
			yield* Fiber.join(active)
			if (policy.mode === 'debounce') {
				assert.strictEqual(yield* replacement.processMailbox({ key }), false)
				yield* TestClock.adjust(1499)
				assert.strictEqual(yield* replacement.processMailbox({ key }), false)
				yield* TestClock.adjust(1)
			}
			assert.strictEqual(yield* replacement.processMailbox({ key }), true)
			assert.deepStrictEqual(yield* Queue.take(calls), ['C', 'D'])
			assert.strictEqual((yield* load(key)).readyAt, null)
			yield* replacement.admit(arrival('E'))
			const now = yield* Clock.currentTimeMillis
			assert.strictEqual((yield* load(key)).readyAt, now + (policy.mode === 'queue' ? 0 : 1500))
		}).pipe(Effect.provide(memory)),
	)
}

it.effect('debounce waits for active completion even after the quiet period expires', () =>
	Effect.gen(function* () {
		const calls = yield* Queue.unbounded<string>()
		const gate = yield* Deferred.make<void>()
		const delivery = bind({
			...registration,
			policy: { ...limits, mode: 'debounce', quietPeriodMs: 200 },
			handler: (event) => Queue.offer(calls, event.id).pipe(Effect.andThen(Deferred.await(gate))),
		})
		const receipt = yield* delivery.admit(arrival('A'))
		yield* TestClock.adjust(200)
		const active = yield* delivery.processMailbox(receipt).pipe(Effect.forkChild)
		assert.strictEqual(yield* Queue.take(calls), 'A')
		yield* delivery.admit(arrival('B'))
		yield* TestClock.adjust(500)
		assert.strictEqual(yield* delivery.processMailbox(receipt), false)
		yield* Deferred.succeed(gate, undefined)
		yield* Fiber.join(active)
		assert.strictEqual(yield* delivery.processMailbox(receipt), true)
		assert.strictEqual(yield* Queue.take(calls), 'B')
	}).pipe(Effect.provide(memory)),
)

it.effect('continuous new debounce arrivals postpone indefinitely; burst does not move its initial deadline', () =>
	Effect.gen(function* () {
		for (const mode of ['debounce', 'burst'] as const) {
			const policy: DeliveryPolicy =
				mode === 'debounce' ? { ...limits, mode, quietPeriodMs: 200 } : { ...limits, mode, windowMs: 200 }
			const delivery = bind({ ...registration, handlerId: mode, policy, handler: () => Effect.void })
			const receipt = yield* delivery.admit(arrival('0'))
			const start = yield* Clock.currentTimeMillis
			for (let i = 1; i <= 5; i++) {
				yield* TestClock.adjust(100)
				yield* delivery.admit(arrival(String(i)))
				assert.strictEqual(
					(yield* load(receipt.key)).pendingReadyAt,
					start + (mode === 'debounce' ? i * 100 + 200 : 200),
				)
				if (mode === 'debounce') assert.strictEqual(yield* delivery.processMailbox(receipt), false)
			}
		}
	}).pipe(Effect.provide(memory)),
)

it.effect('drop records durable outcomes while ready, active and retrying, with bounded dedupe', () =>
	Effect.gen(function* () {
		const started = yield* Deferred.make<void>()
		const gate = yield* Deferred.make<void>()
		const delivery = bind({
			...registration,
			policy: { ...limits, mode: 'drop', maxEnvelopes: 1, maxOutcomes: 4 },
			handler: () =>
				Deferred.succeed(started, undefined).pipe(
					Effect.andThen(Deferred.await(gate)),
					Effect.andThen(HandlerFailure.make({ retryable: true })),
				),
		})
		const receipt = yield* delivery.admit(arrival('A'))
		assert.strictEqual((yield* delivery.admit(arrival('B'))).accepted, true)
		const active = yield* delivery.processMailbox(receipt).pipe(Effect.forkChild)
		yield* Deferred.await(started)
		yield* delivery.admit(arrival('C'))
		yield* Deferred.succeed(gate, undefined)
		yield* Fiber.join(active)
		yield* delivery.admit(arrival('D'))
		assert.deepStrictEqual(
			yield* delivery.admit(arrival('E')).pipe(Effect.flip),
			DeliveryError.make({ reason: 'capacity' }),
		)
		assert.strictEqual((yield* delivery.admit(arrival('D'))).accepted, false)
		const state = yield* load(receipt.key)
		assert.deepStrictEqual(
			state.outcomes.map((outcome) => outcome.kind),
			['dropped', 'dropped', 'dropped'],
		)
		assert.deepStrictEqual(
			state.active?.envelopes.map((event) => event.eventId),
			['A'],
		)
		assert.deepStrictEqual(state.pending, [])
		assert.strictEqual(state.readyAt, 100)
	}).pipe(Effect.provide(memory)),
)

it.effect('two runners enforce shared concurrency, renew all owners, and cancel only the recorded attempt', () =>
	Effect.gen(function* () {
		const calls = yield* Queue.unbounded<string>()
		const finalized = yield* Queue.unbounded<string>()
		const gate = yield* Deferred.make<void>()
		const policy: DeliveryPolicy = { ...limits, mode: 'concurrent', maxConcurrency: 2 }
		const delivery = bind({
			...registration,
			policy,
			handler: (event, context) => {
				assert.deepStrictEqual(context.skipped, [])
				return Queue.offer(calls, event.id).pipe(
					Effect.andThen(Deferred.await(gate)),
					Effect.ensuring(Queue.offer(finalized, event.id)),
				)
			},
		})
		const receipt = yield* delivery.admit(arrival('A'))
		for (const id of ['B', 'C', 'D']) yield* delivery.admit(arrival(id))
		const one = yield* delivery.run({ scanLimit: 10, concurrency: 4, pollMs: 10 }).pipe(Effect.forkChild)
		const two = yield* delivery.run({ scanLimit: 10, concurrency: 4, pollMs: 10 }).pipe(Effect.forkChild)
		assert.deepStrictEqual([yield* Queue.take(calls), yield* Queue.take(calls)].sort(), ['A', 'B'])
		yield* TestClock.adjust(2100)
		assert.strictEqual(yield* Queue.size(calls), 0)
		const before = activeBatches(yield* load(receipt.key))
		assert.strictEqual(before.length, 2)
		assert.ok(before.every((batch) => batch.attempt === 1 && batch.leaseUntil > 2100))
		assert.strictEqual(yield* delivery.cancelActive({ ...receipt, controlId: 'stop', eventId: 'B' }), true)
		yield* TestClock.adjust(110)
		assert.strictEqual(yield* Queue.take(finalized), 'B')
		assert.strictEqual(yield* Queue.take(calls), 'C')
		assert.strictEqual(yield* delivery.cancelActive({ ...receipt, controlId: 'stop' }), false)
		assert.strictEqual(activeBatches(yield* load(receipt.key)).length, 2)
		assert.strictEqual(
			(yield* load(receipt.key)).outcomes.filter((outcome) => outcome.kind === 'cancelled').length,
			1,
		)
		yield* Deferred.succeed(gate, undefined)
		assert.deepStrictEqual([yield* Queue.take(finalized), yield* Queue.take(finalized)].sort(), ['A', 'C'])
		yield* TestClock.adjust(10)
		assert.strictEqual(yield* Queue.take(calls), 'D')
		assert.strictEqual(yield* Queue.take(finalized), 'D')
		yield* TestClock.adjust(10)
		assert.strictEqual((yield* load(receipt.key)).readyAt, null)
		assert.strictEqual(
			(yield* load(receipt.key)).outcomes.filter((outcome) => outcome.kind === 'completed').length,
			3,
		)
		yield* Fiber.interrupt(one)
		yield* Fiber.interrupt(two)
	}).pipe(Effect.provide(memory)),
)

it.effect('a single runner fills multiple slots in one mailbox without exceeding its host limit', () =>
	Effect.gen(function* () {
		const calls = yield* Queue.unbounded<string>()
		const delivery = bind({
			...registration,
			policy: { ...limits, mode: 'concurrent', maxConcurrency: 3 },
			handler: (event) => Queue.offer(calls, event.id).pipe(Effect.andThen(Effect.never)),
		})
		const receipt = yield* delivery.admit(arrival('A'))
		yield* delivery.admit(arrival('B'))
		yield* delivery.admit(arrival('C'))
		const runner = yield* delivery.run({ scanLimit: 10, concurrency: 2, pollMs: 10 }).pipe(Effect.forkChild)
		assert.strictEqual(yield* Queue.take(calls), 'A')
		yield* TestClock.adjust(10)
		assert.strictEqual(yield* Queue.take(calls), 'B')
		yield* TestClock.adjust(100)
		assert.strictEqual(yield* Queue.size(calls), 0)
		assert.strictEqual(activeBatches(yield* load(receipt.key)).length, 2)
		yield* Fiber.interrupt(runner)
	}).pipe(Effect.provide(memory)),
)

it.effect('concurrent retry, terminal failure and completion preserve other owners and pending work', () =>
	Effect.gen(function* () {
		const calls = yield* Queue.unbounded<string>()
		const gate = yield* Deferred.make<void>()
		let attempts = 0
		const delivery = bind({
			...registration,
			policy: { ...limits, mode: 'concurrent', maxConcurrency: 2 },
			handler: (event) =>
				Effect.gen(function* () {
					yield* Queue.offer(calls, event.id)
					if (event.id === 'A' && ++attempts === 1) return yield* HandlerFailure.make({ retryable: true })
					if (event.id === 'B') yield* Deferred.await(gate)
					if (event.id === 'C') return yield* HandlerFailure.make({ retryable: false })
				}),
		})
		const receipt = yield* delivery.admit(arrival('A'))
		yield* delivery.admit(arrival('B'))
		yield* delivery.processMailbox(receipt)
		assert.strictEqual(yield* Queue.take(calls), 'A')
		const other = yield* delivery.processMailbox(receipt).pipe(Effect.forkChild)
		assert.strictEqual(yield* Queue.take(calls), 'B')
		const owner = activeBatches(yield* load(receipt.key)).find((batch) => batch.envelopes[0].eventId === 'B')?.owner
		yield* delivery.admit(arrival('C'))
		assert.strictEqual(yield* delivery.processMailbox(receipt), false)
		yield* TestClock.adjust(100)
		assert.strictEqual(yield* delivery.processMailbox(receipt), true)
		assert.strictEqual(yield* Queue.take(calls), 'A')
		assert.strictEqual(yield* delivery.processMailbox(receipt), true)
		assert.strictEqual(yield* Queue.take(calls), 'C')
		const state = yield* load(receipt.key)
		assert.strictEqual(state.active?.owner, owner)
		assert.strictEqual(state.failed.length, 1)
		assert.deepStrictEqual(
			state.outcomes.map((outcome) => outcome.kind),
			['completed', 'failed'],
		)
		yield* Deferred.succeed(gate, undefined)
		yield* Fiber.join(other)
		assert.strictEqual((yield* load(receipt.key)).readyAt, null)
	}).pipe(Effect.provide(memory)),
)

for (const policy of policies.filter((policy) => policy.mode !== 'queue')) {
	it.effect(`${policy.mode}: retries keep the frozen batch and preserve newer pending deadlines`, () =>
		Effect.gen(function* () {
			const calls = yield* Queue.unbounded<ReadonlyArray<string>>()
			let attempts = 0
			const delivery = bind({
				...registration,
				policy,
				handler: (event, context) =>
					Queue.offer(calls, [...context.skipped.map((entry) => entry.id), event.id]).pipe(
						Effect.andThen(
							Effect.suspend(() =>
								++attempts === 1 ? HandlerFailure.make({ retryable: true }) : Effect.void,
							),
						),
					),
			})
			const receipt = yield* delivery.admit(arrival('A'))
			yield* delivery.admit(arrival('B'))
			yield* TestClock.adjust(1500)
			yield* delivery.processMailbox(receipt)
			yield* delivery.admit(arrival('C'))
			yield* TestClock.adjust(100)
			yield* delivery.processMailbox(receipt)
			assert.deepStrictEqual(yield* Queue.take(calls), ['A', 'B'])
			assert.deepStrictEqual(yield* Queue.take(calls), ['A', 'B'])
			if (policy.mode === 'debounce') {
				assert.strictEqual(yield* delivery.processMailbox(receipt), false)
				yield* TestClock.adjust(1400)
			}
			yield* delivery.processMailbox(receipt)
			assert.deepStrictEqual(yield* Queue.take(calls), ['C'])
		}).pipe(Effect.provide(memory)),
	)
}

it.effect('decodes and upgrades v1 accepted work, preserving frozen retry, failed payloads and dedupe', () =>
	Effect.gen(function* () {
		const calls = yield* Queue.unbounded<ReadonlyArray<string>>()
		const delivery = bind({
			...registration,
			policy: { ...limits, mode: 'queue' },
			handler: (event, context) =>
				Queue.offer(calls, [...context.skipped.map((entry) => entry.id), event.id]).pipe(Effect.asVoid),
		})
		const key = yield* delivery.keyFor(arrival('A'))
		const envelope = (id: string) => ({
			definition: definition.name,
			version: '1',
			eventId: id,
			resource: '"thread"',
			payload: JSON.stringify({ id }),
			acceptedAt: 0,
		})
		const legacyJson = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Json))({
			revision: 7,
			state: {
				version: 1,
				pending: [envelope('C'), envelope('D')],
				active: {
					envelopes: [envelope('A'), envelope('B')],
					attempt: 1,
					owner: null,
					leaseUntil: 100,
					cancelled: false,
				},
				failed: [{ envelopes: [envelope('failed')], attempt: 3, owner: 4, leaseUntil: 1, cancelled: false }],
				outcomes: [{ identity: 'old', kind: 'completed', expiresAt: 9999 }],
				readyAt: 100,
			},
		})
		const snapshot = yield* Schema.decodeEffect(Schema.fromJsonString(MailboxSnapshot))(legacyJson)
		const store = yield* MailboxStore
		yield* store.commitMailbox({ key, expectedRevision: null, nextState: snapshot.state })
		assert.strictEqual((yield* store.loadMailbox({ key }))?.state.version, 1)
		assert.strictEqual((yield* delivery.admit(arrival('B'))).accepted, false)
		assert.strictEqual((yield* store.loadMailbox({ key }))?.state.version, 3)
		assert.strictEqual(yield* delivery.processMailbox({ key }), false)
		yield* TestClock.adjust(100)
		yield* delivery.processMailbox({ key })
		yield* delivery.processMailbox({ key })
		assert.deepStrictEqual(yield* Queue.take(calls), ['A', 'B'])
		assert.deepStrictEqual(yield* Queue.take(calls), ['C', 'D'])
		const state = yield* load(key)
		assert.deepStrictEqual(state.failed, snapshot.state.failed)
		assert.strictEqual(state.outcomes[0]?.identity, 'old')
		assert.strictEqual(state.readyAt, null)
	}).pipe(Effect.provide(memory)),
)

it.effect('rejects invalid mode-specific limits through Schema and before admission', () =>
	Effect.gen(function* () {
		for (const invalid of [
			{ mode: 'concurrent' },
			{ mode: 'concurrent', maxConcurrency: 0 },
			{ mode: 'concurrent', maxConcurrency: 1.5 },
			{ mode: 'debounce' },
			{ mode: 'debounce', quietPeriodMs: -1 },
			{ mode: 'debounce', quietPeriodMs: Infinity },
			{ mode: 'burst' },
			{ mode: 'burst', windowMs: 0 },
			{ mode: 'interrupt' },
		]) {
			assert.strictEqual(Schema.is(DeliveryPolicy)({ ...limits, ...invalid }), false)
		}
		const delivery = bind({
			...registration,
			policy: { ...limits, mode: 'concurrent', maxConcurrency: 0 },
			handler: () => Effect.void,
		})
		assert.deepStrictEqual(
			yield* delivery.admit(arrival('A')).pipe(Effect.flip),
			DeliveryError.make({ reason: 'configuration' }),
		)
		const readiness = yield* MailboxReadiness
		assert.deepStrictEqual(yield* readiness.scanReady({ prefix: '', now: 0, limit: 10 }), [])
	}).pipe(Effect.provide(memory)),
)

it.effect('reconstructs an expired concurrent attempt without reclaiming a live peer', () =>
	Effect.gen(function* () {
		const calls = yield* Queue.unbounded<string>()
		const policy: DeliveryPolicy = { ...limits, mode: 'concurrent', maxConcurrency: 2 }
		const delivery = bind({
			...registration,
			policy,
			handler: (event) => Queue.offer(calls, event.id).pipe(Effect.andThen(Effect.never)),
		})
		const receipt = yield* delivery.admit(arrival('A'))
		yield* delivery.admit(arrival('B'))
		const first = yield* delivery.processMailbox(receipt).pipe(Effect.forkChild)
		assert.strictEqual(yield* Queue.take(calls), 'A')
		const second = yield* delivery.processMailbox(receipt).pipe(Effect.forkChild)
		assert.strictEqual(yield* Queue.take(calls), 'B')
		const peer = activeBatches(yield* load(receipt.key)).find((batch) => batch.envelopes[0].eventId === 'B')
		yield* Fiber.interrupt(first)
		yield* TestClock.adjust(limits.leaseMs)
		const replacement = bind({
			...registration,
			policy,
			handler: (event) => Queue.offer(calls, event.id).pipe(Effect.asVoid),
		})
		assert.strictEqual(yield* replacement.processMailbox(receipt), true)
		assert.strictEqual(yield* Queue.take(calls), 'A')
		const state = yield* load(receipt.key)
		assert.strictEqual(activeBatches(state).length, 1)
		assert.strictEqual(state.active?.owner, peer?.owner)
		assert.strictEqual(state.active?.attempt, 1)
		assert.strictEqual((yield* replacement.admit(arrival('A'))).accepted, false)
		yield* Fiber.interrupt(second)
	}).pipe(Effect.provide(memory)),
)

it.effect('stale concurrent completion cannot clear either the replacement or its peer', () =>
	Effect.gen(function* () {
		const calls = yield* Queue.unbounded<string>()
		const gate = yield* Deferred.make<void>()
		const delivery = bind({
			...registration,
			policy: { ...limits, mode: 'concurrent', maxConcurrency: 2 },
			handler: (event) =>
				Queue.offer(calls, event.id).pipe(
					Effect.andThen(event.id === 'A' ? Deferred.await(gate) : Effect.never),
				),
		})
		const receipt = yield* delivery.admit(arrival('A'))
		yield* delivery.admit(arrival('B'))
		const first = yield* delivery.processMailbox(receipt).pipe(Effect.flip, Effect.forkChild)
		assert.strictEqual(yield* Queue.take(calls), 'A')
		const second = yield* delivery.processMailbox(receipt).pipe(Effect.forkChild)
		assert.strictEqual(yield* Queue.take(calls), 'B')
		const store = yield* MailboxStore
		const snapshot = yield* store.loadMailbox(receipt)
		assert.ok(snapshot !== undefined && snapshot.state.active !== null)
		yield* store.commitMailbox({
			key: receipt.key,
			expectedRevision: snapshot.revision,
			nextState: { ...snapshot.state, active: { ...snapshot.state.active, owner: snapshot.revision + 1 } },
		})
		yield* Deferred.succeed(gate, undefined)
		assert.deepStrictEqual(yield* Fiber.join(first), DeliveryError.make({ reason: 'stale' }))
		const state = yield* load(receipt.key)
		assert.strictEqual(state.active?.owner, snapshot.revision + 1)
		assert.deepStrictEqual(state.additionalActive, currentMailbox(snapshot.state).additionalActive)
		assert.deepStrictEqual(state.outcomes, [])
		yield* Fiber.interrupt(second)
	}).pipe(Effect.provide(memory)),
)

it.effect('debounce admission racing completion retains the reset deadline and all pending context', () =>
	Effect.gen(function* () {
		const reached = yield* Deferred.make<void>()
		const release = yield* Deferred.make<void>()
		const calls = yield* Queue.unbounded<ReadonlyArray<string>>()
		const delivery = bind({
			...registration,
			policy: { ...limits, mode: 'debounce', quietPeriodMs: 200 },
			handler: (event, context) =>
				Queue.offer(calls, [...context.skipped.map((entry) => entry.id), event.id]).pipe(Effect.asVoid),
		})
		const receipt = yield* delivery.admit(arrival('A'))
		yield* TestClock.adjust(200)
		const store = yield* MailboxStore
		const delayed = Layer.succeed(
			MailboxStore,
			MailboxStore.of({
				loadMailbox: store.loadMailbox,
				commitMailbox: (input) =>
					Effect.gen(function* () {
						if (input.nextState.outcomes.some((outcome) => outcome.kind === 'completed')) {
							yield* Deferred.succeed(reached, undefined)
							yield* Deferred.await(release)
						}
						return yield* store.commitMailbox(input)
					}),
			}),
		)
		const active = yield* delivery.processMailbox(receipt).pipe(Effect.provide(delayed), Effect.forkChild)
		yield* Deferred.await(reached)
		yield* delivery.admit(arrival('B'))
		yield* delivery.admit(arrival('C'))
		yield* Deferred.succeed(release, undefined)
		yield* Fiber.join(active)
		assert.strictEqual((yield* load(receipt.key)).readyAt, 400)
		assert.strictEqual(yield* delivery.processMailbox(receipt), false)
		yield* TestClock.adjust(200)
		yield* delivery.processMailbox(receipt)
		assert.deepStrictEqual(yield* Queue.take(calls), ['A'])
		assert.deepStrictEqual(yield* Queue.take(calls), ['B', 'C'])
	}).pipe(Effect.provide(memory)),
)

it.effect('a reconstructed cancellation barrier follows a frozen retry, not its live concurrent peer', () =>
	Effect.gen(function* () {
		const calls = yield* Queue.unbounded<string>()
		const policy: DeliveryPolicy = { ...limits, mode: 'concurrent', maxConcurrency: 2, retentionMs: 10 }
		const delivery = bind({
			...registration,
			policy,
			handler: (event) =>
				Queue.offer(calls, event.id).pipe(
					Effect.andThen(event.id === 'A' ? HandlerFailure.make({ retryable: true }) : Effect.never),
				),
		})
		const receipt = yield* delivery.admit(arrival('A'))
		yield* delivery.processMailbox(receipt)
		assert.strictEqual(yield* Queue.take(calls), 'A')
		yield* delivery.admit(arrival('B'))
		const peer = yield* delivery.processMailbox(receipt).pipe(Effect.forkChild)
		assert.strictEqual(yield* Queue.take(calls), 'B')
		assert.strictEqual(yield* delivery.cancelActive({ ...receipt, controlId: 'stop' }), true)
		const replacement = bind({
			...registration,
			policy,
			handler: () => Effect.die('cancelled retry must not invoke handler'),
		})
		const retired = yield* Deferred.make<void>()
		const barrier = yield* replacement
			.awaitCancellation({ ...receipt, controlId: 'stop' })
			.pipe(Effect.andThen(Deferred.succeed(retired, undefined)), Effect.forkChild)
		yield* TestClock.adjust(50)
		assert.strictEqual(yield* replacement.cancelActive({ ...receipt, controlId: 'stop' }), false)
		assert.strictEqual(yield* Deferred.isDone(retired), false)
		yield* TestClock.adjust(50)
		yield* replacement.processMailbox(receipt)
		yield* TestClock.adjust(100)
		yield* Fiber.join(barrier)
		assert.deepStrictEqual(
			activeBatches(yield* load(receipt.key)).map((batch) => [batch.envelopes[0].eventId, batch.cancelled]),
			[['B', false]],
		)
		yield* replacement.admit(arrival('C'))
		yield* replacement.awaitCancellation({ ...receipt, controlId: 'stop' })
		yield* Fiber.interrupt(peer)
	}).pipe(Effect.provide(memory)),
)

for (const outcome of ['completed', 'retry'] as const) {
	it.effect(`event-targeted Stop racing ${outcome} cannot cancel a replacement attempt or peer`, () =>
		Effect.gen(function* () {
			const reached = yield* Deferred.make<void>()
			const releaseControl = yield* Deferred.make<void>()
			const releaseHandler = yield* Deferred.make<void>()
			const calls = yield* Queue.unbounded<string>()
			let attempts = 0
			const delivery = bind({
				...registration,
				policy: { ...limits, mode: 'concurrent', maxConcurrency: 2 },
				handler: (event) =>
					Effect.gen(function* () {
						yield* Queue.offer(calls, event.id)
						if (event.id !== 'A' || ++attempts > 1) return yield* Effect.never
						yield* Deferred.await(releaseHandler)
						if (outcome === 'retry') return yield* HandlerFailure.make({ retryable: true })
					}),
			})
			const receipt = yield* delivery.admit(arrival('A'))
			yield* delivery.admit(arrival('B'))
			const first = yield* delivery.processMailbox(receipt).pipe(Effect.forkChild)
			assert.strictEqual(yield* Queue.take(calls), 'A')
			const peer = yield* delivery.processMailbox(receipt).pipe(Effect.forkChild)
			assert.strictEqual(yield* Queue.take(calls), 'B')
			const store = yield* MailboxStore
			const delayed = Layer.succeed(
				MailboxStore,
				MailboxStore.of({
					loadMailbox: store.loadMailbox,
					commitMailbox: (input) =>
						Deferred.succeed(reached, undefined).pipe(
							Effect.andThen(Deferred.await(releaseControl)),
							Effect.andThen(store.commitMailbox(input)),
						),
				}),
			)
			const stop = yield* delivery
				.cancelActive({ ...receipt, controlId: 'race', eventId: 'A' })
				.pipe(Effect.provide(delayed), Effect.forkChild)
			yield* Deferred.await(reached)
			yield* Deferred.succeed(releaseHandler, undefined)
			yield* Fiber.join(first)
			if (outcome === 'retry') yield* TestClock.adjust(100)
			else yield* delivery.admit(arrival('C'))
			const replacement = yield* delivery.processMailbox(receipt).pipe(Effect.forkChild)
			assert.strictEqual(yield* Queue.take(calls), outcome === 'retry' ? 'A' : 'C')
			yield* Deferred.succeed(releaseControl, undefined)
			assert.strictEqual(yield* Fiber.join(stop), false)
			assert.strictEqual(yield* delivery.cancelActive({ ...receipt, controlId: 'race', eventId: 'B' }), false)
			yield* delivery.awaitCancellation({ ...receipt, controlId: 'race' })
			const state = yield* load(receipt.key)
			assert.strictEqual(activeBatches(state).length, 2)
			assert.ok(activeBatches(state).every((batch) => !batch.cancelled))
			assert.strictEqual(
				state.outcomes.find((entry) => entry.identity === 'control:race')?.cancellationTarget,
				null,
			)
			yield* Fiber.interrupt(replacement)
			yield* Fiber.interrupt(peer)
		}).pipe(Effect.provide(memory)),
	)
}

it.effect('event targets include coalesced frozen retries, but not pending, unknown or completed events', () =>
	Effect.gen(function* () {
		const calls = yield* Queue.unbounded<string>()
		const delivery = bind({
			...registration,
			policy: { ...limits, mode: 'queue' },
			handler: (event) =>
				Queue.offer(calls, event.id).pipe(Effect.andThen(HandlerFailure.make({ retryable: true }))),
		})
		const receipt = yield* delivery.admit(arrival('A'))
		yield* delivery.admit(arrival('B'))
		yield* delivery.processMailbox(receipt)
		assert.strictEqual(yield* Queue.take(calls), 'B')
		yield* delivery.admit(arrival('C'))
		for (const eventId of ['C', 'unknown']) {
			assert.strictEqual(yield* delivery.cancelActive({ ...receipt, controlId: eventId, eventId }), false)
			yield* delivery.awaitCancellation({ ...receipt, controlId: eventId })
		}
		assert.strictEqual(yield* delivery.cancelActive({ ...receipt, controlId: 'stop', eventId: 'B' }), true)
		assert.strictEqual(yield* delivery.cancelActive({ ...receipt, controlId: 'stop', eventId: 'A' }), false)
		const inactive = yield* delivery.awaitInactive(receipt).pipe(Effect.forkChild)
		const cancelled = yield* delivery.awaitCancellation({ ...receipt, controlId: 'stop' }).pipe(Effect.forkChild)
		yield* TestClock.adjust(100)
		assert.strictEqual(yield* delivery.processMailbox(receipt), true)
		yield* TestClock.adjust(100)
		yield* Fiber.join(inactive)
		yield* Fiber.join(cancelled)
		assert.deepStrictEqual(
			(yield* load(receipt.key)).pending.map((event) => event.eventId),
			['C'],
		)
		assert.strictEqual(yield* Queue.size(calls), 0)
		const completed = bind({ ...registration, policy: { ...limits, mode: 'queue' }, handler: () => Effect.void })
		yield* completed.processMailbox(receipt)
		assert.strictEqual(yield* completed.cancelActive({ ...receipt, controlId: 'completed', eventId: 'C' }), false)
		assert.deepStrictEqual(
			yield* delivery.cancelActive({ ...receipt, controlId: 'invalid', eventId: '' }).pipe(Effect.flip),
			DeliveryError.make({ reason: 'definition' }),
		)
	}).pipe(Effect.provide(memory)),
)

for (const barrier of ['awaitInactive', 'awaitCancellation'] as const) {
	it.effect(`${barrier} checks immediately, polls at heartbeat spacing and stops on interruption`, () =>
		Effect.gen(function* () {
			const started = yield* Deferred.make<void>()
			const reads = yield* Queue.unbounded<number>()
			const delivery = bind({
				...registration,
				policy: { ...limits, mode: 'queue' },
				handler: () => Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never)),
			})
			const receipt = yield* delivery.admit(arrival('A'))
			const running = yield* delivery.processMailbox(receipt).pipe(Effect.forkChild)
			yield* Deferred.await(started)
			yield* Fiber.interrupt(running)
			yield* delivery.cancelActive({ ...receipt, controlId: 'stop', eventId: 'A' })
			const store = yield* MailboxStore
			const observed = Layer.succeed(
				MailboxStore,
				MailboxStore.of({
					loadMailbox: (input) =>
						store
							.loadMailbox(input)
							.pipe(
								Effect.tap(() =>
									Clock.currentTimeMillis.pipe(Effect.flatMap((now) => Queue.offer(reads, now))),
								),
							),
					commitMailbox: store.commitMailbox,
				}),
			)
			const waiting = yield* delivery[barrier]({ ...receipt, controlId: 'stop' }).pipe(
				Effect.provide(observed),
				Effect.forkChild,
			)
			assert.strictEqual(yield* Queue.take(reads), 0)
			yield* TestClock.adjust(99)
			assert.strictEqual(yield* Queue.size(reads), 0)
			yield* TestClock.adjust(1)
			assert.strictEqual(yield* Queue.take(reads), 100)
			yield* Fiber.interrupt(waiting)
			yield* TestClock.adjust(200)
			assert.strictEqual(yield* Queue.size(reads), 0)
		}).pipe(Effect.provide(memory)),
	)
}

it.effect('a no-target control never waits for later work; legacy controls only wait for cancelled batches', () =>
	Effect.gen(function* () {
		const calls = yield* Queue.unbounded<string>()
		const delivery = bind({
			...registration,
			policy: { ...limits, mode: 'concurrent', maxConcurrency: 2 },
			handler: (event) => Queue.offer(calls, event.id).pipe(Effect.andThen(Effect.never)),
		})
		const key = yield* delivery.keyFor(arrival('A'))
		assert.strictEqual(yield* delivery.cancelActive({ key, controlId: 'empty' }), false)
		yield* delivery.admit(arrival('A'))
		const running = yield* delivery.processMailbox({ key }).pipe(Effect.forkChild)
		yield* Queue.take(calls)
		yield* delivery.awaitCancellation({ key, controlId: 'empty' })
		const store = yield* MailboxStore
		const snapshot = yield* store.loadMailbox({ key })
		assert.ok(snapshot !== undefined && snapshot.state.active !== null)
		yield* store.commitMailbox({
			key,
			expectedRevision: snapshot.revision,
			nextState: {
				...snapshot.state,
				active: { ...snapshot.state.active, cancelled: true },
				outcomes: [
					...snapshot.state.outcomes,
					{ identity: 'control:legacy', kind: 'control', expiresAt: 1000 },
				],
			},
		})
		const retired = yield* Deferred.make<void>()
		const wait = yield* delivery
			.awaitCancellation({ key, controlId: 'legacy' })
			.pipe(Effect.andThen(Deferred.succeed(retired, undefined)), Effect.forkChild)
		assert.strictEqual(yield* Deferred.isDone(retired), false)
		yield* TestClock.adjust(200)
		yield* Fiber.join(running)
		yield* Fiber.join(wait)
	}).pipe(Effect.provide(memory)),
)
