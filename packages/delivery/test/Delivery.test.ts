import { assert, it } from '@effect/vitest'
import { Cause, Clock, Deferred, Effect, Exit, Fiber, Layer, Logger, Queue, Schema } from 'effect'
import { TestClock } from 'effect/testing'

import { bind, DeliveryError, HandlerFailure } from '../src/Delivery.ts'
import { DeliveryPolicy } from '../src/DeliveryPolicy.ts'
import type { EventDefinition } from '../src/EventDefinition.ts'
import { emptyMailbox, mailboxKey } from '../src/Mailbox.ts'
import { MailboxReadiness, MailboxStore, MailboxStoreError } from '../src/MailboxStore.ts'
import { layer } from '../src/memory.ts'

const Event = Schema.Struct({
	id: Schema.String,
	installation: Schema.String,
	resource: Schema.String,
	text: Schema.String,
})
type Event = typeof Event.Type
const definition: EventDefinition<typeof Event, typeof Schema.String> = {
	name: 'test.message',
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
const event = (id: string) => Event.make({ id, installation: 'T1', resource: 'C1:root', text: id })
const memory = layer({ maxMailboxes: 100 })

it.effect('admits another mailbox while an earlier handler remains active', () =>
	Effect.gen(function* () {
		const calls = yield* Queue.unbounded<string>()
		const delivery = bind({
			namespace: 'fairness',
			handlerId: 'reply',
			definition,
			policy,
			handler: (input) => Queue.offer(calls, input.id).pipe(Effect.andThen(Effect.never)),
		})
		yield* delivery.admit({ event: event('A') })
		const runner = yield* delivery.run({ scanLimit: 10, concurrency: 2, pollMs: 10 }).pipe(Effect.forkChild)
		assert.strictEqual(yield* Queue.take(calls), 'A')
		yield* delivery.admit({ event: { ...event('B'), resource: 'another-thread' } })
		yield* TestClock.adjust(10)
		assert.strictEqual(yield* Queue.take(calls), 'B')
		yield* Fiber.interrupt(runner)
	}).pipe(Effect.provide(memory)),
)

it.effect('two host-owned runners share one claim and stop with their scope', () =>
	Effect.gen(function* () {
		const calls = yield* Queue.unbounded<string>()
		const finalized = yield* Deferred.make<void>()
		const delivery = bind({
			namespace: 'app',
			handlerId: 'runner',
			definition,
			policy,
			handler: (input) =>
				Queue.offer(calls, input.id).pipe(
					Effect.andThen(Effect.never),
					Effect.ensuring(Deferred.succeed(finalized, undefined)),
				),
		})
		const receipt = yield* delivery.admit({ event: event('A') })
		yield* Effect.gen(function* () {
			yield* delivery.run({ scanLimit: 2, concurrency: 2, pollMs: 10 }).pipe(Effect.forkScoped)
			yield* delivery.run({ scanLimit: 2, concurrency: 2, pollMs: 10 }).pipe(Effect.forkScoped)
			assert.strictEqual(yield* Queue.take(calls), 'A')
			yield* TestClock.adjust(500)
			assert.strictEqual(yield* Queue.size(calls), 0)
			const store = yield* MailboxStore
			assert.strictEqual((yield* store.loadMailbox(receipt))?.state.active?.attempt, 1)
		}).pipe(Effect.scoped)
		assert.strictEqual(yield* Deferred.isDone(finalized), true)
	}).pipe(Effect.provide(memory)),
)

it.effect('commits before executing, queues A then D with typed skipped B/C and retains readiness', () =>
	Effect.gen(function* () {
		const started = yield* Deferred.make<void>()
		const release = yield* Deferred.make<void>()
		const calls = yield* Queue.unbounded<{ readonly text: string; readonly skipped: ReadonlyArray<string> }>()
		const delivery = bind({
			namespace: 'app',
			handlerId: 'reply',
			definition,
			policy,
			handler: (input, context) =>
				Effect.gen(function* () {
					yield* Queue.offer(calls, { text: input.text, skipped: context.skipped.map((entry) => entry.text) })
					if (input.id === 'A') {
						yield* Deferred.succeed(started, undefined)
						yield* Deferred.await(release)
					}
				}),
		})
		const receipt = yield* delivery.admit({ event: event('A') })
		const store = yield* MailboxStore
		assert.strictEqual((yield* store.loadMailbox(receipt))?.state.pending.length, 1)
		assert.strictEqual(yield* Queue.size(calls), 0)
		const running = yield* delivery.processMailbox(receipt).pipe(Effect.forkChild)
		yield* Deferred.await(started)
		for (const id of ['B', 'C', 'D']) yield* delivery.admit({ event: event(id) })
		assert.strictEqual((yield* delivery.admit({ event: event('D') })).accepted, false)
		assert.strictEqual(yield* delivery.processMailbox(receipt), false)
		yield* Deferred.succeed(release, undefined)
		yield* Fiber.join(running)
		const readiness = yield* MailboxReadiness
		assert.deepStrictEqual(yield* readiness.scanReady({ prefix: '', now: 0, limit: 5 }), [receipt.key])
		yield* delivery.processMailbox(receipt)
		assert.deepStrictEqual(yield* Queue.take(calls), { text: 'A', skipped: [] })
		assert.deepStrictEqual(yield* Queue.take(calls), { text: 'D', skipped: ['B', 'C'] })
		const final = yield* store.loadMailbox(receipt)
		assert.strictEqual(final?.state.active, null)
		assert.strictEqual(final?.state.pending.length, 0)
		assert.strictEqual(final?.state.readyAt, null)
		assert.strictEqual(final?.state.outcomes.length, 4)
	}).pipe(Effect.provide(memory)),
)

it.effect('freezes failed batches across retries and parks exhausted work without discarding newer work', () =>
	Effect.gen(function* () {
		const calls = yield* Queue.unbounded<ReadonlyArray<string>>()
		const delivery = bind({
			namespace: 'app',
			handlerId: 'retry',
			definition,
			policy,
			handler: (input, context) =>
				Queue.offer(calls, [...context.skipped.map((entry) => entry.id), input.id]).pipe(
					Effect.andThen(Effect.fail(HandlerFailure.make({ retryable: true }))),
				),
		})
		const receipt = yield* delivery.admit({ event: event('A') })
		yield* delivery.admit({ event: event('B') })
		yield* delivery.processMailbox(receipt)
		yield* delivery.admit({ event: event('C') })
		assert.strictEqual(yield* delivery.processMailbox(receipt), false)
		yield* TestClock.adjust(100)
		yield* delivery.processMailbox(receipt)
		yield* TestClock.adjust(200)
		yield* delivery.processMailbox(receipt)
		for (let i = 0; i < 3; i++) assert.deepStrictEqual(yield* Queue.take(calls), ['A', 'B'])
		const store = yield* MailboxStore
		const snapshot = yield* store.loadMailbox(receipt)
		assert.strictEqual(snapshot?.state.failed.length, 1)
		assert.deepStrictEqual(
			snapshot?.state.pending.map((entry) => entry.eventId),
			['C'],
		)
		assert.strictEqual(snapshot?.state.active, null)
	}).pipe(Effect.provide(memory)),
)

it.effect('targets cancellation at the active batch; duplicate Stop cannot cancel its successor', () =>
	Effect.gen(function* () {
		const calls = yield* Queue.unbounded<string>()
		const release = yield* Deferred.make<void>()
		const finalized = yield* Queue.unbounded<string>()
		const delivery = bind({
			namespace: 'app',
			handlerId: 'cancel',
			definition,
			policy,
			handler: (input) =>
				Queue.offer(calls, input.id).pipe(
					Effect.andThen(Deferred.await(release)),
					Effect.ensuring(Queue.offer(finalized, input.id)),
				),
		})
		const receipt = yield* delivery.admit({ event: event('A') })
		const first = yield* delivery.processMailbox(receipt).pipe(Effect.forkChild)
		assert.strictEqual(yield* Queue.take(calls), 'A')
		yield* delivery.admit({ event: event('B') })
		assert.strictEqual(yield* delivery.cancelActive({ ...receipt, controlId: 'stop' }), true)
		yield* TestClock.adjust(100)
		yield* Fiber.join(first)
		assert.strictEqual(yield* Queue.take(finalized), 'A')
		const second = yield* delivery.processMailbox(receipt).pipe(Effect.forkChild)
		assert.strictEqual(yield* Queue.take(calls), 'B')
		assert.strictEqual(yield* delivery.cancelActive({ ...receipt, controlId: 'stop' }), false)
		yield* TestClock.adjust(100)
		assert.strictEqual(yield* Queue.size(finalized), 0)
		yield* Deferred.succeed(release, undefined)
		yield* Fiber.join(second)
		assert.strictEqual(yield* Queue.take(finalized), 'B')
	}).pipe(Effect.provide(memory)),
)

it.effect('isolates installations and handlers and rejects delimiter collisions', () =>
	Effect.gen(function* () {
		const first = bind({ namespace: 'app', handlerId: 'one', definition, policy, handler: () => Effect.void })
		const second = bind({ namespace: 'app', handlerId: 'two', definition, policy, handler: () => Effect.void })
		const a = yield* first.admit({ event: event('A') })
		const b = yield* first.admit({ event: { ...event('A'), installation: 'T2' } })
		const c = yield* second.admit({ event: event('A') })
		assert.strictEqual(new Set([a.key, b.key, c.key]).size, 3)
		const address = { namespace: 'app', handlerId: 'one', provider: 'test', installation: 'a:b', resourceKey: 'c' }
		assert.notStrictEqual(mailboxKey(address), mailboxKey({ ...address, installation: 'a', resourceKey: 'b:c' }))
		assert.strictEqual(
			mailboxKey(address),
			mailboxKey({ resourceKey: 'c', installation: 'a:b', provider: 'test', handlerId: 'one', namespace: 'app' }),
		)
	}).pipe(Effect.provide(memory)),
)

it.effect('renews ownership until cancelled handler finalizers finish', () =>
	Effect.gen(function* () {
		const started = yield* Deferred.make<void>()
		const cleaning = yield* Deferred.make<void>()
		const release = yield* Deferred.make<void>()
		const calls = yield* Queue.unbounded<string>()
		const delivery = bind({
			namespace: 'app',
			handlerId: 'cancel-cleanup',
			definition,
			policy,
			handler: (input) =>
				Effect.gen(function* () {
					yield* Effect.addFinalizer(() =>
						Deferred.succeed(cleaning, undefined).pipe(Effect.andThen(Deferred.await(release))),
					)
					yield* Queue.offer(calls, input.id)
					yield* Deferred.succeed(started, undefined)
					return yield* Effect.never
				}),
		})
		const receipt = yield* delivery.admit({ event: event('A') })
		const running = yield* delivery.processMailbox(receipt).pipe(Effect.forkChild)
		yield* Deferred.await(started)
		yield* delivery.admit({ event: event('B') })
		yield* delivery.cancelActive({ ...receipt, controlId: 'stop-cleanup' })
		yield* TestClock.adjust(policy.heartbeatMs)
		yield* Deferred.await(cleaning)
		yield* TestClock.adjust(policy.leaseMs * 2)
		const store = yield* MailboxStore
		const snapshot = yield* store.loadMailbox(receipt)
		assert.ok(snapshot?.state.active !== null && snapshot?.state.active !== undefined)
		assert.ok(snapshot.state.active.leaseUntil > (yield* Clock.currentTimeMillis))
		assert.strictEqual(yield* delivery.processMailbox(receipt), false)
		assert.deepStrictEqual(yield* Queue.takeAll(calls), ['A'])
		yield* Deferred.succeed(release, undefined)
		assert.strictEqual(yield* Fiber.join(running), true)
		const final = yield* store.loadMailbox(receipt)
		assert.strictEqual(final?.state.active, null)
		assert.strictEqual(final?.state.outcomes.at(-1)?.kind, 'cancelled')
		assert.deepStrictEqual(
			final?.state.pending.map((entry) => entry.eventId),
			['B'],
		)
	}).pipe(Effect.provide(memory)),
)

it.effect('parks defects without retrying them and leaves subsequent work ready', () =>
	Effect.gen(function* () {
		const calls = yield* Queue.unbounded<string>()
		const delivery = bind({
			namespace: 'app',
			handlerId: 'defect',
			definition,
			policy,
			handler: (input) =>
				Queue.offer(calls, input.id).pipe(
					Effect.andThen(input.id === 'A' ? Effect.die('handler defect') : Effect.void),
				),
		})
		const receipt = yield* delivery.admit({ event: event('A') })
		const result = yield* delivery.processMailbox(receipt).pipe(Effect.exit)
		assert.ok(Exit.isFailure(result))
		assert.ok(Cause.hasDies(result.cause))
		const store = yield* MailboxStore
		const failed = yield* store.loadMailbox(receipt)
		assert.strictEqual(failed?.state.active, null)
		assert.strictEqual(failed?.state.failed.length, 1)
		yield* delivery.admit({ event: event('B') })
		assert.strictEqual(yield* delivery.processMailbox(receipt), true)
		yield* TestClock.adjust(policy.leaseMs * 2)
		assert.strictEqual(yield* delivery.processMailbox(receipt), false)
		assert.deepStrictEqual(yield* Queue.takeAll(calls), ['A', 'B'])
	}).pipe(Effect.provide(memory)),
)

it.effect('rejects invalid runner configuration before scanning an empty store', () =>
	Effect.gen(function* () {
		const invalid = bind({
			namespace: '',
			handlerId: 'invalid-runner',
			definition,
			policy,
			handler: () => Effect.void,
		})
		assert.deepStrictEqual(
			yield* invalid.run({ scanLimit: 1, concurrency: 1, pollMs: 10 }).pipe(Effect.flip),
			DeliveryError.make({ reason: 'configuration' }),
		)
	}).pipe(Effect.provide(memory)),
)

it.effect('reports corrupt persisted payloads without logging their contents', () =>
	Effect.gen(function* () {
		const logs: Array<string> = []
		const logger = Logger.layer([Logger.make((entry) => logs.push(JSON.stringify(entry.message)))])
		const delivery = bind({
			namespace: 'app',
			handlerId: 'private-payload',
			definition,
			policy,
			handler: () => Effect.die('must not run'),
		})
		const receipt = yield* delivery.admit({ event: event('A') })
		const store = yield* MailboxStore
		const snapshot = yield* store.loadMailbox(receipt)
		assert.ok(snapshot !== undefined)
		yield* store.commitMailbox({
			key: receipt.key,
			expectedRevision: snapshot.revision,
			nextState: {
				...snapshot.state,
				pending: snapshot.state.pending.map((entry) => ({
					...entry,
					payload: JSON.stringify({ ...event('A'), text: { secret: 'private-payload-sentinel' } }),
				})),
			},
		})
		assert.deepStrictEqual(
			yield* delivery.processMailbox(receipt).pipe(Effect.provide(logger), Effect.flip),
			DeliveryError.make({ reason: 'payload' }),
		)
		assert.ok(logs.some((entry) => entry.includes('schema decoding')))
		assert.ok(logs.every((entry) => !entry.includes('private-payload-sentinel')))
		assert.strictEqual((yield* store.loadMailbox(receipt))?.state.failed.length, 1)
	}).pipe(Effect.provide(memory)),
)

it.effect('rejects overflow before acceptance and permits safe duplicate admission at capacity', () =>
	Effect.gen(function* () {
		const delivery = bind({
			namespace: 'app',
			handlerId: 'limited',
			definition,
			policy: { ...policy, maxEnvelopes: 1 },
			handler: () => Effect.void,
		})
		const receipt = yield* delivery.admit({ event: event('A') })
		assert.strictEqual((yield* delivery.admit({ event: event('A') })).accepted, false)
		assert.deepStrictEqual(
			yield* delivery.admit({ event: event('B') }).pipe(Effect.flip),
			DeliveryError.make({ reason: 'capacity' }),
		)
		const store = yield* MailboxStore
		assert.deepStrictEqual(
			(yield* store.loadMailbox(receipt))?.state.pending.map((entry) => entry.eventId),
			['A'],
		)
	}).pipe(Effect.provide(memory)),
)

it.effect('uses atomic conditional creation and bounded conflict retries', () =>
	Effect.gen(function* () {
		const store = yield* MailboxStore
		const writes = yield* Effect.all(
			[
				store.commitMailbox({ key: 'same', expectedRevision: null, nextState: emptyMailbox() }),
				store.commitMailbox({ key: 'same', expectedRevision: null, nextState: emptyMailbox() }),
			],
			{ concurrency: 2 },
		)
		assert.deepStrictEqual([...writes].sort(), ['committed', 'conflict'])
		let attempts = 0
		const delivery = bind({
			namespace: 'app',
			handlerId: 'conflict',
			definition,
			policy,
			handler: () => Effect.void,
		})
		const conflicting = Layer.succeed(
			MailboxStore,
			MailboxStore.of({
				loadMailbox: store.loadMailbox,
				commitMailbox: () =>
					Effect.sync(() => {
						attempts++
						return 'conflict' as const
					}),
			}),
		)
		assert.deepStrictEqual(
			yield* delivery.admit({ event: event('A') }).pipe(Effect.provide(conflicting), Effect.flip),
			DeliveryError.make({ reason: 'conflict' }),
		)
		assert.strictEqual(attempts, policy.conflictRetries + 1)
	}).pipe(Effect.provide(memory)),
)

it.effect('quarantines incompatible definitions without invoking a mismatched handler', () =>
	Effect.gen(function* () {
		const delivery = bind({
			namespace: 'app',
			handlerId: 'version',
			definition,
			policy,
			handler: () => Effect.die('must not run'),
		})
		const receipt = yield* delivery.admit({ event: event('A') })
		const upgraded = bind({
			namespace: 'app',
			handlerId: 'version',
			definition: { ...definition, version: '2' },
			policy,
			handler: () => Effect.die('must not run'),
		})
		assert.deepStrictEqual(
			yield* upgraded.processMailbox(receipt).pipe(Effect.flip),
			DeliveryError.make({ reason: 'definition' }),
		)
		const store = yield* MailboxStore
		assert.strictEqual((yield* store.loadMailbox(receipt))?.state.failed.length, 1)
	}).pipe(Effect.provide(memory)),
)

it.effect('rejects stale completion after another owner takes over', () =>
	Effect.gen(function* () {
		const calls = yield* Queue.unbounded<string>()
		const release = yield* Deferred.make<void>()
		const delivery = bind({
			namespace: 'app',
			handlerId: 'stale',
			definition,
			policy,
			handler: (input) => Queue.offer(calls, input.id).pipe(Effect.andThen(Deferred.await(release))),
		})
		const receipt = yield* delivery.admit({ event: event('A') })
		const first = yield* delivery.processMailbox(receipt).pipe(Effect.exit, Effect.forkChild)
		yield* Queue.take(calls)
		const store = yield* MailboxStore
		const snapshot = yield* store.loadMailbox(receipt)
		assert.ok(snapshot !== undefined && snapshot.state.active !== null)
		assert.strictEqual(
			yield* store.commitMailbox({
				key: receipt.key,
				expectedRevision: snapshot.revision,
				nextState: { ...snapshot.state, active: { ...snapshot.state.active, owner: snapshot.revision + 1 } },
			}),
			'committed',
		)
		yield* Deferred.succeed(release, undefined)
		const result = yield* Fiber.join(first)
		assert.ok(Exit.isFailure(result))
		assert.deepStrictEqual(
			yield* Effect.failCause(result.cause).pipe(Effect.flip),
			DeliveryError.make({ reason: 'stale' }),
		)
		const final = yield* store.loadMailbox(receipt)
		assert.strictEqual(final?.state.active?.owner, snapshot.revision + 1)
		assert.strictEqual(final?.state.outcomes.length, 0)
	}).pipe(Effect.provide(memory)),
)

it.effect('reconstructs processing over retained memory after interrupted work loses its lease', () =>
	Effect.gen(function* () {
		const calls = yield* Queue.unbounded<string>()
		const original = bind({
			namespace: 'app',
			handlerId: 'recovery',
			definition,
			policy,
			handler: (input) => Queue.offer(calls, input.id).pipe(Effect.andThen(Effect.never)),
		})
		const receipt = yield* original.admit({ event: event('A') })
		const running = yield* original.processMailbox(receipt).pipe(Effect.forkChild)
		yield* Queue.take(calls)
		yield* Fiber.interrupt(running)
		yield* TestClock.adjust(policy.leaseMs)
		const replacement = bind({
			namespace: 'app',
			handlerId: 'recovery',
			definition,
			policy,
			handler: (input) => Queue.offer(calls, input.id).pipe(Effect.asVoid),
		})
		assert.strictEqual(yield* replacement.processMailbox(receipt), true)
		assert.strictEqual(yield* Queue.take(calls), 'A')
		assert.strictEqual((yield* replacement.admit({ event: event('A') })).accepted, false)
		const store = yield* MailboxStore
		assert.strictEqual((yield* store.loadMailbox(receipt))?.state.active, null)
	}).pipe(Effect.provide(memory)),
)

it.effect('a completion racing admission retries without losing the new envelope', () =>
	Effect.gen(function* () {
		const reached = yield* Deferred.make<void>()
		const release = yield* Deferred.make<void>()
		const calls = yield* Queue.unbounded<string>()
		const delivery = bind({
			namespace: 'app',
			handlerId: 'race',
			definition,
			policy,
			handler: (input) => Queue.offer(calls, input.id).pipe(Effect.asVoid),
		})
		const receipt = yield* delivery.admit({ event: event('A') })
		const store = yield* MailboxStore
		const delayedCompletion = Layer.succeed(
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
		const running = yield* delivery
			.processMailbox(receipt)
			.pipe(Effect.provide(delayedCompletion), Effect.forkChild)
		yield* Deferred.await(reached)
		yield* delivery.admit({ event: event('B') })
		yield* Deferred.succeed(release, undefined)
		yield* Fiber.join(running)
		assert.deepStrictEqual(
			(yield* store.loadMailbox(receipt))?.state.pending.map((entry) => entry.eventId),
			['B'],
		)
		yield* delivery.processMailbox(receipt)
		assert.strictEqual(yield* Queue.take(calls), 'A')
		assert.strictEqual(yield* Queue.take(calls), 'B')
	}).pipe(Effect.provide(memory)),
)

it.effect('a Stop racing completion cannot retarget a successor on conflict', () =>
	Effect.gen(function* () {
		const reached = yield* Deferred.make<void>()
		const releaseControl = yield* Deferred.make<void>()
		const releaseHandler = yield* Deferred.make<void>()
		const calls = yield* Queue.unbounded<string>()
		const delivery = bind({
			namespace: 'app',
			handlerId: 'stop-race',
			definition,
			policy,
			handler: (input) =>
				Queue.offer(calls, input.id).pipe(
					Effect.andThen(input.id === 'A' ? Deferred.await(releaseHandler) : Effect.never),
				),
		})
		const receipt = yield* delivery.admit({ event: event('A') })
		const first = yield* delivery.processMailbox(receipt).pipe(Effect.forkChild)
		yield* Queue.take(calls)
		const store = yield* MailboxStore
		const delayedControl = Layer.succeed(
			MailboxStore,
			MailboxStore.of({
				loadMailbox: store.loadMailbox,
				commitMailbox: (input) =>
					Effect.gen(function* () {
						yield* Deferred.succeed(reached, undefined)
						yield* Deferred.await(releaseControl)
						return yield* store.commitMailbox(input)
					}),
			}),
		)
		const stop = yield* delivery
			.cancelActive({ ...receipt, controlId: 'stop' })
			.pipe(Effect.provide(delayedControl), Effect.forkChild)
		yield* Deferred.await(reached)
		yield* delivery.admit({ event: event('B') })
		yield* Deferred.succeed(releaseHandler, undefined)
		yield* Fiber.join(first)
		const second = yield* delivery.processMailbox(receipt).pipe(Effect.forkChild)
		yield* Queue.take(calls)
		yield* Deferred.succeed(releaseControl, undefined)
		assert.strictEqual(yield* Fiber.join(stop), false)
		assert.strictEqual((yield* store.loadMailbox(receipt))?.state.active?.cancelled, false)
		yield* Fiber.interrupt(second)
	}).pipe(Effect.provide(memory)),
)

it.effect('validates configuration, codec payloads, and bounded memory creation', () =>
	Effect.gen(function* () {
		const invalid = bind({
			namespace: 'app',
			handlerId: 'invalid',
			definition,
			policy: { ...policy, heartbeatMs: policy.leaseMs },
			handler: () => Effect.void,
		})
		assert.deepStrictEqual(
			yield* invalid.admit({ event: event('A') }).pipe(Effect.flip),
			DeliveryError.make({ reason: 'configuration' }),
		)
		const delivery = bind({
			namespace: 'app',
			handlerId: 'payload',
			definition,
			policy,
			handler: () => Effect.die('must not run'),
		})
		const receipt = yield* delivery.admit({ event: event('A') })
		const store = yield* MailboxStore
		const snapshot = yield* store.loadMailbox(receipt)
		assert.ok(snapshot !== undefined)
		yield* store.commitMailbox({
			key: receipt.key,
			expectedRevision: snapshot.revision,
			nextState: {
				...snapshot.state,
				pending: snapshot.state.pending.map((entry) => ({ ...entry, payload: '{"text":17}' })),
			},
		})
		assert.deepStrictEqual(
			yield* delivery.processMailbox(receipt).pipe(Effect.flip),
			DeliveryError.make({ reason: 'payload' }),
		)
		assert.deepStrictEqual(
			yield* store
				.commitMailbox({ key: 'another', expectedRevision: null, nextState: emptyMailbox() })
				.pipe(Effect.flip),
			MailboxStoreError.make({ operation: 'commit' }),
		)
	}).pipe(Effect.provide(layer({ maxMailboxes: 1 }))),
)
