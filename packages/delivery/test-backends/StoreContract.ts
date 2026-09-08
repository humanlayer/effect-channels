import { assert } from '@effect/vitest'
import { Clock, Deferred, Effect, Exit, Fiber, Queue, Schema } from 'effect'

import { bind, DeliveryError } from '../src/Delivery.js'
import { DeliveryPolicy } from '../src/DeliveryPolicy.js'
import type { EventDefinition } from '../src/EventDefinition.js'
import { emptyMailbox, MailboxState } from '../src/Mailbox.js'
import { MailboxReadiness, MailboxStore } from '../src/MailboxStore.js'

export const storedState = MailboxState.make({
	...emptyMailbox(),
	pending: [
		{
			definition: 'backend.test',
			version: '1',
			eventId: 'A',
			resource: 'root',
			payload: '"persisted"',
			acceptedAt: 0,
		},
	],
	readyAt: 10,
})

export const storageContract = Effect.gen(function* () {
	const store = yield* MailboxStore
	const readiness = yield* MailboxReadiness
	const key = 'contract%_!\\:cas'
	const input = { key, expectedRevision: null, nextState: storedState }
	assert.strictEqual(yield* store.loadMailbox({ key }), undefined)
	const creates = yield* Effect.all([store.commitMailbox(input), store.commitMailbox(input)], { concurrency: 2 })
	assert.deepStrictEqual([...creates].sort(), ['committed', 'conflict'])
	assert.deepStrictEqual(yield* store.loadMailbox({ key }), { revision: 0, state: storedState })
	assert.deepStrictEqual(yield* readiness.scanReady({ prefix: key, now: 9, limit: 1 }), [])
	assert.deepStrictEqual(yield* readiness.scanReady({ prefix: key, now: 10, limit: 1 }), [key])
	const updates = yield* Effect.all(
		[
			store.commitMailbox({ key, expectedRevision: 0, nextState: { ...storedState, readyAt: 50 } }),
			store.commitMailbox({ key, expectedRevision: 0, nextState: { ...storedState, readyAt: null } }),
		],
		{ concurrency: 2 },
	)
	assert.deepStrictEqual([...updates].sort(), ['committed', 'conflict'])
	const winner = yield* store.loadMailbox({ key })
	assert.ok(winner !== undefined)
	assert.strictEqual(winner.revision, 1)
	assert.deepStrictEqual(winner.state.pending, storedState.pending)
	assert.strictEqual(winner.state.readyAt, updates[0] === 'committed' ? 50 : null)
	assert.deepStrictEqual(
		yield* readiness.scanReady({ prefix: key, now: 50, limit: 1 }),
		winner.state.readyAt === null ? [] : [key],
	)
	assert.strictEqual(yield* store.commitMailbox({ key, expectedRevision: 0, nextState: emptyMailbox() }), 'conflict')
	assert.deepStrictEqual(yield* store.loadMailbox({ key }), winner)
	assert.strictEqual(
		yield* store.commitMailbox({ key, expectedRevision: 1, nextState: { ...storedState, readyAt: 25 } }),
		'committed',
	)
	assert.deepStrictEqual(yield* readiness.scanReady({ prefix: key, now: 24, limit: 1 }), [])
	assert.deepStrictEqual(yield* readiness.scanReady({ prefix: key, now: 25, limit: 1 }), [key])
	assert.strictEqual(yield* store.commitMailbox({ key, expectedRevision: 2, nextState: emptyMailbox() }), 'committed')
	assert.deepStrictEqual(yield* readiness.scanReady({ prefix: key, now: 100, limit: 1 }), [])
	assert.deepStrictEqual(yield* store.loadMailbox({ key }), { revision: 3, state: emptyMailbox() })
	for (let index = 0; index < 32; index++) {
		yield* store.commitMailbox({
			key: `unrelated:${index}`,
			expectedRevision: null,
			nextState: { ...storedState, readyAt: -1 },
		})
	}
	const literalKey = 'literal%_!\\:wanted'
	yield* store.commitMailbox({
		key: 'literalXXa!\\:decoy',
		expectedRevision: null,
		nextState: { ...storedState, readyAt: -1 },
	})
	yield* store.commitMailbox({ key: literalKey, expectedRevision: null, nextState: storedState })
	assert.deepStrictEqual(yield* readiness.scanReady({ prefix: 'literal%_!\\:', now: 10, limit: 1 }), [literalKey])
})

const Event = Schema.Struct({ id: Schema.String })
const definition: EventDefinition<typeof Event, typeof Schema.String> = {
	name: 'backend.test',
	version: '1',
	provider: 'test',
	event: Event,
	resource: Schema.String,
	identify: (event) => ({ eventId: event.id, installation: 'T', resource: 'root' }),
	resourceKey: (resource) => resource,
}
export const policy = DeliveryPolicy.make({
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

export const staleAttemptContract = Effect.gen(function* () {
	const started = yield* Deferred.make<void>()
	const release = yield* Deferred.make<void>()
	const delivery = bind({
		namespace: 'backend-contract',
		handlerId: 'stale',
		definition,
		policy,
		handler: () => Deferred.succeed(started, undefined).pipe(Effect.andThen(Deferred.await(release))),
	})
	const receipt = yield* delivery.admit({ event: { id: 'A' } })
	const first = yield* delivery.processMailbox(receipt).pipe(Effect.exit, Effect.forkChild)
	yield* Deferred.await(started)
	const store = yield* MailboxStore
	const current = yield* store.loadMailbox(receipt)
	assert.ok(current !== undefined && current.state.active !== null)
	assert.strictEqual(
		yield* store.commitMailbox({
			key: receipt.key,
			expectedRevision: current.revision,
			nextState: { ...current.state, active: { ...current.state.active, owner: current.revision + 1 } },
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
	assert.strictEqual(final?.state.active?.owner, current.revision + 1)
	assert.strictEqual(final?.state.outcomes.length, 0)
})

export const interruptForReconstruction = Effect.gen(function* () {
	const calls = yield* Queue.unbounded<string>()
	const original = bind({
		namespace: 'backend-contract',
		handlerId: 'reconstruction',
		definition,
		policy,
		handler: (event) => Queue.offer(calls, event.id).pipe(Effect.andThen(Effect.never)),
	})
	const receipt = yield* original.admit({ event: { id: 'recover' } })
	const running = yield* original.processMailbox(receipt).pipe(Effect.forkChild)
	assert.strictEqual(yield* Queue.take(calls), 'recover')
	yield* Fiber.interrupt(running)
	const store = yield* MailboxStore
	assert.strictEqual((yield* store.loadMailbox(receipt))?.state.active?.attempt, 1)
	return receipt
})

export const resumeAfterReconstruction = Effect.fn('test.delivery.resume')(function* (input: { readonly key: string }) {
	const calls = yield* Queue.unbounded<string>()
	const replacement = bind({
		namespace: 'backend-contract',
		handlerId: 'reconstruction',
		definition,
		policy,
		handler: (event) => Queue.offer(calls, event.id).pipe(Effect.asVoid),
	})
	const readiness = yield* MailboxReadiness
	assert.deepStrictEqual(
		yield* readiness.scanReady({ prefix: input.key, now: yield* Clock.currentTimeMillis, limit: 1 }),
		[input.key],
	)
	assert.strictEqual(yield* replacement.processMailbox(input), true)
	assert.strictEqual(yield* Queue.take(calls), 'recover')
	assert.strictEqual((yield* replacement.admit({ event: { id: 'recover' } })).accepted, false)
	const store = yield* MailboxStore
	const final = yield* store.loadMailbox(input)
	assert.strictEqual(final?.state.active, null)
	assert.strictEqual(final?.state.outcomes.length, 1)
	assert.deepStrictEqual(
		yield* readiness.scanReady({ prefix: input.key, now: yield* Clock.currentTimeMillis, limit: 1 }),
		[],
	)
})
