import { assert, it } from '@effect/vitest'
import { Effect, Logger, Queue, Schema } from 'effect'
import { TestClock } from 'effect/testing'

import { bind, DeliveryError, HandlerFailure } from '../src/Delivery.js'
import { DeliveryPolicy } from '../src/DeliveryPolicy.js'
import { loadIngressAttribution, saveIngressAttribution } from '../src/IngressAttribution.js'
import { emptyMailbox } from '../src/Mailbox.js'
import { MailboxStore, MailboxStoreError } from '../src/MailboxStore.js'
import { layer } from '../src/memory.js'

const Event = Schema.Struct({ id: Schema.String })
const definition = {
	name: 'test.event',
	version: '1',
	provider: 'test',
	event: Event,
	resource: Schema.String,
	identify: (event: typeof Event.Type) => ({ installation: 'installation', eventId: event.id, resource: 'thread' }),
	resourceKey: (resource: string) => resource,
}
const policy = DeliveryPolicy.make({
	mode: 'queue',
	maxPayloadBytes: 4096,
	maxEnvelopes: 16,
	maxOutcomes: 64,
	retentionMs: 60000,
	maxAttempts: 3,
	retryBaseMs: 100,
	retryMaxMs: 1000,
	leaseMs: 1000,
	heartbeatMs: 100,
	conflictRetries: 8,
})
const identity = { namespace: 'app', provider: 'slack', installation: 'T1', eventId: 'E1' }
const memory = layer({ maxMailboxes: 100 })

it.effect(
	'corrupt attribution records and invalid inputs are classified without logging stored or supplied values',
	() =>
		Effect.gen(function* () {
			const logs: string[] = []
			const logger = Logger.layer([Logger.make((entry) => logs.push(JSON.stringify(entry.message)))])
			const store = yield* MailboxStore
			for (const version of [2, 3] as const) {
				const input = { ...identity, eventId: `private-record-${version}` }
				const encodedIdentity = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Array(Schema.String)))([
					input.namespace,
					input.provider,
					input.installation,
					input.eventId,
				])
				const key = `delivery-attribution:v1:${encodedIdentity}`
				yield* store.commitMailbox({ key, expectedRevision: null, nextState: { ...emptyMailbox(), version } })
				const before = yield* store.loadMailbox({ key })
				assert.deepStrictEqual(
					yield* loadIngressAttribution(input).pipe(Effect.provide(logger), Effect.flip),
					MailboxStoreError.make({ operation: 'load' }),
				)
				assert.deepStrictEqual(yield* store.loadMailbox({ key }), before)
			}
			assert.deepStrictEqual(
				yield* saveIngressAttribution({ ...identity, namespace: '', organizationId: 'private-owner' }).pipe(
					Effect.provide(logger),
					Effect.flip,
				),
				MailboxStoreError.make({ operation: 'commit' }),
			)
			assert.deepStrictEqual(
				yield* saveIngressAttribution({ ...identity, organizationId: '' }).pipe(
					Effect.provide(logger),
					Effect.flip,
				),
				MailboxStoreError.make({ operation: 'commit' }),
			)
			assert.strictEqual(yield* loadIngressAttribution(identity), undefined)
			for (const classification of [
				'incompatible_record',
				'missing_attribution',
				'invalid_identity',
				'invalid_attribution',
			])
				assert.ok(logs.some((log) => log.includes(classification)))
			assert.ok(logs.every((log) => !log.includes('private-')))
		}).pipe(Effect.provide(memory)),
)

it.effect('attribution capacity rejects a new decision without losing an existing association', () =>
	Effect.gen(function* () {
		yield* saveIngressAttribution({ ...identity, organizationId: 'A' })
		assert.deepStrictEqual(
			yield* saveIngressAttribution({ ...identity, eventId: 'E2', organizationId: 'B' }).pipe(Effect.flip),
			MailboxStoreError.make({ operation: 'commit' }),
		)
		assert.strictEqual(yield* loadIngressAttribution({ ...identity, eventId: 'E2' }), undefined)
		assert.deepStrictEqual(yield* saveIngressAttribution({ ...identity, organizationId: 'B' }), {
			organizationId: 'A',
		})
	}).pipe(Effect.provide(layer({ maxMailboxes: 1 }))),
)

it.effect('one guarded attribution wins competing fan-out decisions and survives rereading', () =>
	Effect.gen(function* () {
		assert.strictEqual(yield* loadIngressAttribution(identity), undefined)
		const results = yield* Effect.all(
			[
				saveIngressAttribution({ ...identity, organizationId: 'A' }),
				saveIngressAttribution({ ...identity, organizationId: 'B' }),
			],
			{ concurrency: 2 },
		)
		assert.deepStrictEqual(results[0], results[1])
		assert.deepStrictEqual(yield* loadIngressAttribution(identity), results[0])
		assert.deepStrictEqual(yield* saveIngressAttribution({ ...identity, organizationId: 'reassigned' }), results[0])
	}).pipe(Effect.provide(memory)),
)

it.effect('retry and duplicate admission preserve the saved organization', () =>
	Effect.gen(function* () {
		const observed = yield* Queue.unbounded<string>()
		const delivery = bind({
			namespace: 'app',
			handlerId: 'reply',
			definition,
			policy,
			handler: (_, context) =>
				Queue.offer(observed, context.organizationId).pipe(
					Effect.andThen(HandlerFailure.make({ retryable: true })),
				),
		})
		const receipt = yield* delivery.admit({ event: { id: '1' }, organizationId: 'A' })
		yield* delivery.admit({ event: { id: '1' }, organizationId: 'B' })
		yield* delivery.processMailbox(receipt)
		assert.strictEqual(yield* Queue.take(observed), 'A')
		yield* TestClock.adjust(100)
		yield* delivery.processMailbox(receipt)
		assert.strictEqual(yield* Queue.take(observed), 'A')
		const saved = yield* (yield* MailboxStore).loadMailbox(receipt)
		assert.strictEqual(saved?.state.active?.envelopes[0].organizationId, 'A')
		assert.strictEqual(saved?.state.active?.attempt, 2)
	}).pipe(Effect.provide(memory)),
)

it.effect('a queue never coalesces differently attributed events', () =>
	Effect.gen(function* () {
		const logs: string[] = []
		const logger = Logger.layer([Logger.make((entry) => logs.push(JSON.stringify(entry.message)))])
		const delivery = bind({
			namespace: 'app',
			handlerId: 'reply',
			definition,
			policy,
			handler: () => Effect.die('must not execute'),
		})
		const receipt = yield* delivery.admit({ event: { id: '1' }, organizationId: 'A' })
		yield* delivery.admit({ event: { id: '2' }, organizationId: 'B' })
		assert.deepStrictEqual(
			yield* delivery.processMailbox(receipt).pipe(Effect.provide(logger), Effect.flip),
			DeliveryError.make({ reason: 'configuration' }),
		)
		const saved = yield* (yield* MailboxStore).loadMailbox(receipt)
		assert.strictEqual(saved?.state.pending.length, 2)
		assert.strictEqual(saved?.state.active, null)
		assert.deepStrictEqual(logs, [
			'["Delivery organization attribution rejected",{"classification":"mixed_organization_batch"}]',
		])
	}).pipe(Effect.provide(memory)),
)

for (const version of [1, 2] as const) {
	it.effect(
		`v${version} legacy work is fixed-attributed before dispatch; custom lookup deployments fail closed`,
		() =>
			Effect.gen(function* () {
				const logs: string[] = []
				const logger = Logger.layer([Logger.make((entry) => logs.push(JSON.stringify(entry.message)))])
				const observed = yield* Queue.unbounded<string>()
				const registration = {
					namespace: 'app',
					handlerId: 'reply',
					definition,
					policy,
					handler: (_: typeof Event.Type, context: { readonly organizationId: string }) =>
						Queue.offer(observed, context.organizationId).pipe(Effect.asVoid),
				}
				const key = yield* bind(registration).keyFor({ event: { id: '1' } })
				const store = yield* MailboxStore
				yield* store.commitMailbox({
					key,
					expectedRevision: null,
					nextState: {
						...emptyMailbox(),
						version,
						pendingReadyAt: 0,
						readyAt: 0,
						pending: [
							{
								definition: definition.name,
								version: '1',
								eventId: '1',
								resource: '"thread"',
								payload: '{"id":"1"}',
								acceptedAt: 0,
							},
						],
					},
				})
				assert.deepStrictEqual(
					yield* bind({ ...registration, legacyOrganizationId: null })
						.processMailbox({ key })
						.pipe(Effect.provide(logger), Effect.flip),
					DeliveryError.make({ reason: 'configuration' }),
				)
				assert.strictEqual((yield* store.loadMailbox({ key }))?.state.version, version)
				assert.deepStrictEqual(logs, [
					'["Delivery organization attribution rejected",{"classification":"unattributed_legacy_batch"}]',
				])
				yield* bind({ ...registration, legacyOrganizationId: 'fixed' }).processMailbox({ key })
				assert.strictEqual(yield* Queue.take(observed), 'fixed')
				assert.strictEqual((yield* store.loadMailbox({ key }))?.state.version, 5)
			}).pipe(Effect.provide(memory)),
	)
}
