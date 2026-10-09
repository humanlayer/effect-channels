import { it } from '@effect/vitest'
import {
	ClaimWaitingEvents,
	DeliveryAdmission,
	DeliveryControlBackend,
	CompleteDelivery,
	DeliveryOutputSettlement,
	DeliveryReceipt,
	MailboxDelivery,
	MailboxProcessing,
	MailboxProcessingAttemptCompleted,
	MailboxProcessingBackend,
	MailboxProcessingSummary,
	MailboxProcessingUnavailable,
	Timestamp,
	WaitingMailbox,
	deliveryMailboxKey,
	makeDeliveryId,
	parseDeliveryId,
} from '@humanlayer/channels-delivery'
import { Clock, Effect, Layer, Option, Schema } from 'effect'
import { TestClock } from 'effect/testing'

import { mailboxBackendContract, nextBatchIdentity, preparation } from '../../delivery/test/backend-contract'
import { deliveryHandoffContract } from '../../delivery/test/delivery-handoff-contract'
import { sequentialCallbackContract } from '../../sql/test-backends/sequential-callback-contract'
import {
	DeliveryControlBackendFromDurableObjectStorage,
	MailboxProcessingBackendFromDurableObjectStorage,
	makeDeliverFromDurableObjectStorage,
	MailboxStorage,
	makeMailboxAlarmHandler,
} from '../src'
import { DurableObjectFake, DurableObjectFakeAlarm } from './DurableObjectFake'

/** The Durable Object's own `deliver` RPC, called directly instead of through a Worker namespace. */
const MailboxDeliveryFromDurableObjectStorage = Layer.effect(
	MailboxDelivery,
	Effect.gen(function* () {
		const deliver = yield* makeDeliverFromDurableObjectStorage
		return MailboxDelivery.of({
			deliver: (admission) =>
				deliver(admission).pipe(
					Effect.map(({ accepted }) =>
						DeliveryReceipt.make({ mailboxKey: deliveryMailboxKey(admission), accepted }),
					),
				),
		})
	}),
)

/** `Layer.fresh` gives every test its own Durable Object, whatever layer memoization is in play. */
const makeEmptyStore = () =>
	Layer.mergeAll(
		MailboxDeliveryFromDurableObjectStorage,
		MailboxProcessingBackendFromDurableObjectStorage,
		DeliveryControlBackendFromDurableObjectStorage,
	).pipe(Layer.provideMerge(DurableObjectFake), Layer.fresh)

mailboxBackendContract('Durable Object', makeEmptyStore, { holdsManyMailboxes: false })
deliveryHandoffContract('Durable Object', makeEmptyStore)
sequentialCallbackContract('Durable Object', makeEmptyStore)

const leaseMs = 1_000

const event = (eventId: string) =>
	DeliveryAdmission.make({
		namespace: 'alarm',
		provider: 'example',
		installationId: 'installation',
		resourceId: 'thread-1',
		eventId,
		payload: { eventId },
	})

const mailboxKey = deliveryMailboxKey(event('any'))

it.effect('Durable Object: keeps the alarm on the time the mailbox is next due', ({ expect }) =>
	Effect.gen(function* () {
		const delivery = yield* MailboxDelivery
		const backend = yield* MailboxProcessingBackend
		const alarm = yield* DurableObjectFakeAlarm
		expect(yield* alarm.scheduledAt).toEqual(null)

		yield* TestClock.adjust(100)
		yield* delivery.deliver(event('a'))
		expect(yield* alarm.scheduledAt).toEqual(100)

		const [waiting] = (yield* backend.findReadyMailboxes).filter(Schema.is(WaitingMailbox))
		if (waiting === undefined) return yield* Effect.die(new Error('expected one waiting mailbox'))
		expect(yield* alarm.scheduledAt).toEqual(100)

		yield* backend.deferMailbox({
			mailboxKey,
			until: Timestamp.make(700),
			lastSequenceSeen: waiting.waiting.lastSequence,
		})
		expect(yield* alarm.scheduledAt).toEqual(700)

		yield* TestClock.adjust(600)
		const claim = Option.getOrThrow(
			yield* backend.claimMailbox(
				ClaimWaitingEvents.make({
					mailboxKey,
					upToSequence: waiting.waiting.lastSequence,
					leaseMs,
					...nextBatchIdentity(),
				}),
			),
		)
		expect(yield* alarm.scheduledAt).toEqual(700 + leaseMs)

		yield* TestClock.adjust(400)
		yield* backend.renewClaim({ mailboxKey, claimId: claim.claimId, leaseMs })
		expect(yield* alarm.scheduledAt).toEqual(1_100 + leaseMs)

		yield* backend.recordProcessingAttemptResult({
			claim,
			result: MailboxProcessingAttemptCompleted.make({}),
			finishedAt: Timestamp.make(yield* Clock.currentTimeMillis),
		})
		expect(yield* alarm.scheduledAt).toEqual(null)
	}).pipe(Effect.provide(makeEmptyStore())),
)

it.effect(
	'Durable Object: an event that arrives during a run leaves the lease alarm alone, then wakes the mailbox',
	({ expect }) =>
		Effect.gen(function* () {
			const delivery = yield* MailboxDelivery
			const backend = yield* MailboxProcessingBackend
			const alarm = yield* DurableObjectFakeAlarm

			yield* delivery.deliver(event('a'))
			const claim = Option.getOrThrow(
				yield* backend.claimMailbox(
					ClaimWaitingEvents.make({ mailboxKey, upToSequence: 0, leaseMs, ...nextBatchIdentity() }),
				),
			)
			yield* TestClock.adjust(250)
			yield* delivery.deliver(event('during-run'))
			expect(yield* alarm.scheduledAt).toEqual(leaseMs)

			yield* backend.recordProcessingAttemptResult({
				claim,
				result: MailboxProcessingAttemptCompleted.make({}),
				finishedAt: Timestamp.make(yield* Clock.currentTimeMillis),
			})
			expect(yield* alarm.scheduledAt).toEqual(250)
		}).pipe(Effect.provide(makeEmptyStore())),
)

it.effect(
	'Durable Object: a handed-off delivery clears the alarm; its result wakes output, and settled output wakes the events that wait',
	({ expect }) =>
		Effect.gen(function* () {
			const delivery = yield* MailboxDelivery
			const backend = yield* MailboxProcessingBackend
			const control = yield* DeliveryControlBackend
			const alarm = yield* DurableObjectFakeAlarm

			yield* delivery.deliver(event('a'))
			const claim = Option.getOrThrow(
				yield* backend.claimMailbox(
					ClaimWaitingEvents.make({ mailboxKey, upToSequence: 0, leaseMs, ...nextBatchIdentity() }),
				),
			)
			yield* backend.prepareDelivery({
				mailboxKey,
				claimId: claim.claimId,
				prepared: preparation('onMention'),
				callbackAccessTokens: [claim.accessToken],
			})
			yield* backend.handOffDelivery({
				mailboxKey,
				claimId: claim.claimId,
				handedOffAt: Timestamp.make(0),
				links: [],
			})
			expect(yield* alarm.scheduledAt).toEqual(leaseMs)

			yield* backend.recordProcessingAttemptResult({
				claim,
				result: MailboxProcessingAttemptCompleted.make({}),
				finishedAt: Timestamp.make(yield* Clock.currentTimeMillis),
			})
			expect(yield* alarm.scheduledAt).toEqual(null)

			yield* TestClock.adjust(300)
			yield* delivery.deliver(event('while-waiting'))
			expect(yield* alarm.scheduledAt).toEqual(null)

			yield* TestClock.adjust(200)
			const reference = Option.getOrThrow(parseDeliveryId(makeDeliveryId({ mailboxKey, batchId: claim.batchId })))
			yield* control.applyDeliveryMutation({
				reference,
				accessToken: claim.accessToken,
				mutation: CompleteDelivery.make({}),
			})
			expect(yield* alarm.scheduledAt).toEqual(500)

			const output = Option.getOrThrow(
				yield* backend.claimDeliveryOutput({
					mailboxKey,
					leaseMs,
					idempotencyKey: '00000000-0000-4000-8000-000000000001',
				}),
			)
			expect(yield* alarm.scheduledAt).toEqual(500 + leaseMs)
			yield* TestClock.adjust(100)
			yield* backend.settleDeliveryOutput({
				mailboxKey,
				operationId: output.operationId,
				claimId: output.claimId,
				settlement: DeliveryOutputSettlement.cases.Applied.make({}),
				settledAt: Timestamp.make(yield* Clock.currentTimeMillis),
			})
			expect(yield* alarm.scheduledAt).toEqual(600)
		}).pipe(Effect.provide(makeEmptyStore())),
)

it.effect('Durable Object: a new event puts back the alarm of a busy mailbox that lost it', ({ expect }) =>
	Effect.gen(function* () {
		const delivery = yield* MailboxDelivery
		const backend = yield* MailboxProcessingBackend
		const alarm = yield* DurableObjectFakeAlarm
		yield* delivery.deliver(event('a'))
		yield* backend.claimMailbox(
			ClaimWaitingEvents.make({ mailboxKey, upToSequence: 0, leaseMs, ...nextBatchIdentity() }),
		)
		expect(yield* alarm.scheduledAt).toEqual(leaseMs)
		yield* alarm.clearAsCloudflareDoesBeforeTheHandler
		yield* TestClock.adjust(leaseMs * 2)
		yield* delivery.deliver(event('b'))
		expect(yield* alarm.scheduledAt).toEqual(leaseMs)
	}).pipe(Effect.provide(makeEmptyStore())),
)

/** A processing pass that does nothing, as when the storage refuses the claim and the mailbox is skipped. */
const processingThatSkipsEverything = Layer.succeed(
	MailboxProcessing,
	MailboxProcessing.of({
		processReady: Effect.succeed(MailboxProcessingSummary.make({ claimed: 0, deferred: 0, output: 0 })),
	}),
)

const processingThatFails = Layer.succeed(
	MailboxProcessing,
	MailboxProcessing.of({
		processReady: Effect.fail(new MailboxProcessingUnavailable({ reason: 'simulated_look_failure' })),
	}),
)

const alarmHandlerOver = (processing: Layer.Layer<MailboxProcessing>) =>
	Effect.gen(function* () {
		const delivery = yield* MailboxDelivery
		const alarm = yield* DurableObjectFakeAlarm
		const runMailboxAlarm = yield* makeMailboxAlarmHandler({ rearmAfterMs: 1_000 })
		const storage = yield* MailboxStorage
		return { delivery, alarm, runMailboxAlarm, storage }
	}).pipe(Effect.provide(Layer.merge(processing, makeEmptyStore())))

it.effect('Durable Object alarm: puts the alarm back when a due mailbox was not worked on', ({ expect }) =>
	Effect.gen(function* () {
		const { delivery, alarm, runMailboxAlarm } = yield* alarmHandlerOver(processingThatSkipsEverything)
		yield* delivery.deliver(event('a'))
		yield* TestClock.adjust(5_000)
		yield* alarm.clearAsCloudflareDoesBeforeTheHandler
		yield* runMailboxAlarm()
		expect(yield* alarm.scheduledAt).toBe(6_000)
	}),
)

it.effect('Durable Object alarm: puts the alarm back when the pass fails, and does not fail itself', ({ expect }) =>
	Effect.gen(function* () {
		const { delivery, alarm, runMailboxAlarm } = yield* alarmHandlerOver(processingThatFails)
		yield* delivery.deliver(event('a'))
		yield* alarm.clearAsCloudflareDoesBeforeTheHandler
		yield* runMailboxAlarm()
		expect(yield* alarm.scheduledAt).toBe(1_000)
	}),
)

it.effect(
	'Durable Object alarm: leaves a quiet mailbox, and a future alarm the store already set, alone',
	({ expect }) =>
		Effect.gen(function* () {
			const { delivery, alarm, runMailboxAlarm, storage } = yield* alarmHandlerOver(processingThatSkipsEverything)
			yield* runMailboxAlarm()
			expect(yield* alarm.scheduledAt).toBe(null)
			yield* TestClock.adjust(10_000)
			yield* delivery.deliver(event('a'))
			yield* storage.setAlarm(20_000)
			yield* runMailboxAlarm()
			expect(yield* alarm.scheduledAt).toBe(20_000)
		}),
)

it.effect(
	'Durable Object alarm: moves forward an alarm the handler left at or before now, which Cloudflare would drop',
	({ expect }) =>
		Effect.gen(function* () {
			const { delivery, alarm, runMailboxAlarm } = yield* alarmHandlerOver(processingThatSkipsEverything)
			yield* TestClock.adjust(5_000)
			yield* delivery.deliver(event('a'))
			expect(yield* alarm.scheduledAt).toBe(5_000)
			yield* runMailboxAlarm()
			expect(yield* alarm.scheduledAt).toBe(6_000)
		}),
)
