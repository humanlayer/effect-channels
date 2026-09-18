import { it } from '@effect/vitest'
import {
	ClaimWaitingEvents,
	DeliveryAdmission,
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
} from '@humanlayer/channels-delivery-next'
import { RuntimeContext } from 'alchemy/RuntimeContext'
import { Clock, Effect, Layer, Option, Schema } from 'effect'
import { TestClock } from 'effect/testing'
import { expect } from 'vite-plus/test'

import { mailboxBackendContract } from '../../delivery-next/test/backend-contract'
import {
	MailboxProcessingBackendFromDurableObjectStorage,
	makeDeliverFromDurableObjectStorage,
	makeMailboxAlarmHandler,
} from '../src'
import { DurableObjectFake, DurableObjectFakeAlarm } from './DurableObjectFake'

/** The Durable Object's own `deliver` RPC, called directly instead of through a Worker namespace. */
const MailboxDeliveryFromDurableObjectStorage = Layer.effect(
	MailboxDelivery,
	Effect.gen(function* () {
		const deliver = yield* makeDeliverFromDurableObjectStorage
		const runtimeContext = yield* RuntimeContext
		return MailboxDelivery.of({
			deliver: (admission) =>
				deliver(admission).pipe(
					Effect.map(({ accepted }) =>
						DeliveryReceipt.make({ mailboxKey: deliveryMailboxKey(admission), accepted }),
					),
					Effect.provideService(RuntimeContext, runtimeContext),
				),
		})
	}),
)

/** `Layer.fresh` gives every test its own Durable Object, whatever layer memoization is in play. */
const makeEmptyStore = () =>
	Layer.mergeAll(MailboxDeliveryFromDurableObjectStorage, MailboxProcessingBackendFromDurableObjectStorage).pipe(
		Layer.provideMerge(DurableObjectFake),
		Layer.fresh,
	)

mailboxBackendContract('Durable Object', makeEmptyStore, { holdsManyMailboxes: false })

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

it.effect('Durable Object: keeps the alarm on the time the mailbox is next due', () =>
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
				ClaimWaitingEvents.make({ mailboxKey, upToSequence: waiting.waiting.lastSequence, leaseMs }),
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
	() =>
		Effect.gen(function* () {
			const delivery = yield* MailboxDelivery
			const backend = yield* MailboxProcessingBackend
			const alarm = yield* DurableObjectFakeAlarm

			yield* delivery.deliver(event('a'))
			const claim = Option.getOrThrow(
				yield* backend.claimMailbox(ClaimWaitingEvents.make({ mailboxKey, upToSequence: 0, leaseMs })),
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

/** A processing pass that does nothing, as when the storage refuses the claim and the mailbox is skipped. */
const processingThatSkipsEverything = Layer.succeed(
	MailboxProcessing,
	MailboxProcessing.of({ processReady: Effect.succeed(MailboxProcessingSummary.make({ claimed: 0, deferred: 0 })) }),
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
		return { delivery, alarm, runMailboxAlarm }
	}).pipe(Effect.provide(processing), Effect.provide(makeEmptyStore()))

it.effect('Durable Object alarm: puts the alarm back when a due mailbox was not worked on', () =>
	Effect.gen(function* () {
		const { delivery, alarm, runMailboxAlarm } = yield* alarmHandlerOver(processingThatSkipsEverything)
		yield* delivery.deliver(event('a'))
		yield* TestClock.adjust(5_000)
		yield* alarm.clearAsCloudflareDoesBeforeTheHandler
		yield* runMailboxAlarm
		expect(yield* alarm.scheduledAt).toBe(6_000)
	}),
)

it.effect('Durable Object alarm: puts the alarm back when the pass fails, and does not fail itself', () =>
	Effect.gen(function* () {
		const { delivery, alarm, runMailboxAlarm } = yield* alarmHandlerOver(processingThatFails)
		yield* delivery.deliver(event('a'))
		yield* alarm.clearAsCloudflareDoesBeforeTheHandler
		yield* runMailboxAlarm
		expect(yield* alarm.scheduledAt).toBe(1_000)
	}),
)

it.effect('Durable Object alarm: leaves a quiet mailbox, and an alarm the store already set, alone', () =>
	Effect.gen(function* () {
		const { delivery, alarm, runMailboxAlarm } = yield* alarmHandlerOver(processingThatSkipsEverything)
		yield* runMailboxAlarm
		expect(yield* alarm.scheduledAt).toBe(null)
		yield* delivery.deliver(event('a'))
		yield* runMailboxAlarm
		expect(yield* alarm.scheduledAt).toBe(0)
	}),
)
