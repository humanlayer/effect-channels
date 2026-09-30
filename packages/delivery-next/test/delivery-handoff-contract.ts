/**
 * The contract for stores that support remote handoff: memory and the Durable Object.
 *
 * It drives a store through MailboxDelivery, MailboxProcessingBackend, and DeliveryControlBackend,
 * under the test clock. Stores that do not support handoff yet run `handoffUnsupportedContract`.
 */
import { it } from '@effect/vitest'
import { Clock, Effect, Option, type Layer } from 'effect'
import { TestClock } from 'effect/testing'
import { expect } from 'vite-plus/test'

import {
	BatchId,
	DELIVERY_RETENTION_MS,
	DeliveryAdmission,
	DeliveryControlBackend,
	DeliveryOutcome,
	DeliveryTerminal,
	ExternalLink,
	MailboxDelivery,
	MailboxProcessingBackend,
	RecoverableMailbox,
	Timestamp,
	makeDeliveryId,
	parseDeliveryId,
	type ClaimedMailboxBatch,
} from '../src'
import {
	claimAll,
	claimFrozen,
	deliver,
	event,
	findReady,
	findWaiting,
	leaseMs,
	mailboxKey,
	preparation,
	settle,
} from './backend-contract'

type HandoffStore = MailboxDelivery | MailboxProcessingBackend | DeliveryControlBackend

const reference = (claim: ClaimedMailboxBatch) =>
	Option.getOrThrow(parseDeliveryId(makeDeliveryId({ mailboxKey: claim.mailboxKey, batchId: claim.batchId })))

const handOff = (claim: ClaimedMailboxBatch) =>
	Effect.gen(function* () {
		const backend = yield* MailboxProcessingBackend
		const handedOffAt = Timestamp.make(yield* Clock.currentTimeMillis)
		yield* backend.handOffDelivery({ mailboxKey: claim.mailboxKey, claimId: claim.claimId, handedOffAt, links: [] })
	})

const status = (claim: ClaimedMailboxBatch, accessToken = claim.accessToken) =>
	Effect.gen(function* () {
		return yield* (yield* DeliveryControlBackend).readDeliveryStatus({ reference: reference(claim), accessToken })
	})

const completed = DeliveryTerminal.make({ outcome: DeliveryOutcome.cases.Completed.make({}), markdown: 'done' })
const failed = DeliveryTerminal.make({ outcome: DeliveryOutcome.cases.Failed.make({}) })

const finish = (claim: ClaimedMailboxBatch, terminal = completed, accessToken = claim.accessToken) =>
	Effect.gen(function* () {
		return yield* (yield* DeliveryControlBackend).recordDeliveryTerminal({
			reference: reference(claim),
			accessToken,
			terminal,
		})
	})

/** Claim the one waiting batch, prepare it, and hand it off. */
const claimAndHandOff = Effect.gen(function* () {
	const claim = yield* claimAll(yield* findWaiting)
	yield* (yield* MailboxProcessingBackend).prepareDelivery({
		mailboxKey: claim.mailboxKey,
		claimId: claim.claimId,
		prepared: preparation('onNewMention'),
	})
	yield* handOff(claim)
	return claim
})

export const deliveryHandoffContract = <E>(storeName: string, makeEmptyStore: () => Layer.Layer<HandoffStore, E>) => {
	const contract = <TestError>(name: string, test: Effect.Effect<void, TestError, HandoffStore>) =>
		it.effect(`${storeName} handoff: ${name}`, () => test.pipe(Effect.provide(makeEmptyStore())))

	contract(
		'a handed-off delivery holds the mailbox, with no lease, until its remote worker finishes',
		Effect.gen(function* () {
			yield* deliver('a')
			const claim = yield* claimAndHandOff
			expect((yield* status(claim)).stage).toEqual('ExternalCleaning')
			yield* settle(claim, 'completed')
			expect((yield* status(claim)).stage).toEqual('ExternalWaiting')
			expect((yield* status(claim)).supportedOperations).toEqual(['CreateMessage'])
			yield* deliver('follow-up')
			yield* TestClock.adjust(60 * leaseMs)
			expect(yield* findReady).toEqual([])

			expect((yield* finish(claim)).status).toEqual('accepted')
			expect((yield* status(claim)).stage).toEqual('Retired')
			expect((yield* status(claim)).outcome).toEqual(completed.outcome)
			expect((yield* findWaiting).waiting.count).toEqual(1)
		}),
	)

	contract(
		'a callback failure after handoff does not undo it or schedule a retry',
		Effect.gen(function* () {
			yield* deliver('a')
			const claim = yield* claimAndHandOff
			yield* settle(claim, { retryAfterMs: 10 })
			yield* TestClock.adjust(1_000)
			expect(yield* findReady).toEqual([])
			expect((yield* status(claim)).stage).toEqual('ExternalWaiting')
		}),
	)

	contract(
		'handing off twice is harmless',
		Effect.gen(function* () {
			yield* deliver('a')
			const claim = yield* claimAndHandOff
			yield* handOff(claim)
			expect((yield* status(claim)).stage).toEqual('ExternalCleaning')
		}),
	)

	contract(
		'keeps a result that arrives before handoff, and retires once the callback returns',
		Effect.gen(function* () {
			yield* deliver('a')
			const claim = yield* claimAll(yield* findWaiting)
			expect((yield* finish(claim)).status).toEqual('accepted')
			expect((yield* status(claim)).stage).toEqual('Finishing')
			yield* handOff(claim)
			yield* deliver('b')
			yield* settle(claim, 'completed')
			expect((yield* status(claim)).stage).toEqual('Retired')
			expect((yield* findWaiting).waiting.count).toEqual(1)
		}),
	)

	contract(
		'keeps a result that arrives while the handed-off callback is still returning',
		Effect.gen(function* () {
			yield* deliver('a')
			const claim = yield* claimAndHandOff
			yield* finish(claim)
			expect((yield* status(claim)).stage).toEqual('Finishing')
			yield* settle(claim, 'completed')
			expect((yield* status(claim)).stage).toEqual('Retired')
		}),
	)

	contract(
		'a lease that runs out after handoff does not run the callback again',
		Effect.gen(function* () {
			yield* deliver('a')
			const claim = yield* claimAndHandOff
			yield* TestClock.adjust(leaseMs)
			expect(yield* findReady).toEqual([RecoverableMailbox.make({ mailboxKey })])
			expect(Option.isNone(yield* claimFrozen)).toEqual(true)
			expect((yield* status(claim)).stage).toEqual('ExternalWaiting')
			const late = yield* settle(claim, 'completed').pipe(Effect.flip)
			expect(late._tag).toEqual('MailboxProcessingClaimLost')
		}),
	)

	contract(
		'a lease that runs out after a result retires the delivery without running it again',
		Effect.gen(function* () {
			yield* deliver('a')
			const claim = yield* claimAll(yield* findWaiting)
			yield* finish(claim)
			yield* TestClock.adjust(leaseMs)
			expect(Option.isNone(yield* claimFrozen)).toEqual(true)
			expect((yield* status(claim)).stage).toEqual('Retired')
		}),
	)

	contract(
		'a result that arrives while waiting to retry retires the delivery',
		Effect.gen(function* () {
			yield* deliver('a')
			const claim = yield* claimAll(yield* findWaiting)
			yield* settle(claim, { retryAfterMs: 5_000 })
			yield* finish(claim)
			expect((yield* status(claim)).stage).toEqual('Retired')
			yield* TestClock.adjust(5_000)
			expect(yield* findReady).toEqual([])
		}),
	)

	contract(
		'replays the same result and refuses a different one',
		Effect.gen(function* () {
			yield* deliver('a')
			const claim = yield* claimAndHandOff
			yield* settle(claim, 'completed')
			yield* finish(claim)
			expect((yield* finish(claim)).status).toEqual('already_recorded')
			expect((yield* finish(claim, failed).pipe(Effect.flip))._tag).toEqual('DeliveryTerminalConflict')
			const otherMarkdown = DeliveryTerminal.make({ ...completed, markdown: 'different' })
			expect((yield* finish(claim, otherMarkdown).pipe(Effect.flip))._tag).toEqual('DeliveryTerminalConflict')
		}),
	)

	contract(
		'a wrong token and an unknown delivery look the same',
		Effect.gen(function* () {
			yield* deliver('a')
			const claim = yield* claimAndHandOff
			expect((yield* status(claim, 'wrong-token').pipe(Effect.flip))._tag).toEqual('DeliveryNotFound')
			expect((yield* finish(claim, completed, 'wrong-token').pipe(Effect.flip))._tag).toEqual('DeliveryNotFound')
			const unknown = { ...claim, batchId: BatchId.make('unknown-batch') }
			expect((yield* status(unknown).pipe(Effect.flip))._tag).toEqual('DeliveryNotFound')
			expect((yield* status(claim)).stage).toEqual('ExternalCleaning')
		}),
	)

	contract(
		'a delivery that finished without handoff takes no remote result',
		Effect.gen(function* () {
			yield* deliver('a')
			const claim = yield* claimAll(yield* findWaiting)
			yield* settle(claim, 'completed')
			expect((yield* status(claim)).stage).toEqual('Retired')
			expect((yield* finish(claim).pipe(Effect.flip))._tag).toEqual('DeliveryClosed')
		}),
	)

	contract(
		'an interrupting event marks the active delivery and still waits its turn',
		Effect.gen(function* () {
			yield* deliver('a')
			const claim = yield* claimAndHandOff
			yield* settle(claim, 'completed')
			expect((yield* status(claim)).interruptRequested).toEqual(false)
			const delivery = yield* MailboxDelivery
			yield* delivery.deliver(DeliveryAdmission.make({ ...event('stop'), interrupt: true }))
			expect((yield* status(claim)).interruptRequested).toEqual(true)
			expect(yield* findReady).toEqual([])
			yield* finish(claim)
			expect((yield* status(claim)).interruptRequested).toEqual(true)
			expect((yield* findWaiting).waiting.count).toEqual(1)
		}),
	)

	contract(
		'an interrupting event at an idle mailbox marks nothing',
		Effect.gen(function* () {
			const delivery = yield* MailboxDelivery
			yield* delivery.deliver(DeliveryAdmission.make({ ...event('stop'), interrupt: true }))
			const claim = yield* claimAll(yield* findWaiting)
			expect((yield* status(claim)).interruptRequested).toEqual(false)
		}),
	)

	contract(
		'keeps a finished delivery readable for the retention period, then forgets it',
		Effect.gen(function* () {
			yield* deliver('a')
			const claim = yield* claimAndHandOff
			yield* settle(claim, 'completed')
			yield* finish(claim)
			yield* TestClock.adjust(DELIVERY_RETENTION_MS - 1)
			expect((yield* status(claim)).stage).toEqual('Retired')
			yield* TestClock.adjust(1)
			expect((yield* status(claim).pipe(Effect.flip))._tag).toEqual('DeliveryNotFound')
		}),
	)

	contract(
		'accepts links given at handoff, repeated or not',
		Effect.gen(function* () {
			yield* deliver('a')
			const claim = yield* claimAll(yield* findWaiting)
			const backend = yield* MailboxProcessingBackend
			const link = ExternalLink.make({ label: 'Run', url: 'https://example.com/run/1' })
			yield* backend.handOffDelivery({
				mailboxKey,
				claimId: claim.claimId,
				handedOffAt: Timestamp.make(0),
				links: [link, link],
			})
			expect((yield* status(claim)).stage).toEqual('ExternalCleaning')
		}),
	)
}

/** The contract for stores that keep batch identity but cannot hand deliveries off yet. */
export const handoffUnsupportedContract = <E>(
	storeName: string,
	makeEmptyStore: () => Layer.Layer<MailboxDelivery | MailboxProcessingBackend, E>,
) =>
	it.effect(`${storeName}: refuses handoff until it supports remote control`, () =>
		Effect.gen(function* () {
			yield* deliver('a')
			const claim = yield* claimAll(yield* findWaiting)
			const refused = yield* handOff(claim).pipe(Effect.flip)
			expect(refused._tag).toEqual('DeliveryHandoffUnsupported')
			yield* settle(claim, 'completed')
			expect(yield* findReady).toEqual([])
		}).pipe(Effect.provide(makeEmptyStore())),
	)
