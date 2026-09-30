/**
 * The contract every mailbox store must pass: SQL, Redis, the Durable Object, and the test memory store.
 *
 * It drives a store only through MailboxDelivery and MailboxProcessingBackend, under the test clock,
 * so the stores cannot drift apart in how they look, take, defer, renew and settle.
 *
 * Store tests call `mailboxBackendContract` with a layer that starts from an empty store.
 */
import { it } from '@effect/vitest'
import { Clock, Effect, Match, Option, type Layer } from 'effect'
import { TestClock } from 'effect/testing'
import { expect } from 'vite-plus/test'

import {
	BatchId,
	ClaimFrozenBatch,
	ClaimWaitingEvents,
	DeliveryAccessToken,
	PreparedDeliveryInvocation,
	DeliveryAdmission,
	MailboxDelivery,
	MailboxProcessingAttemptCompleted,
	MailboxProcessingAttemptRetryableFailure,
	MailboxProcessingBackend,
	RecoverableMailbox,
	Timestamp,
	deliveryMailboxKey,
	type ClaimedMailboxBatch,
	type WaitingMailbox,
} from '../src'

export const leaseMs = 1_000

export const event = (eventId: string, resourceId = 'thread-1') =>
	DeliveryAdmission.make({
		namespace: 'contract',
		provider: 'example',
		installationId: 'installation',
		resourceId,
		eventId,
		payload: { eventId },
	})

export const mailboxKey = deliveryMailboxKey(event('any'))

export const deliver = (eventId: string, resourceId?: string) =>
	Effect.gen(function* () {
		return yield* (yield* MailboxDelivery).deliver(event(eventId, resourceId))
	})

export const findReady = Effect.gen(function* () {
	return yield* (yield* MailboxProcessingBackend).findReadyMailboxes
})

/** The one waiting mailbox the test expects to be ready. Dies loudly when the store reports anything else. */
export const findWaiting = Effect.gen(function* () {
	const ready = yield* findReady
	const [only] = ready.flatMap((mailbox) =>
		Match.value(mailbox).pipe(
			Match.tag('WaitingMailbox', (waiting) => [waiting]),
			Match.orElse(() => []),
		),
	)
	if (ready.length !== 1 || only === undefined) {
		return yield* Effect.die(new Error(`expected one waiting mailbox, got ${ready.length} ready mailboxes`))
	}
	return only
})

let batchesMade = 0

/** A new batch's identity, as mailbox processing would make it. */
export const nextBatchIdentity = () => {
	batchesMade += 1
	return {
		batchId: BatchId.make(`batch-${batchesMade}`),
		accessToken: DeliveryAccessToken.make(`token-${batchesMade}`),
	}
}

export const claimUpTo = (upToSequence: number, identity = nextBatchIdentity()) =>
	Effect.gen(function* () {
		const backend = yield* MailboxProcessingBackend
		return yield* backend.claimMailbox(ClaimWaitingEvents.make({ mailboxKey, upToSequence, leaseMs, ...identity }))
	})

export const claimFrozen = Effect.gen(function* () {
	const backend = yield* MailboxProcessingBackend
	return yield* backend.claimMailbox(ClaimFrozenBatch.make({ mailboxKey, leaseMs }))
})

export const claimAll = (waiting: WaitingMailbox) =>
	claimUpTo(waiting.waiting.lastSequence).pipe(Effect.map(Option.getOrThrow))

export const settle = (claim: ClaimedMailboxBatch, result: 'completed' | { readonly retryAfterMs: number }) =>
	Effect.gen(function* () {
		const backend = yield* MailboxProcessingBackend
		const finishedAt = Timestamp.make(yield* Clock.currentTimeMillis)
		return yield* backend.recordProcessingAttemptResult({
			claim,
			finishedAt,
			result:
				result === 'completed'
					? MailboxProcessingAttemptCompleted.make({})
					: MailboxProcessingAttemptRetryableFailure.make({
							safeCode: 'temporary',
							retryAfterMs: result.retryAfterMs,
						}),
		})
	})

export const eventIds = (claim: ClaimedMailboxBatch) => claim.admissions.map(({ eventId }) => eventId)

export const preparation = (callback: string) =>
	PreparedDeliveryInvocation.make({
		callback,
		presentationVersion: 1,
		destination: { thread: 'thread-1' },
		supportedOperations: ['CreateMessage'],
	})

export const mailboxBackendContract = <E>(
	storeName: string,
	makeEmptyStore: () => Layer.Layer<MailboxDelivery | MailboxProcessingBackend, E>,
	/** A Durable Object store holds one mailbox, so it skips the tests that need several. */
	options: { readonly holdsManyMailboxes: boolean } = { holdsManyMailboxes: true },
) => {
	const contract = <TestError>(
		name: string,
		test: Effect.Effect<void, TestError, MailboxDelivery | MailboxProcessingBackend>,
	) => it.effect(`${storeName}: ${name}`, () => test.pipe(Effect.provide(makeEmptyStore())))

	contract(
		'reports what is waiting, with arrival times and growing sequence numbers',
		Effect.gen(function* () {
			expect(yield* findReady).toEqual([])
			yield* deliver('a')
			yield* TestClock.adjust(500)
			yield* deliver('b')
			const mailbox = yield* findWaiting
			expect(mailbox.mailboxKey).toEqual(mailboxKey)
			expect(mailbox.provider).toEqual('example')
			expect(mailbox.waiting.count).toEqual(2)
			expect(mailbox.waiting.firstArrivedAt).toEqual(0)
			expect(mailbox.waiting.lastArrivedAt).toEqual(500)
			expect(mailbox.waiting.firstSequence < mailbox.waiting.lastSequence).toEqual(true)
		}),
	)

	contract(
		'accepts an event once',
		Effect.gen(function* () {
			expect((yield* deliver('a')).accepted).toEqual(true)
			expect((yield* deliver('a')).accepted).toEqual(false)
			expect((yield* findWaiting).waiting.count).toEqual(1)
		}),
	)

	contract(
		'claims only the events at or below the named sequence, in order',
		Effect.gen(function* () {
			yield* deliver('a')
			yield* deliver('b')
			yield* deliver('c')
			const mailbox = yield* findWaiting
			const first = Option.getOrThrow(yield* claimUpTo(mailbox.waiting.firstSequence))
			expect(eventIds(first)).toEqual(['a'])
			expect(first.attempt).toEqual(1)
			expect(yield* findReady).toEqual([])
			yield* settle(first, 'completed')
			const rest = yield* claimAll(yield* findWaiting)
			expect(eventIds(rest)).toEqual(['b', 'c'])
		}),
	)

	contract(
		'leaves an event that arrived after the look for the next batch',
		Effect.gen(function* () {
			yield* deliver('a')
			const seen = yield* findWaiting
			yield* deliver('late')
			const claim = yield* claimAll(seen)
			expect(eventIds(claim)).toEqual(['a'])
			yield* settle(claim, 'completed')
			expect(eventIds(yield* claimAll(yield* findWaiting))).toEqual(['late'])
		}),
	)

	contract(
		'gives a mailbox to one claimer only',
		Effect.gen(function* () {
			yield* deliver('a')
			const mailbox = yield* findWaiting
			expect(Option.isSome(yield* claimUpTo(mailbox.waiting.lastSequence))).toEqual(true)
			expect(Option.isNone(yield* claimUpTo(mailbox.waiting.lastSequence))).toEqual(true)
			expect(Option.isNone(yield* claimFrozen)).toEqual(true)
		}),
	)

	contract(
		'refuses to claim a frozen batch from an idle mailbox',
		Effect.gen(function* () {
			yield* deliver('a')
			expect(Option.isNone(yield* claimFrozen)).toEqual(true)
			expect((yield* findWaiting).waiting.count).toEqual(1)
		}),
	)

	contract(
		'hides a deferred mailbox until the named time',
		Effect.gen(function* () {
			yield* deliver('a')
			const mailbox = yield* findWaiting
			const backend = yield* MailboxProcessingBackend
			yield* backend.deferMailbox({
				mailboxKey,
				until: Timestamp.make(2_000),
				lastSequenceSeen: mailbox.waiting.lastSequence,
			})
			expect(yield* findReady).toEqual([])
			yield* TestClock.adjust(1_999)
			expect(yield* findReady).toEqual([])
			yield* TestClock.adjust(1)
			expect((yield* findWaiting).waiting.count).toEqual(1)
		}),
	)

	contract(
		'wakes a deferred mailbox when a new event arrives',
		Effect.gen(function* () {
			yield* deliver('a')
			const mailbox = yield* findWaiting
			const backend = yield* MailboxProcessingBackend
			yield* backend.deferMailbox({
				mailboxKey,
				until: Timestamp.make(2_000),
				lastSequenceSeen: mailbox.waiting.lastSequence,
			})
			yield* TestClock.adjust(500)
			yield* deliver('b')
			expect((yield* findWaiting).waiting.count).toEqual(2)
		}),
	)

	contract(
		'ignores a deferral that was decided before a newer event arrived',
		Effect.gen(function* () {
			yield* deliver('a')
			const seen = yield* findWaiting
			yield* deliver('b')
			const backend = yield* MailboxProcessingBackend
			yield* backend.deferMailbox({
				mailboxKey,
				until: Timestamp.make(2_000),
				lastSequenceSeen: seen.waiting.lastSequence,
			})
			expect((yield* findWaiting).waiting.count).toEqual(2)
		}),
	)

	contract(
		'offers an unrenewed claim for recovery once its lease runs out, with the same frozen batch',
		Effect.gen(function* () {
			yield* deliver('a')
			const abandoned = yield* claimAll(yield* findWaiting)
			yield* deliver('during-run')
			yield* TestClock.adjust(leaseMs - 1)
			expect(yield* findReady).toEqual([])
			yield* TestClock.adjust(1)
			expect(yield* findReady).toEqual([RecoverableMailbox.make({ mailboxKey })])
			const recovered = Option.getOrThrow(yield* claimFrozen)
			expect(eventIds(recovered)).toEqual(['a'])
			expect(recovered.attempt).toEqual(2)
			expect(recovered.claimId === abandoned.claimId).toEqual(false)

			const backend = yield* MailboxProcessingBackend
			const lateRenewal = yield* backend
				.renewClaim({ mailboxKey, claimId: abandoned.claimId, leaseMs })
				.pipe(Effect.flip)
			expect(lateRenewal._tag).toEqual('MailboxProcessingClaimLost')
			const lateResult = yield* settle(abandoned, 'completed').pipe(Effect.flip)
			expect(lateResult._tag).toEqual('MailboxProcessingClaimLost')

			yield* settle(recovered, 'completed')
			expect(eventIds(yield* claimAll(yield* findWaiting))).toEqual(['during-run'])
		}),
	)

	contract(
		'keeps a renewed claim away from recovery',
		Effect.gen(function* () {
			yield* deliver('a')
			const claim = yield* claimAll(yield* findWaiting)
			const backend = yield* MailboxProcessingBackend
			yield* TestClock.adjust(900)
			yield* backend.renewClaim({ mailboxKey, claimId: claim.claimId, leaseMs })
			yield* TestClock.adjust(999)
			expect(yield* findReady).toEqual([])
			yield* TestClock.adjust(1)
			expect(yield* findReady).toEqual([RecoverableMailbox.make({ mailboxKey })])
		}),
	)

	contract(
		'retries the same frozen batch after the retry delay, ahead of newer events',
		Effect.gen(function* () {
			yield* deliver('a')
			const first = yield* claimAll(yield* findWaiting)
			yield* settle(first, { retryAfterMs: 5_000 })
			yield* deliver('newer')
			expect(yield* findReady).toEqual([])
			expect(Option.isNone(yield* claimUpTo(Number.MAX_SAFE_INTEGER))).toEqual(true)
			yield* TestClock.adjust(5_000)
			expect(yield* findReady).toEqual([RecoverableMailbox.make({ mailboxKey })])
			const retry = Option.getOrThrow(yield* claimFrozen)
			expect(eventIds(retry)).toEqual(['a'])
			expect(retry.attempt).toEqual(2)
			yield* settle(retry, 'completed')
			expect(eventIds(yield* claimAll(yield* findWaiting))).toEqual(['newer'])
		}),
	)

	if (options.holdsManyMailboxes) {
		contract(
			'reports every due mailbox and claims them independently',
			Effect.gen(function* () {
				yield* deliver('a', 'thread-1')
				yield* deliver('b', 'thread-2')
				const ready = yield* findReady
				expect(ready.map(({ mailboxKey: key }) => key).toSorted()).toEqual(
					[deliveryMailboxKey(event('a', 'thread-1')), deliveryMailboxKey(event('b', 'thread-2'))].toSorted(),
				)
				yield* claimUpTo(Number.MAX_SAFE_INTEGER)
				expect((yield* findReady).map(({ mailboxKey: key }) => key)).toEqual([
					deliveryMailboxKey(event('b', 'thread-2')),
				])
			}),
		)
	}

	contract(
		'keeps the batch ID and token across recovery, with a new claim ID',
		Effect.gen(function* () {
			yield* deliver('a')
			const identity = nextBatchIdentity()
			const first = Option.getOrThrow(yield* claimUpTo((yield* findWaiting).waiting.lastSequence, identity))
			expect(first.batchId).toEqual(identity.batchId)
			expect(first.accessToken).toEqual(identity.accessToken)
			yield* TestClock.adjust(leaseMs)
			const recovered = Option.getOrThrow(yield* claimFrozen)
			expect(recovered.batchId).toEqual(identity.batchId)
			expect(recovered.accessToken).toEqual(identity.accessToken)
			expect(recovered.claimId === first.claimId).toEqual(false)
		}),
	)

	contract(
		'gives the next batch its own batch ID',
		Effect.gen(function* () {
			yield* deliver('a')
			const first = yield* claimAll(yield* findWaiting)
			yield* deliver('b')
			yield* settle(first, 'completed')
			const identity = nextBatchIdentity()
			const second = Option.getOrThrow(yield* claimUpTo((yield* findWaiting).waiting.lastSequence, identity))
			expect(second.batchId).toEqual(identity.batchId)
			expect(second.batchId === first.batchId).toEqual(false)
		}),
	)

	contract(
		'saves one preparation per batch and hands it to the next attempt',
		Effect.gen(function* () {
			const backend = yield* MailboxProcessingBackend
			yield* deliver('a')
			const first = yield* claimAll(yield* findWaiting)
			expect(first.prepared).toBeUndefined()
			const owner = { mailboxKey, claimId: first.claimId }
			expect(yield* backend.prepareDelivery({ ...owner, prepared: preparation('onNewMention') })).toEqual(
				preparation('onNewMention'),
			)
			expect(yield* backend.prepareDelivery({ ...owner, prepared: preparation('onNewMention') })).toEqual(
				preparation('onNewMention'),
			)
			const conflict = yield* backend
				.prepareDelivery({ ...owner, prepared: preparation('onSubscribedThreadEvents') })
				.pipe(Effect.flip)
			expect(conflict._tag).toEqual('DeliveryPreparationConflict')
			yield* settle(first, { retryAfterMs: 100 })
			yield* TestClock.adjust(100)
			const retry = Option.getOrThrow(yield* claimFrozen)
			expect(retry.prepared).toEqual(preparation('onNewMention'))
		}),
	)

	contract(
		'refuses a preparation from a claim that no longer owns the batch',
		Effect.gen(function* () {
			const backend = yield* MailboxProcessingBackend
			yield* deliver('a')
			const abandoned = yield* claimAll(yield* findWaiting)
			yield* TestClock.adjust(leaseMs)
			Option.getOrThrow(yield* claimFrozen)
			const lost = yield* backend
				.prepareDelivery({ mailboxKey, claimId: abandoned.claimId, prepared: preparation('onNewMention') })
				.pipe(Effect.flip)
			expect(lost._tag).toEqual('MailboxProcessingClaimLost')
		}),
	)

	contract(
		'goes quiet once everything is settled',
		Effect.gen(function* () {
			yield* deliver('a')
			yield* settle(yield* claimAll(yield* findWaiting), 'completed')
			yield* TestClock.adjust(60_000)
			expect(yield* findReady).toEqual([])
		}),
	)
}
