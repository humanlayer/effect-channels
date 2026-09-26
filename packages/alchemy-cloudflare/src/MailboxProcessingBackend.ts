import {
	ClaimedMailboxBatch,
	DeliveryAdmissionBatch,
	MailboxProcessingBackend,
	MailboxProcessingClaimLost,
	MailboxProcessingUnavailable,
	RecoverableMailbox,
	Timestamp,
	WaitingMailbox,
	type ClaimMailbox,
	type DeferMailbox,
	type ReadyMailbox,
	type RecordProcessingAttemptResult,
	type RenewMailboxClaim,
} from '@humanlayer/channels-delivery-next'
import { Clock, Effect, Exit, Layer, Match, Option, Predicate, Random, Schema } from 'effect'

import { DurableMailboxState, mailboxStateKey, type WaitingAdmission } from './MailboxState'
import { MailboxStorage } from './MailboxStorage'

const makeClaimId = Effect.gen(function* () {
	const now = yield* Clock.currentTimeMillis
	return `${now}-${Math.abs(yield* Random.nextInt)}`
})

/** Log the raw failure, then narrow it to the one error callers can act on. */
const narrowToUnavailable =
	(message: string) =>
	<A, E, R>(effect: Effect.Effect<A, E, R>) =>
		effect.pipe(
			Effect.tapError((error) => Effect.logError(message, error)),
			Effect.mapError(() => new MailboxProcessingUnavailable({ reason: 'cloudflare_unavailable' })),
			Effect.catchDefect((defect) =>
				Effect.logError(message, defect).pipe(
					Effect.andThen(Effect.fail(new MailboxProcessingUnavailable({ reason: 'cloudflare_unavailable' }))),
				),
			),
		)

/**
 * What one operation does to the stored mailbox.
 *
 * @property next - the state to store, or none to leave storage and the alarm untouched
 */
type MailboxTransition<A> = {
	readonly result: A
	readonly next: Option.Option<DurableMailboxState>
}

const unchanged = <A>(result: A): MailboxTransition<A> => ({ result, next: Option.none() })

const toBatch = (entries: ReadonlyArray<WaitingAdmission>) => {
	const [first, ...rest] = entries.map(({ admission }) => admission)
	return Predicate.isUndefined(first) ? null : DeliveryAdmissionBatch.make([first, ...rest])
}

const isDue = (current: DurableMailboxState, now: number) =>
	Predicate.isNotNull(current.readyAt) && current.readyAt <= now

const describeReadyMailbox = (current: DurableMailboxState, now: number): ReadonlyArray<ReadyMailbox> => {
	if (!isDue(current, now)) return []
	if (current.status !== 'idle') return [RecoverableMailbox.make({ mailboxKey: current.mailboxKey })]
	const first = current.waiting[0]
	const last = current.waiting.at(-1)
	if (Predicate.isUndefined(first) || Predicate.isUndefined(last)) return []
	return [
		WaitingMailbox.make({
			mailboxKey: current.mailboxKey,
			provider: current.provider,
			waiting: {
				count: current.waiting.length,
				firstSequence: first.sequence,
				firstArrivedAt: first.arrivedAt,
				lastSequence: last.sequence,
				lastArrivedAt: last.arrivedAt,
			},
		}),
	]
}

/** Pick the batch a claim takes, or null when the mailbox is not in the state the claim needs. */
const selectBatch = (current: DurableMailboxState, claim: ClaimMailbox) =>
	Match.value(claim).pipe(
		Match.tagsExhaustive({
			ClaimWaitingEvents: ({ upToSequence }) => {
				if (current.status !== 'idle') return null
				const admissions = toBatch(current.waiting.filter(({ sequence }) => sequence <= upToSequence))
				return Predicate.isNull(admissions)
					? null
					: {
							admissions,
							attempt: 1,
							waiting: current.waiting.filter(({ sequence }) => sequence > upToSequence),
						}
			},
			ClaimFrozenBatch: () =>
				current.status === 'idle' || Predicate.isNull(current.activeBatch)
					? null
					: { admissions: current.activeBatch, attempt: current.attempt + 1, waiting: current.waiting },
		}),
	)

const claimTransition = (input: {
	readonly current: DurableMailboxState
	readonly claim: ClaimMailbox
	readonly claimId: string
	readonly now: number
}): MailboxTransition<Option.Option<ClaimedMailboxBatch>> => {
	const { current, claim, claimId, now } = input
	if (current.mailboxKey !== claim.mailboxKey || !isDue(current, now)) return unchanged(Option.none())
	const selection = selectBatch(current, claim)
	if (Predicate.isNull(selection)) return unchanged(Option.none())
	return {
		result: Option.some(
			ClaimedMailboxBatch.make({
				mailboxKey: current.mailboxKey,
				claimId,
				attempt: selection.attempt,
				admissions: selection.admissions,
			}),
		),
		next: Option.some(
			DurableMailboxState.make({
				...current,
				status: 'active',
				waiting: selection.waiting,
				activeBatch: selection.admissions,
				claimId,
				attempt: selection.attempt,
				readyAt: Timestamp.make(now + claim.leaseMs),
			}),
		),
	}
}

const deferTransition = (current: DurableMailboxState, input: DeferMailbox): MailboxTransition<void> => {
	const seenLatest = current.waiting.at(-1)?.sequence === input.lastSequenceSeen
	if (current.mailboxKey !== input.mailboxKey || current.status !== 'idle' || !seenLatest) return unchanged(undefined)
	return { result: undefined, next: Option.some(DurableMailboxState.make({ ...current, readyAt: input.until })) }
}

const ownsClaim = (current: DurableMailboxState, claim: { readonly mailboxKey: string; readonly claimId: string }) =>
	current.mailboxKey === claim.mailboxKey && current.status === 'active' && current.claimId === claim.claimId

/** The result is whether the claim was still ours. */
const renewTransition = (input: {
	readonly current: DurableMailboxState
	readonly renewal: RenewMailboxClaim
	readonly now: number
}): MailboxTransition<boolean> =>
	ownsClaim(input.current, input.renewal)
		? {
				result: true,
				next: Option.some(
					DurableMailboxState.make({
						...input.current,
						readyAt: Timestamp.make(input.now + input.renewal.leaseMs),
					}),
				),
			}
		: unchanged(false)

/** The result is whether the claim was still ours. */
const recordTransition = (
	current: DurableMailboxState,
	input: RecordProcessingAttemptResult,
): MailboxTransition<boolean> => {
	if (!ownsClaim(current, input.claim)) return unchanged(false)
	const settled = { ...current, lastResult: input.result, claimId: null }
	const next = Match.value(input.result).pipe(
		Match.tag('RetryableFailure', ({ retryAfterMs }) =>
			DurableMailboxState.make({
				...settled,
				status: 'retry',
				readyAt: Timestamp.make(input.finishedAt + (retryAfterMs ?? 1_000)),
			}),
		),
		Match.orElse(() =>
			DurableMailboxState.make({
				...settled,
				status: 'idle',
				activeBatch: null,
				attempt: 0,
				readyAt: current.waiting.length > 0 ? input.finishedAt : null,
			}),
		),
	)
	return { result: true, next: Option.some(next) }
}

/**
 * Builds a mailbox-processing backend over the current Durable Object's persistent storage.
 *
 * One Durable Object holds one mailbox. Its alarm always matches the stored `readyAt`,
 * so the host's alarm handler wakes processing exactly when the mailbox is next due.
 */
export const makeMailboxProcessingBackendFromDurableObjectStorage = Effect.gen(function* () {
	const storage = yield* MailboxStorage

	const decodeStored = Schema.decodeUnknownEffect(DurableMailboxState)

	/**
	 * Apply one transition to the stored mailbox inside a storage transaction, and move the alarm with `readyAt`.
	 * The transition is pure: the transaction runs outside this fiber, so it must not read the clock.
	 */
	const transact = <A>(input: {
		readonly whenNothingStored: A
		readonly transition: (current: DurableMailboxState) => MailboxTransition<A>
	}) =>
		storage
			.transaction((transaction) =>
				Effect.gen(function* () {
					const stored = yield* transaction.get(mailboxStateKey)
					if (Predicate.isUndefined(stored)) return Exit.succeed(input.whenNothingStored)
					const decoded = yield* decodeStored(stored).pipe(Effect.exit)
					if (Exit.isFailure(decoded)) return Exit.failCause(decoded.cause)
					const { result, next } = input.transition(decoded.value)
					if (Option.isSome(next)) {
						yield* transaction.put(mailboxStateKey, next.value)
						if (Predicate.isNull(next.value.readyAt)) yield* transaction.deleteAlarm
						else yield* transaction.setAlarm(next.value.readyAt)
					}
					return Exit.succeed(result)
				}),
			)
			.pipe(Effect.flatten)

	const claimLostUnless = (owned: boolean, claim: { readonly mailboxKey: string; readonly claimId: string }) =>
		owned
			? Effect.void
			: Effect.fail(new MailboxProcessingClaimLost({ mailboxKey: claim.mailboxKey, claimId: claim.claimId }))

	return MailboxProcessingBackend.of({
		findReadyMailboxes: Effect.gen(function* () {
			const now = yield* Clock.currentTimeMillis
			const stored = yield* storage.get(mailboxStateKey)
			if (Predicate.isUndefined(stored)) return []
			return describeReadyMailbox(yield* decodeStored(stored), now)
		}).pipe(
			narrowToUnavailable('Cloudflare mailbox look failed'),
			Effect.withSpan('delivery.cloudflare.find_ready_mailboxes'),
		),

		claimMailbox: Effect.fn('delivery.cloudflare.claim_mailbox')(function* (claim: ClaimMailbox) {
			const now = yield* Clock.currentTimeMillis
			const claimId = yield* makeClaimId
			return yield* transact({
				whenNothingStored: Option.none(),
				transition: (current) => claimTransition({ current, claim, claimId, now }),
			})
		}, narrowToUnavailable('Cloudflare mailbox claim failed')),

		deferMailbox: (input) =>
			transact({
				whenNothingStored: undefined,
				transition: (current) => deferTransition(current, input),
			}).pipe(
				narrowToUnavailable('Cloudflare mailbox deferral failed'),
				Effect.withSpan('delivery.cloudflare.defer_mailbox'),
			),

		renewClaim: Effect.fn('delivery.cloudflare.renew_claim')(function* (renewal: RenewMailboxClaim) {
			const now = yield* Clock.currentTimeMillis
			const owned = yield* transact({
				whenNothingStored: false,
				transition: (current) => renewTransition({ current, renewal, now }),
			}).pipe(narrowToUnavailable('Cloudflare mailbox claim renewal failed'))
			yield* claimLostUnless(owned, renewal)
		}),

		recordProcessingAttemptResult: Effect.fn('delivery.cloudflare.record_processing_attempt_result')(function* (
			input: RecordProcessingAttemptResult,
		) {
			const owned = yield* transact({
				whenNothingStored: false,
				transition: (current) => recordTransition(current, input),
			}).pipe(narrowToUnavailable('Cloudflare mailbox result recording failed'))
			yield* claimLostUnless(owned, input.claim)
		}),
	})
})

export const MailboxProcessingBackendFromDurableObjectStorage = Layer.effect(
	MailboxProcessingBackend,
	makeMailboxProcessingBackendFromDurableObjectStorage,
)
