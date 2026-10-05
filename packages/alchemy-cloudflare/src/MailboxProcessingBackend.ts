import {
	ClaimedMailboxBatch,
	DEFAULT_RETRY_AFTER_MS,
	DeliveryAdmissionBatch,
	DeliveryPreparationConflict,
	MailboxProcessingBackend,
	MailboxProcessingClaimLost,
	MailboxProcessingUnavailable,
	OutputReadyMailbox,
	RecoverableMailbox,
	WaitingMailbox,
	activeDeliveryWork,
	claimDeliveryOutput,
	claimFrozenBatch,
	handOffDeliverySlot,
	makeDeliveryId,
	prepareDeliverySlot,
	recordDeliveryAttempt,
	renewDeliveryClaim,
	renewDeliveryOutput,
	settleDeliveryOutput,
	startDeliveryBatch,
	toClaimedDeliveryOutput,
	toClaimedMailboxBatch,
	type ClaimDeliveryOutput,
	type ClaimedDeliveryOutput,
	type ClaimMailbox,
	type DeferMailbox,
	type HandOffMailboxDelivery,
	type PrepareMailboxDelivery,
	type ReadyMailbox,
	type RecordProcessingAttemptResult,
	type RenewDeliveryOutput,
	type RenewMailboxClaim,
	type SettleDeliveryOutput,
} from '@humanlayer/channels-delivery'
import { Array as Arr, Clock, Effect, Layer, Match, Option, Predicate, Random, type Schema } from 'effect'

import { DurableMailboxState, mailboxStateKey, type WaitingAdmission } from './MailboxState'
import { MailboxStorage } from './MailboxStorage'
import {
	changeDeliveries,
	decodeMailboxState,
	transactMailbox,
	unchanged,
	type DeliveriesChange,
	type MailboxTransition,
} from './MailboxTransaction'

const makeClaimId = Effect.gen(function* () {
	const now = yield* Clock.currentTimeMillis
	return `${now}-${Math.abs(yield* Random.nextInt)}`
})

/** Log a stored mailbox that cannot be decoded, where it is read, then narrow it to the one error callers can act on. */
const undecodable = (message: string) => (error: Schema.SchemaError) =>
	Effect.logError(message, error).pipe(
		Effect.andThen(Effect.fail(new MailboxProcessingUnavailable({ reason: 'cloudflare_unavailable' }))),
	)

/**
 * Storage failures arrive as defects (see `MailboxStorage`): log one, then narrow it to the one error
 * callers can act on. Typed errors, such as a lost claim, are not touched.
 */
const narrowStorageDefect =
	(message: string) =>
	<A, E, R>(effect: Effect.Effect<A, E, R>) =>
		effect.pipe(
			Effect.catchDefect((defect) =>
				Effect.logError(message, defect).pipe(
					Effect.andThen(Effect.fail(new MailboxProcessingUnavailable({ reason: 'cloudflare_unavailable' }))),
				),
			),
		)

const toBatch = (entries: ReadonlyArray<WaitingAdmission>) => {
	const [first, ...rest] = entries.map(({ admission }) => admission)
	return Predicate.isUndefined(first) ? null : DeliveryAdmissionBatch.make([first, ...rest])
}

const isDue = (current: DurableMailboxState, now: number) =>
	Predicate.isNotNull(current.deliveries.readyAt) && current.deliveries.readyAt <= now

const describeReadyMailbox = (current: DurableMailboxState, now: number): ReadonlyArray<ReadyMailbox> => {
	if (!isDue(current, now)) return []
	if (Predicate.isNotNull(current.deliveries.active)) {
		return activeDeliveryWork(current.deliveries.active) === 'Output'
			? [OutputReadyMailbox.make({ mailboxKey: current.mailboxKey })]
			: [RecoverableMailbox.make({ mailboxKey: current.mailboxKey })]
	}
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

const claimTransition = (input: {
	readonly current: DurableMailboxState
	readonly claim: ClaimMailbox
	readonly claimId: string
	readonly now: number
}): MailboxTransition<Option.Option<ClaimedMailboxBatch>> => {
	const { current, claim, claimId, now } = input
	if (current.mailboxKey !== claim.mailboxKey || !isDue(current, now)) return unchanged(Option.none())
	return Match.value(claim).pipe(
		Match.tagsExhaustive({
			ClaimWaitingEvents: ({ upToSequence, batchId, accessToken, leaseMs }) => {
				const admissions = toBatch(current.waiting.filter(({ sequence }) => sequence <= upToSequence))
				if (Predicate.isNull(admissions)) return unchanged(Option.none())
				const started = startDeliveryBatch(current.deliveries, {
					batchId,
					accessToken,
					admissions,
					claimId,
					leaseMs,
					now,
				})
				if (Predicate.isNull(started)) return unchanged(Option.none())
				return {
					result: Option.some(
						toClaimedMailboxBatch({ mailboxKey: current.mailboxKey, active: started.claimed, claimId }),
					),
					next: Option.some(
						DurableMailboxState.make({
							...current,
							waiting: current.waiting.filter(({ sequence }) => sequence > upToSequence),
							deliveries: started.slot,
						}),
					),
				}
			},
			ClaimFrozenBatch: ({ leaseMs }) => {
				const { slot, claimed } = claimFrozenBatch(current.deliveries, {
					claimId,
					leaseMs,
					now,
					hasWaiting: Arr.isReadonlyArrayNonEmpty(current.waiting),
				})
				return {
					result: Predicate.isNull(claimed)
						? Option.none()
						: Option.some(
								toClaimedMailboxBatch({ mailboxKey: current.mailboxKey, active: claimed, claimId }),
							),
					next: Option.some(DurableMailboxState.make({ ...current, deliveries: slot })),
				}
			},
		}),
	)
}

const claimOutputTransition = (input: {
	readonly current: DurableMailboxState
	readonly claim: ClaimDeliveryOutput
	readonly claimId: string
	readonly now: number
}): MailboxTransition<Option.Option<ClaimedDeliveryOutput>> => {
	const { current, claim, claimId, now } = input
	if (current.mailboxKey !== claim.mailboxKey) return unchanged(Option.none())
	const { slot, claimed } = claimDeliveryOutput(current.deliveries, {
		claimId,
		leaseMs: claim.leaseMs,
		now,
		idempotencyKey: claim.idempotencyKey,
	})
	if (Predicate.isNull(claimed)) return unchanged(Option.none())
	return {
		result: Option.some(toClaimedDeliveryOutput({ mailboxKey: current.mailboxKey, claimId, ...claimed })),
		next: Option.some(DurableMailboxState.make({ ...current, deliveries: slot })),
	}
}

const deferTransition = (current: DurableMailboxState, input: DeferMailbox): MailboxTransition<void> => {
	const seenLatest = current.waiting.at(-1)?.sequence === input.lastSequenceSeen
	if (current.mailboxKey !== input.mailboxKey || Predicate.isNotNull(current.deliveries.active) || !seenLatest) {
		return unchanged(undefined)
	}
	return {
		result: undefined,
		next: Option.some(
			DurableMailboxState.make({ ...current, deliveries: { ...current.deliveries, readyAt: input.until } }),
		),
	}
}

/**
 * Builds a mailbox-processing backend over the current Durable Object's persistent storage.
 *
 * One Durable Object holds one mailbox. Its alarm always matches the stored `deliveries.readyAt`,
 * so the host's alarm handler wakes processing exactly when the mailbox is next due, whether for a
 * callback or for output.
 * The lifecycle rules themselves are the shared `DeliveryLifecycle` transitions.
 */
export const makeMailboxProcessingBackendFromDurableObjectStorage = Effect.gen(function* () {
	const storage = yield* MailboxStorage

	const claimLost = (claim: { readonly mailboxKey: string; readonly claimId: string }) =>
		new MailboxProcessingClaimLost({ mailboxKey: claim.mailboxKey, claimId: claim.claimId })

	/**
	 * A change to a claimed batch. A mailbox that is not the claim's own is a lost claim. `message` is
	 * what a storage or decode failure is logged with.
	 */
	const changeClaimedDeliveries = <A, E>(
		message: string,
		claim: { readonly mailboxKey: string; readonly claimId: string },
		change: DeliveriesChange<A, E | MailboxProcessingClaimLost>,
	) =>
		changeDeliveries(storage, {
			onMissing: claimLost(claim),
			change: (current) =>
				current.mailboxKey === claim.mailboxKey ? change(current) : Effect.fail(claimLost(claim)),
			onUndecodable: undecodable(message),
		}).pipe(narrowStorageDefect(message))

	return MailboxProcessingBackend.of({
		findReadyMailboxes: Effect.gen(function* () {
			const now = yield* Clock.currentTimeMillis
			const stored = yield* storage.get(mailboxStateKey)
			if (Predicate.isUndefined(stored)) return []
			const current = yield* decodeMailboxState(stored).pipe(
				Effect.catchTag('SchemaError', undecodable('Cloudflare mailbox look failed')),
			)
			return describeReadyMailbox(current, now)
		}).pipe(
			narrowStorageDefect('Cloudflare mailbox look failed'),
			Effect.withSpan('delivery.cloudflare.find_ready_mailboxes'),
		),

		claimMailbox: Effect.fn('delivery.cloudflare.claim_mailbox')(function* (claim: ClaimMailbox) {
			const now = yield* Clock.currentTimeMillis
			const claimId = yield* makeClaimId
			return yield* transactMailbox(storage, {
				whenNothingStored: Effect.succeedNone,
				transition: (current) => Effect.succeed(claimTransition({ current, claim, claimId, now })),
				onUndecodable: undecodable('Cloudflare mailbox claim failed'),
			})
		}, narrowStorageDefect('Cloudflare mailbox claim failed')),

		deferMailbox: (input) =>
			transactMailbox(storage, {
				whenNothingStored: Effect.void,
				transition: (current) => Effect.succeed(deferTransition(current, input)),
				onUndecodable: undecodable('Cloudflare mailbox deferral failed'),
			}).pipe(
				narrowStorageDefect('Cloudflare mailbox deferral failed'),
				Effect.withSpan('delivery.cloudflare.defer_mailbox'),
			),

		renewClaim: Effect.fn('delivery.cloudflare.renew_claim')(function* (renewal: RenewMailboxClaim) {
			const now = yield* Clock.currentTimeMillis
			yield* changeClaimedDeliveries('Cloudflare mailbox claim renewal failed', renewal, (current) =>
				renewDeliveryClaim(current.deliveries, { ...renewal, now }).pipe(
					Effect.map((slot) => ({ slot, value: undefined })),
					Effect.catchTag('ClaimNotOwned', () => Effect.fail(claimLost(renewal))),
				),
			)
		}),

		recordProcessingAttemptResult: Effect.fn('delivery.cloudflare.record_processing_attempt_result')(function* (
			input: RecordProcessingAttemptResult,
		) {
			const retryAfterMs = Match.value(input.result).pipe(
				Match.tag('RetryableFailure', ({ retryAfterMs }) => retryAfterMs ?? DEFAULT_RETRY_AFTER_MS),
				Match.orElse(() => null),
			)
			yield* changeClaimedDeliveries('Cloudflare mailbox result recording failed', input.claim, (current) =>
				recordDeliveryAttempt(current.deliveries, {
					claimId: input.claim.claimId,
					retryAfterMs,
					now: input.finishedAt,
					hasWaiting: Arr.isReadonlyArrayNonEmpty(current.waiting),
				}).pipe(
					Effect.map((slot) => ({ slot, value: undefined })),
					Effect.catchTag('ClaimNotOwned', () => Effect.fail(claimLost(input.claim))),
				),
			)
		}),

		prepareDelivery: Effect.fn('delivery.cloudflare.prepare_delivery')(function* (input: PrepareMailboxDelivery) {
			return yield* changeClaimedDeliveries('Cloudflare mailbox delivery preparation failed', input, (current) =>
				prepareDeliverySlot(current.deliveries, input).pipe(
					Effect.map(({ slot, prepared }) => ({ slot, value: prepared })),
					Effect.catchTags({
						ClaimNotOwned: () => Effect.fail(claimLost(input)),
						PreparationMismatch: ({ batchId }) =>
							Effect.fail(
								new DeliveryPreparationConflict({
									deliveryId: makeDeliveryId({ mailboxKey: input.mailboxKey, batchId }),
								}),
							),
					}),
				),
			)
		}),

		handOffDelivery: Effect.fn('delivery.cloudflare.hand_off_delivery')(function* (input: HandOffMailboxDelivery) {
			yield* changeClaimedDeliveries('Cloudflare mailbox handoff failed', input, (current) =>
				handOffDeliverySlot(current.deliveries, input).pipe(
					Effect.map((slot) => ({ slot, value: undefined })),
					Effect.catchTag('ClaimNotOwned', () => Effect.fail(claimLost(input))),
				),
			)
		}),

		claimDeliveryOutput: Effect.fn('delivery.cloudflare.claim_delivery_output')(function* (
			claim: ClaimDeliveryOutput,
		) {
			const now = yield* Clock.currentTimeMillis
			const claimId = yield* makeClaimId
			return yield* transactMailbox(storage, {
				whenNothingStored: Effect.succeedNone,
				transition: (current) => Effect.succeed(claimOutputTransition({ current, claim, claimId, now })),
				onUndecodable: undecodable('Cloudflare delivery output claim failed'),
			})
		}, narrowStorageDefect('Cloudflare delivery output claim failed')),

		renewDeliveryOutput: Effect.fn('delivery.cloudflare.renew_delivery_output')(function* (
			renewal: RenewDeliveryOutput,
		) {
			const now = yield* Clock.currentTimeMillis
			yield* changeClaimedDeliveries('Cloudflare delivery output renewal failed', renewal, (current) =>
				renewDeliveryOutput(current.deliveries, { ...renewal, now }).pipe(
					Effect.map((slot) => ({ slot, value: undefined })),
					Effect.catchTag('ClaimNotOwned', () => Effect.fail(claimLost(renewal))),
				),
			)
		}),

		settleDeliveryOutput: Effect.fn('delivery.cloudflare.settle_delivery_output')(function* (
			input: SettleDeliveryOutput,
		) {
			yield* changeClaimedDeliveries('Cloudflare delivery output settlement failed', input, (current) =>
				settleDeliveryOutput(current.deliveries, {
					...input,
					now: input.settledAt,
					hasWaiting: Arr.isReadonlyArrayNonEmpty(current.waiting),
				}).pipe(
					Effect.map((slot) => ({ slot, value: undefined })),
					Effect.catchTag('ClaimNotOwned', () => Effect.fail(claimLost(input))),
				),
			)
		}),
	})
})

export const MailboxProcessingBackendFromDurableObjectStorage = Layer.effect(
	MailboxProcessingBackend,
	makeMailboxProcessingBackendFromDurableObjectStorage,
)
