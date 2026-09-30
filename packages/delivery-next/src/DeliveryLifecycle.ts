/**
 * This file defines the life of one delivery as pure transitions over a mailbox's delivery slot.
 *
 * A store that keeps a whole mailbox as one value, such as memory or a Durable Object, runs these
 * inside its own atomic update. Keeping them here means those stores cannot drift apart. Stores with
 * their own data layout, such as SQL, follow the same rules through the shared backend contract.
 *
 * ```text
 * Local ──handoff──▶ ExternalCleaning ──callback returns──▶ ExternalWaiting ──terminal──▶ Retired
 *   │                     │ terminal: retire once the callback returns
 *   │ retryable failure
 *   ▼
 * Retry ──due──▶ Local (new claim, same batch)
 * ```
 */
import { Data, Match, Predicate, Result, Schema } from 'effect'

import {
	DeliveryOperationKind,
	DeliveryTerminal,
	ExternalLink,
	PreparedDeliveryInvocation,
	type DeliveryStage,
} from './DeliveryContext'
import {
	DeliveryClosed,
	DeliveryMutationReceipt,
	DeliveryNotFound,
	DeliveryStatus,
	DeliveryTerminalConflict,
	sameDeliveryTerminal,
} from './DeliveryControl'
import {
	BatchId,
	DeliveryAccessToken,
	deliveryAccessTokenMatches,
	type DeliveryReference,
} from './DeliveryReference'
import { Timestamp } from './MailboxPolicy'
import { DeliveryAdmissionBatch } from './ProviderEventProcessing'

/** How long a finished delivery stays readable, so a remote worker can retry its last request. */
export const DELIVERY_RETENTION_MS = 7 * 24 * 60 * 60 * 1_000

/** The most finished deliveries one mailbox keeps. The oldest go first. */
export const MAX_RETAINED_DELIVERIES = 20

/** The default wait before a retryable failure runs again. */
export const DEFAULT_RETRY_AFTER_MS = 1_000

export const ActiveDeliveryStage = Schema.Literals(['Local', 'Retry', 'ExternalCleaning', 'ExternalWaiting'])
export type ActiveDeliveryStage = typeof ActiveDeliveryStage.Type

/**
 * The batch a mailbox is working on.
 *
 * @property claimId - the attempt that owns the batch; kept through `ExternalCleaning` as ownership of callback cleanup
 * @property terminal - the remote worker's result, when it has sent one
 */
export const ActiveDelivery = Schema.Struct({
	batchId: BatchId,
	accessToken: DeliveryAccessToken,
	admissions: DeliveryAdmissionBatch,
	attempt: Schema.Int.check(Schema.isGreaterThan(0)),
	claimId: Schema.NullOr(Schema.NonEmptyString),
	stage: ActiveDeliveryStage,
	prepared: Schema.optionalKey(PreparedDeliveryInvocation),
	terminal: Schema.optionalKey(DeliveryTerminal),
	interruptRequestedAt: Schema.optionalKey(Timestamp),
	handedOffAt: Schema.optionalKey(Timestamp),
	links: Schema.Array(ExternalLink),
})
export type ActiveDelivery = typeof ActiveDelivery.Type

/** A finished delivery, kept for status reads and repeated terminal requests. */
export const RetainedDelivery = Schema.Struct({
	batchId: BatchId,
	accessToken: DeliveryAccessToken,
	supportedOperations: Schema.Array(DeliveryOperationKind),
	terminal: Schema.optionalKey(DeliveryTerminal),
	interruptRequested: Schema.Boolean,
	retainUntil: Timestamp,
})
export type RetainedDelivery = typeof RetainedDelivery.Type

/**
 * One mailbox's deliveries.
 *
 * @property readyAt - when the mailbox next needs processing: a lease deadline, a retry, or waiting events
 */
export const DeliverySlot = Schema.Struct({
	active: Schema.NullOr(ActiveDelivery),
	readyAt: Schema.NullOr(Timestamp),
	retained: Schema.Array(RetainedDelivery),
})
export type DeliverySlot = typeof DeliverySlot.Type

export const emptyDeliverySlot = DeliverySlot.make({ active: null, readyAt: null, retained: [] })

/** What a transition needs to know about the rest of the mailbox. */
export type MailboxFacts = {
	readonly now: number
	readonly hasWaiting: boolean
}

/** A slot after a claim, and the delivery the claim now owns, if any. */
export type DeliverySlotClaim = { readonly slot: DeliverySlot; readonly claimed: ActiveDelivery | null }

/** The claim does not own the batch, or the batch is not in a stage the operation allows. */
export class ClaimNotOwned extends Data.TaggedError('ClaimNotOwned')<{}> {}
const claimNotOwned = new ClaimNotOwned()

/** A different preparation is already saved. */
export class PreparationMismatch extends Data.TaggedError('PreparationMismatch')<{ readonly batchId: BatchId }> {}

const retire = (slot: DeliverySlot, active: ActiveDelivery, facts: MailboxFacts): DeliverySlot => {
	const entry = {
		batchId: active.batchId,
		accessToken: active.accessToken,
		supportedOperations: active.prepared?.supportedOperations ?? [],
		interruptRequested: active.interruptRequestedAt !== undefined,
		retainUntil: Timestamp.make(facts.now + DELIVERY_RETENTION_MS),
	}
	const retiredEntry = RetainedDelivery.make(
		Predicate.isUndefined(active.terminal) ? entry : { ...entry, terminal: active.terminal },
	)
	const retained = [...slot.retained.filter(({ retainUntil }) => retainUntil > facts.now), retiredEntry].slice(
		-MAX_RETAINED_DELIVERIES,
	)
	return DeliverySlot.make({
		active: null,
		readyAt: facts.hasWaiting ? Timestamp.make(facts.now) : null,
		retained,
	})
}

const withActive = (slot: DeliverySlot, active: ActiveDelivery, readyAt: number | null): DeliverySlot =>
	DeliverySlot.make({ ...slot, active, readyAt: readyAt === null ? null : Timestamp.make(readyAt) })

/** The owned active delivery, when `claimId` owns it in a stage where callback code may be running. */
const owned = (slot: DeliverySlot, claimId: string) =>
	slot.active !== null &&
	slot.active.claimId === claimId &&
	(slot.active.stage === 'Local' || slot.active.stage === 'ExternalCleaning')
		? slot.active
		: null

/**
 * Start a new batch in an idle slot. The store has already chosen its admissions.
 * Returns null when the slot already has an active delivery.
 */
export const startDeliveryBatch = (
	slot: DeliverySlot,
	input: {
		readonly batchId: BatchId
		readonly accessToken: DeliveryAccessToken
		readonly admissions: DeliveryAdmissionBatch
		readonly claimId: string
		readonly leaseMs: number
		readonly now: number
	},
): { readonly slot: DeliverySlot; readonly claimed: ActiveDelivery } | null => {
	if (slot.active !== null) return null
	const claimed = ActiveDelivery.make({
		batchId: input.batchId,
		accessToken: input.accessToken,
		admissions: input.admissions,
		attempt: 1,
		claimId: input.claimId,
		stage: 'Local',
		links: [],
	})
	return { slot: withActive(slot, claimed, input.now + input.leaseMs), claimed }
}

/**
 * Take the frozen batch again after a retry comes due or a lease runs out.
 *
 * A lease that ran out after a handoff does not run the callback again: the handoff stands, so the
 * delivery waits for its remote worker, or retires if the remote worker already finished.
 */
export const claimFrozenBatch = (
	slot: DeliverySlot,
	input: { readonly claimId: string; readonly leaseMs: number } & MailboxFacts,
): DeliverySlotClaim => {
	const active = slot.active
	if (active === null || slot.readyAt === null || slot.readyAt > input.now) return { slot, claimed: null }
	if (active.terminal !== undefined) return { slot: retire(slot, active, input), claimed: null }
	const runAgain = (): DeliverySlotClaim => {
		const claimed = ActiveDelivery.make({
			...active,
			stage: 'Local',
			claimId: input.claimId,
			attempt: active.attempt + 1,
		})
		return { slot: withActive(slot, claimed, input.now + input.leaseMs), claimed }
	}
	return Match.value(active.stage).pipe(
		Match.when('ExternalWaiting', () => ({ slot, claimed: null })),
		Match.when('ExternalCleaning', () => ({
			slot: withActive(slot, ActiveDelivery.make({ ...active, stage: 'ExternalWaiting', claimId: null }), null),
			claimed: null,
		})),
		Match.when('Local', runAgain),
		Match.when('Retry', runAgain),
		Match.exhaustive,
	)
}

/** Move a live claim's lease forward. */
export const renewDeliveryClaim = (
	slot: DeliverySlot,
	input: { readonly claimId: string; readonly leaseMs: number; readonly now: number },
): Result.Result<DeliverySlot, ClaimNotOwned> => {
	const active = owned(slot, input.claimId)
	return active === null ? Result.fail(claimNotOwned) : Result.succeed(withActive(slot, active, input.now + input.leaseMs))
}

/** Save the callback choice and destination, once per batch. */
export const prepareDeliverySlot = (
	slot: DeliverySlot,
	input: { readonly claimId: string; readonly prepared: PreparedDeliveryInvocation },
): Result.Result<
	{ readonly slot: DeliverySlot; readonly prepared: PreparedDeliveryInvocation },
	ClaimNotOwned | PreparationMismatch
> => {
	const active = owned(slot, input.claimId)
	if (active === null) return Result.fail(claimNotOwned)
	if (active.prepared !== undefined) {
		return Schema.toEquivalence(PreparedDeliveryInvocation)(active.prepared, input.prepared)
			? Result.succeed({ slot, prepared: active.prepared })
			: Result.fail(new PreparationMismatch({ batchId: active.batchId }))
	}
	const next = ActiveDelivery.make({ ...active, prepared: input.prepared })
	return Result.succeed({ slot: withActive(slot, next, slot.readyAt), prepared: input.prepared })
}

/** Hand the batch off. Repeating it is harmless. The claim stays as ownership of callback cleanup. */
export const handOffDeliverySlot = (
	slot: DeliverySlot,
	input: { readonly claimId: string; readonly handedOffAt: number; readonly links: ReadonlyArray<ExternalLink> },
): Result.Result<DeliverySlot, ClaimNotOwned> => {
	const active = owned(slot, input.claimId)
	if (active === null) return Result.fail(claimNotOwned)
	if (active.stage === 'ExternalCleaning') return Result.succeed(slot)
	const links = [...active.links, ...input.links.filter((link) => !active.links.some(({ url }) => url === link.url))]
	const next = ActiveDelivery.make({
		...active,
		stage: 'ExternalCleaning',
		handedOffAt: Timestamp.make(input.handedOffAt),
		links,
	})
	return Result.succeed(withActive(slot, next, slot.readyAt))
}

/**
 * Record how an attempt ended. The stage decides, not only the result: a handed-off delivery waits
 * for its remote worker whatever the callback returned, and one with a remote result retires.
 */
export const recordDeliveryAttempt = (
	slot: DeliverySlot,
	input: {
		readonly claimId: string
		readonly retryAfterMs: number | null
	} & MailboxFacts,
): Result.Result<DeliverySlot, ClaimNotOwned> => {
	const active = owned(slot, input.claimId)
	if (active === null) return Result.fail(claimNotOwned)
	if (active.terminal !== undefined) return Result.succeed(retire(slot, active, input))
	if (active.stage === 'ExternalCleaning') {
		return Result.succeed(withActive(slot, ActiveDelivery.make({ ...active, stage: 'ExternalWaiting', claimId: null }), null))
	}
	if (input.retryAfterMs !== null) {
		const next = ActiveDelivery.make({ ...active, stage: 'Retry', claimId: null })
		return Result.succeed(withActive(slot, next, input.now + input.retryAfterMs))
	}
	return Result.succeed(retire(slot, active, input))
}

/** An event asked the current delivery to stop. Marks it once; the event itself still waits its turn. */
export const requestDeliveryInterrupt = (slot: DeliverySlot, now: number): DeliverySlot =>
	slot.active === null || slot.active.interruptRequestedAt !== undefined
		? slot
		: DeliverySlot.make({
				...slot,
				active: ActiveDelivery.make({ ...slot.active, interruptRequestedAt: Timestamp.make(now) }),
			})

/** The stage a remote worker sees. A delivery with a result whose callback is still returning is finishing. */
const publicStage = (active: ActiveDelivery): DeliveryStage =>
	active.terminal !== undefined ? 'Finishing' : active.stage

type Located = Data.TaggedEnum<{
	Active: { readonly active: ActiveDelivery }
	Retained: { readonly retained: RetainedDelivery }
}>
const Located = Data.taggedEnum<Located>()

/** Find the delivery and check the token. A missing delivery and a wrong token look the same. */
const locate = (
	slot: DeliverySlot,
	input: { readonly reference: DeliveryReference; readonly accessToken: string; readonly now: number },
): Located | null => {
	const matches = (saved: string) => deliveryAccessTokenMatches({ saved, presented: input.accessToken })
	if (slot.active !== null && slot.active.batchId === input.reference.batchId) {
		return matches(slot.active.accessToken) ? Located.Active({ active: slot.active }) : null
	}
	const retained = slot.retained.find(
		({ batchId, retainUntil }) => batchId === input.reference.batchId && retainUntil > input.now,
	)
	return retained !== undefined && matches(retained.accessToken) ? Located.Retained({ retained }) : null
}

const statusWithOutcome = (
	status: Omit<DeliveryStatus, '_tag' | 'outcome'>,
	terminal: DeliveryTerminal | undefined,
) =>
	DeliveryStatus.make(Predicate.isUndefined(terminal) ? status : { ...status, outcome: terminal.outcome })

/** What a remote worker may read about its delivery. */
export const readDeliverySlotStatus = (
	slot: DeliverySlot,
	input: { readonly reference: DeliveryReference; readonly accessToken: string; readonly now: number },
): Result.Result<DeliveryStatus, DeliveryNotFound> => {
	const located = locate(slot, input)
	if (located === null) return Result.fail(new DeliveryNotFound())
	return Result.succeed(
		Located.$match(located, {
			Active: ({ active }) =>
				statusWithOutcome(
					{
						deliveryId: input.reference.deliveryId,
						stage: publicStage(active),
						interruptRequested: active.interruptRequestedAt !== undefined,
						supportedOperations: active.prepared?.supportedOperations ?? [],
					},
					active.terminal,
				),
			Retained: ({ retained }) =>
				statusWithOutcome(
					{
						deliveryId: input.reference.deliveryId,
						stage: 'Retired',
						interruptRequested: retained.interruptRequested,
						supportedOperations: retained.supportedOperations,
					},
					retained.terminal,
				),
		}),
	)
}

/**
 * Record a remote worker's final result.
 *
 * - The same result again is `already_recorded`; a different one is a conflict.
 * - A delivery waiting for its remote worker, or waiting to retry, retires at once.
 * - A delivery whose callback is still running keeps the result and retires when the callback returns.
 * - A delivery that ended without a remote result is closed.
 */
export const recordDeliverySlotTerminal = (
	slot: DeliverySlot,
	input: {
		readonly reference: DeliveryReference
		readonly accessToken: string
		readonly terminal: DeliveryTerminal
	} & MailboxFacts,
): Result.Result<
	{ readonly slot: DeliverySlot; readonly receipt: DeliveryMutationReceipt },
	DeliveryNotFound | DeliveryTerminalConflict | DeliveryClosed
> => {
	const located = locate(slot, input)
	if (located === null) return Result.fail(new DeliveryNotFound())
	const receipt = (status: DeliveryMutationReceipt['status']) =>
		DeliveryMutationReceipt.make({ deliveryId: input.reference.deliveryId, status })
	const replay = (saved: DeliveryTerminal | undefined) => {
		if (saved === undefined) return Result.fail(new DeliveryClosed())
		return sameDeliveryTerminal(saved, input.terminal)
			? Result.succeed({ slot, receipt: receipt('already_recorded') })
			: Result.fail(new DeliveryTerminalConflict())
	}
	return Located.$match(located, {
		Retained: ({ retained }) => replay(retained.terminal),
		Active: ({ active }) => {
			if (active.terminal !== undefined) return replay(active.terminal)
			const next = ActiveDelivery.make({ ...active, terminal: input.terminal })
			/** No callback code is running in these stages, so nothing is left to wait for. */
			const nextSlot =
				active.stage === 'ExternalWaiting' || active.stage === 'Retry'
					? retire(slot, next, input)
					: withActive(slot, next, slot.readyAt)
			return Result.succeed({ slot: nextSlot, receipt: receipt('accepted') })
		},
	})
}
