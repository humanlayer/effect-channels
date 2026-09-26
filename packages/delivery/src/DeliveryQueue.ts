import { Array as Arr, Clock, Context, Effect, Layer, Predicate, Schema } from 'effect'

import { DeliveryPolicy } from './DeliveryPolicy'
import {
	activeBatches,
	type CurrentMailboxState,
	currentMailbox,
	emptyMailbox,
	Envelope,
	eventIdentity,
	mailboxCapacityUsage,
	mailboxEnvelopes,
	Outcome,
	retainedOutcomes,
} from './Mailbox'
import { MailboxStore } from './MailboxStore'

export const DeliveryAdmission = Schema.Struct({
	key: Schema.NonEmptyString,
	envelope: Envelope,
	policy: DeliveryPolicy,
})
export interface DeliveryAdmission extends Schema.Schema.Type<typeof DeliveryAdmission> {}

export const DeliveryReceipt = Schema.Struct({
	key: Schema.NonEmptyString,
	accepted: Schema.Boolean,
})
export interface DeliveryReceipt extends Schema.Schema.Type<typeof DeliveryReceipt> {}

export class DeliveryQueueError extends Schema.TaggedError<DeliveryQueueError>()('DeliveryQueueError', {
	reason: Schema.Literals(['capacity', 'conflict', 'unavailable']),
}) {}

const scheduled = (state: CurrentMailboxState, policy: DeliveryPolicy): CurrentMailboxState => {
	const concurrency = policy.mode === 'concurrent' ? policy.maxConcurrency : 1
	const batches = activeBatches(state)
	const deadlines = batches.flatMap((batch) => {
		if (Predicate.isTagged('External')(batch.stage))
			return batch.stage.cleanupLeaseUntil === null ? [] : [batch.stage.cleanupLeaseUntil]
		return [batch.stage?.leaseUntil ?? batch.leaseUntil]
	})
	if (Arr.isReadonlyArrayNonEmpty(state.pending) && batches.length < concurrency && state.pendingReadyAt !== null)
		deadlines.push(state.pendingReadyAt)
	for (const operation of state.operations ?? []) {
		if (Predicate.isTagged('Pending')(operation.state)) deadlines.push(operation.state.readyAt)
		if (Predicate.isTagged('Delivering')(operation.state)) deadlines.push(operation.state.leaseUntil)
	}
	return {
		...state,
		readyAt: Arr.isReadonlyArrayEmpty(deadlines) ? null : Math.min(...deadlines),
		burstDraining:
			state.burstDraining && (Arr.isReadonlyArrayNonEmpty(batches) || Arr.isReadonlyArrayNonEmpty(state.pending)),
	}
}

const known = (state: CurrentMailboxState, identity: string) =>
	state.outcomes.some((outcome) => outcome.identity === identity) ||
	mailboxEnvelopes(state).some((event) => eventIdentity(event) === identity)

/** Shared durable admission transition used by every DeliveryQueue implementation backed by MailboxStore. */
export const enqueueDelivery = Effect.fn('delivery.queue.enqueue')(function* (input: DeliveryAdmission) {
	const store = yield* MailboxStore
	const policy = input.policy
	for (let attempt = 0; attempt <= policy.conflictRetries; attempt++) {
		const snapshot = yield* store
			.loadMailbox({ key: input.key })
			.pipe(Effect.mapError(() => DeliveryQueueError.make({ reason: 'unavailable' })))
		const now = yield* Clock.currentTimeMillis
		const current = currentMailbox(snapshot?.state ?? emptyMailbox())
		const state = {
			...current,
			outcomes: retainedOutcomes(current, now),
			maxOutcomes: policy.maxOutcomes,
		}
		const duplicate = known(state, eventIdentity(input.envelope))
		let next: CurrentMailboxState
		if (duplicate) {
			next = state
		} else if (
			policy.mode === 'drop' &&
			(Arr.isReadonlyArrayNonEmpty(state.pending) || Arr.isReadonlyArrayNonEmpty(activeBatches(state)))
		) {
			if (mailboxCapacityUsage(state) >= policy.maxOutcomes)
				return yield* DeliveryQueueError.make({ reason: 'capacity' })
			next = {
				...state,
				outcomes: [
					...state.outcomes,
					Outcome.make({
						identity: eventIdentity(input.envelope),
						kind: 'dropped',
						expiresAt: now + policy.retentionMs,
					}),
				],
			}
		} else {
			if (
				mailboxEnvelopes(state).length >= policy.maxEnvelopes ||
				mailboxCapacityUsage(state) >= policy.maxOutcomes
			)
				return yield* DeliveryQueueError.make({ reason: 'capacity' })
			const pendingReadyAt =
				policy.mode === 'debounce'
					? now + policy.quietPeriodMs
					: (state.pendingReadyAt ??
						(policy.mode === 'burst' && !state.burstDraining ? now + policy.windowMs : now))
			next = { ...state, pending: [...state.pending, input.envelope], pendingReadyAt }
		}

		const committed = yield* store
			.commitMailbox({
				key: input.key,
				expectedRevision: snapshot?.revision ?? null,
				nextState: scheduled(next, policy),
			})
			.pipe(Effect.mapError(() => DeliveryQueueError.make({ reason: 'unavailable' })))
		if (committed === 'committed') return DeliveryReceipt.make({ key: input.key, accepted: !duplicate })
	}
	return yield* DeliveryQueueError.make({ reason: 'conflict' })
})

export class DeliveryQueue extends Context.Service<
	DeliveryQueue,
	{
		readonly enqueue: (input: DeliveryAdmission) => Effect.Effect<DeliveryReceipt, DeliveryQueueError>
	}
>()('delivery/DeliveryQueue') {
	/** Implements durable admission with optimistic commits against the supplied MailboxStore. */
	static readonly layerMailboxStore = Layer.effect(
		DeliveryQueue,
		Effect.gen(function* () {
			const store = yield* MailboxStore
			const clock = yield* Clock.Clock
			return DeliveryQueue.of({
				enqueue: (input) =>
					enqueueDelivery(input).pipe(
						Effect.provideService(MailboxStore, store),
						Effect.provideService(Clock.Clock, clock),
					),
			})
		}),
	)
}
