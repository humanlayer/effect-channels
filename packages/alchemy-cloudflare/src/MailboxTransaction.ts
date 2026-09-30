/**
 * This file applies one change to the stored mailbox inside a Durable Object storage transaction.
 *
 * Every write that changes the mailbox's deliveries goes through here, so the alarm always matches
 * `deliveries.readyAt`. A transaction closure may run outside the calling fiber, so changes are pure:
 * the caller reads the clock first and passes the time in.
 */
import type { DeliverySlot } from '@humanlayer/channels-delivery-next'
import { Effect, Exit, Option, Predicate, Result, Schema } from 'effect'

import { DurableMailboxState, mailboxStateKey } from './MailboxState'
import type { MailboxStorage, MailboxStorageTransaction } from './MailboxStorage'

/**
 * What one operation does to the stored mailbox.
 *
 * @property next - the state to store, or none to leave storage and the alarm untouched
 */
export type MailboxTransition<A> = {
	readonly result: A
	readonly next: Option.Option<DurableMailboxState>
}

export const unchanged = <A>(result: A): MailboxTransition<A> => ({ result, next: Option.none() })

export const decodeMailboxState = Schema.decodeUnknownEffect(DurableMailboxState)

/** Store the mailbox and move the alarm to its `deliveries.readyAt`, or delete it when nothing is due. */
export const writeMailboxState = (transaction: MailboxStorageTransaction, state: DurableMailboxState) =>
	Effect.gen(function* () {
		yield* transaction.put(mailboxStateKey, state)
		const { readyAt } = state.deliveries
		if (Predicate.isNull(readyAt)) yield* transaction.deleteAlarm
		else yield* transaction.setAlarm(readyAt)
	})

/** Apply one transition to the stored mailbox in one storage transaction. Fails only when storage or decoding does. */
export const transactMailbox = <A>(
	storage: (typeof MailboxStorage)['Service'],
	input: {
		readonly whenNothingStored: A
		readonly transition: (current: DurableMailboxState) => MailboxTransition<A>
	},
) =>
	storage
		.transaction((transaction) =>
			Effect.gen(function* () {
				const stored = yield* transaction.get(mailboxStateKey)
				if (Predicate.isUndefined(stored)) return Exit.succeed(input.whenNothingStored)
				const decoded = yield* decodeMailboxState(stored).pipe(Effect.exit)
				if (Exit.isFailure(decoded)) return Exit.failCause(decoded.cause)
				const { result, next } = input.transition(decoded.value)
				if (Option.isSome(next)) yield* writeMailboxState(transaction, next.value)
				return Exit.succeed(result)
			}),
		)
		.pipe(Effect.flatten)

/** A lifecycle change to the mailbox's deliveries: the new slot and a value, or the reason it was refused. */
export type DeliveriesChange<A, E> = (
	current: DurableMailboxState,
) => Result.Result<{ readonly slot: DeliverySlot; readonly value: A }, E>

/**
 * Apply one lifecycle change to the mailbox's deliveries. A missing mailbox or a refused change writes
 * nothing and comes back as a failed `Result`; the effect itself fails only when storage does.
 */
export const changeDeliveries = <A, E>(
	storage: (typeof MailboxStorage)['Service'],
	input: {
		readonly onMissing: E
		readonly change: DeliveriesChange<A, E>
	},
) =>
	transactMailbox<Result.Result<A, E>>(storage, {
		whenNothingStored: Result.fail(input.onMissing),
		transition: (current) => {
			const changed = input.change(current)
			if (Result.isFailure(changed)) return unchanged(Result.fail(changed.failure))
			return {
				result: Result.succeed(changed.success.value),
				next: Option.some(DurableMailboxState.make({ ...current, deliveries: changed.success.slot })),
			}
		},
	})
