/**
 * This file applies one change to the stored mailbox inside a Durable Object storage transaction.
 *
 * Every write that changes the mailbox's deliveries goes through here, so the alarm always matches
 * `deliveries.readyAt`. A transaction closure may run outside the calling fiber, so changes need no
 * services: the caller reads the clock first and passes the time in.
 */
import type { DeliverySlot } from '@humanlayer/channels-delivery'
import { Effect, Option, Predicate, Schema } from 'effect'

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

/**
 * Apply one transition to the stored mailbox in one storage transaction. A transition that fails,
 * such as a refused lifecycle change, writes nothing, and the returned effect fails with its error.
 * A stored mailbox that cannot be decoded is narrowed where it is read, by `onUndecodable`, to the
 * calling service's own error. Storage failures are defects (see `MailboxStorage`).
 *
 * The storage transaction takes a closure that cannot fail: Alchemy runs it with `runPromise`, so a
 * failure would reach the caller as an untyped rejection. The closure therefore returns its outcome
 * as an `Exit`, which goes back into the error channel as soon as the transaction ends. Nothing
 * outside this function sees it as a value.
 */
export const transactMailbox = <A, E, U>(
	storage: (typeof MailboxStorage)['Service'],
	input: {
		readonly whenNothingStored: Effect.Effect<A, E>
		readonly transition: (current: DurableMailboxState) => Effect.Effect<MailboxTransition<A>, E>
		readonly onUndecodable: (error: Schema.SchemaError) => Effect.Effect<never, U>
	},
): Effect.Effect<A, E | U> =>
	storage
		.transaction((transaction) =>
			Effect.gen(function* () {
				const stored = yield* transaction.get(mailboxStateKey)
				if (Predicate.isUndefined(stored)) return yield* input.whenNothingStored
				const current = yield* decodeMailboxState(stored).pipe(
					Effect.catchTag('SchemaError', input.onUndecodable),
				)
				const { result, next } = yield* input.transition(current)
				if (Option.isSome(next)) yield* writeMailboxState(transaction, next.value)
				return result
			}).pipe(Effect.exit),
		)
		.pipe(Effect.flatten)

/** A lifecycle change to the mailbox's deliveries: the new slot and a value, or the reason it was refused. */
export type DeliveriesChange<A, E> = (
	current: DurableMailboxState,
) => Effect.Effect<{ readonly slot: DeliverySlot; readonly value: A }, E>

/**
 * Apply one lifecycle change to the mailbox's deliveries. A missing mailbox fails with `onMissing`,
 * and a refused change with its refusal; neither writes anything.
 */
export const changeDeliveries = <A, E, U>(
	storage: (typeof MailboxStorage)['Service'],
	input: {
		readonly onMissing: E
		readonly change: DeliveriesChange<A, E>
		readonly onUndecodable: (error: Schema.SchemaError) => Effect.Effect<never, U>
	},
) =>
	transactMailbox<A, E, U>(storage, {
		onUndecodable: input.onUndecodable,
		whenNothingStored: Effect.fail(input.onMissing),
		transition: (current) =>
			input.change(current).pipe(
				Effect.map(({ slot, value }) => ({
					result: value,
					next: Option.some(DurableMailboxState.make({ ...current, deliveries: slot })),
				})),
			),
	})
