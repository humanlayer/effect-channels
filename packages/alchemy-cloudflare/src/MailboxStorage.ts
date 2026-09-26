/**
 * This file defines `MailboxStorage`: the part of a Durable Object's storage that the mailbox uses.
 *
 * The mailbox code depends on this narrow service instead of the whole Durable Object state, so a
 * test can stand in for storage without faking the rest of the Durable Object.
 */
import * as Cloudflare from 'alchemy/Cloudflare'
import { RuntimeContext } from 'alchemy/RuntimeContext'
import { Context, Effect, Layer, type Schema } from 'effect'

/** The storage operations a mailbox calls inside a storage transaction. */
export interface MailboxStorageTransaction {
	readonly get: (key: string) => Effect.Effect<unknown>
	readonly put: (key: string, value: Schema.Json) => Effect.Effect<void>
	readonly setAlarm: (scheduledTime: number) => Effect.Effect<void>
	readonly deleteAlarm: Effect.Effect<void>
}

export class MailboxStorage extends Context.Service<
	MailboxStorage,
	MailboxStorageTransaction & {
		readonly delete: (key: string) => Effect.Effect<void>
		readonly getAlarm: Effect.Effect<number | null>
		/** Run the closure in one storage transaction; like Cloudflare, it may run outside the calling fiber. */
		readonly transaction: <A>(
			closure: (transaction: MailboxStorageTransaction) => Effect.Effect<A>,
		) => Effect.Effect<A>
	}
>()('@humanlayer/channels-alchemy-cloudflare/MailboxStorage') {}

/** Mailbox storage over the current Durable Object's persistent storage. */
export const MailboxStorageFromDurableObjectState = Layer.effect(
	MailboxStorage,
	Effect.gen(function* () {
		const { storage } = yield* Cloudflare.DurableObjectState
		const runtimeContext = yield* RuntimeContext
		const run = <A>(effect: Effect.Effect<A, never, RuntimeContext>) =>
			effect.pipe(Effect.provideService(RuntimeContext, runtimeContext))
		const fromTransaction = (transaction: Cloudflare.DurableObjectTransaction): MailboxStorageTransaction => ({
			get: (key) => run(transaction.get(key)),
			put: (key, value) => run(transaction.put(key, value)),
			setAlarm: (scheduledTime) => run(transaction.setAlarm(scheduledTime)),
			deleteAlarm: run(transaction.deleteAlarm()),
		})
		return MailboxStorage.of({
			get: (key) => run(storage.get(key)),
			put: (key, value) => run(storage.put(key, value)),
			delete: (key) => run(storage.delete(key)).pipe(Effect.asVoid),
			getAlarm: run(storage.getAlarm()),
			setAlarm: (scheduledTime) => run(storage.setAlarm(scheduledTime)),
			deleteAlarm: run(storage.deleteAlarm()),
			transaction: (closure) => run(storage.transaction((transaction) => closure(fromTransaction(transaction)))),
		})
	}),
)
