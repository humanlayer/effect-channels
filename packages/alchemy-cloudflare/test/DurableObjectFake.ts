/**
 * Test-only in-memory stand-in for one Durable Object: its key-value storage, its transactions and its alarm.
 *
 * Like the real alchemy bridge, a transaction closure runs in its own runtime, outside the calling fiber.
 * Code that reads the clock inside a transaction therefore misses the test clock here too, and its test fails.
 */
import { Context, Effect, Layer, Ref, type Schema } from 'effect'

import { MailboxStorage, type MailboxStorageTransaction } from '../src/MailboxStorage'

type Stored = {
	readonly entries: ReadonlyMap<string, Schema.Json>
	readonly alarm: number | null
}

/** Lets a test read the alarm the store has set on the fake Durable Object. */
export class DurableObjectFakeAlarm extends Context.Service<
	DurableObjectFakeAlarm,
	{
		/** The time the alarm is set for, or null when no alarm is set. */
		readonly scheduledAt: Effect.Effect<number | null>
		/** What Cloudflare does right before it calls the alarm handler: the alarm is cleared. */
		readonly clearAsCloudflareDoesBeforeTheHandler: Effect.Effect<void>
	}
>()('@humanlayer/channels-alchemy-cloudflare/test/DurableObjectFakeAlarm') {}

const makeTransaction = (stored: Ref.Ref<Stored>): MailboxStorageTransaction => ({
	get: (key) => Ref.get(stored).pipe(Effect.map(({ entries }) => entries.get(key))),
	put: (key, value) =>
		Ref.update(stored, (current) => ({ ...current, entries: new Map(current.entries).set(key, value) })),
	setAlarm: (scheduledTime) => Ref.update(stored, (current) => ({ ...current, alarm: scheduledTime })),
	deleteAlarm: Ref.update(stored, (current) => ({ ...current, alarm: null })),
})

/** One empty Durable Object for each build of this layer. */
export const DurableObjectFake = Layer.effectContext(
	Effect.gen(function* () {
		const committed = yield* Ref.make<Stored>({ entries: new Map(), alarm: null })

		const storage = MailboxStorage.of({
			...makeTransaction(committed),
			delete: (key) =>
				Ref.update(committed, (current) => {
					const entries = new Map(current.entries)
					entries.delete(key)
					return { ...current, entries }
				}),
			getAlarm: Ref.get(committed).pipe(Effect.map(({ alarm }) => alarm)),
			transaction: (closure) =>
				Effect.gen(function* () {
					const working = yield* Ref.make(yield* Ref.get(committed))
					const result = yield* Effect.promise(() => Effect.runPromise(closure(makeTransaction(working))))
					yield* Ref.set(committed, yield* Ref.get(working))
					return result
				}),
		})

		return Context.make(MailboxStorage, storage).pipe(
			Context.add(
				DurableObjectFakeAlarm,
				DurableObjectFakeAlarm.of({
					scheduledAt: Ref.get(committed).pipe(Effect.map(({ alarm }) => alarm)),
					clearAsCloudflareDoesBeforeTheHandler: Ref.update(committed, (current) => ({
						...current,
						alarm: null,
					})),
				}),
			),
		)
	}),
)
