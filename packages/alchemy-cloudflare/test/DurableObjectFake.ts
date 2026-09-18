/**
 * Test-only in-memory stand-in for one Durable Object: its key-value storage, its transactions and its alarm.
 *
 * Like the real alchemy bridge, a transaction closure runs in its own runtime, outside the calling fiber.
 * Code that reads the clock inside a transaction therefore misses the test clock here too, and its test fails.
 */
import * as Cloudflare from 'alchemy/Cloudflare'
import { RuntimeContext } from 'alchemy/RuntimeContext'
import { Context, Effect, Layer, Predicate, Ref, type Schema } from 'effect'

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

const toMillis = (scheduledTime: number | Date) =>
	Predicate.isDate(scheduledTime) ? scheduledTime.getTime() : scheduledTime

/** The part of Durable Object storage, and of a storage transaction, that the mailbox store calls. */
type StorageOperations = {
	readonly get: (key: string) => Effect.Effect<Schema.Json | undefined, never, RuntimeContext>
	readonly put: (
		keyOrEntries: string | Readonly<Record<string, Schema.Json>>,
		value?: Schema.Json,
	) => Effect.Effect<void, never, RuntimeContext>
	readonly getAlarm: () => Effect.Effect<number | null, never, RuntimeContext>
	readonly setAlarm: (scheduledTime: number | Date) => Effect.Effect<void, never, RuntimeContext>
	readonly deleteAlarm: () => Effect.Effect<void, never, RuntimeContext>
}

const makeStorageOperations = (stored: Ref.Ref<Stored>): StorageOperations => ({
	get: (key) => Ref.get(stored).pipe(Effect.map(({ entries }) => entries.get(key))),
	put: (keyOrEntries, value = null) =>
		Ref.update(stored, (current) => ({
			...current,
			entries: new Map([
				...current.entries,
				...(Predicate.isString(keyOrEntries) ? [[keyOrEntries, value] as const] : Object.entries(keyOrEntries)),
			]),
		})),
	getAlarm: () => Ref.get(stored).pipe(Effect.map(({ alarm }) => alarm)),
	setAlarm: (scheduledTime: number | Date) =>
		Ref.update(stored, (current) => ({ ...current, alarm: toMillis(scheduledTime) })),
	deleteAlarm: () => Ref.update(stored, (current) => ({ ...current, alarm: null })),
})

/** One empty Durable Object for each build of this layer. */
export const DurableObjectFake = Layer.effectContext(
	Effect.gen(function* () {
		const committed = yield* Ref.make<Stored>({ entries: new Map(), alarm: null })

		const storage: StorageOperations & Pick<Cloudflare.DurableObjectStorage, 'transaction'> = {
			...makeStorageOperations(committed),
			transaction: <T>(
				closure: (transaction: Cloudflare.DurableObjectTransaction) => Effect.Effect<T, never, RuntimeContext>,
			) =>
				Effect.gen(function* () {
					const working = yield* Ref.make(yield* Ref.get(committed))
					// SAFETY: the fake implements only the transaction methods the store calls; any other call throws.
					const transaction = makeStorageOperations(working) as Cloudflare.DurableObjectTransaction
					const result = yield* Effect.promise(() =>
						Effect.runPromise(closure(transaction).pipe(Effect.provide(RuntimeContext.phantom))),
					)
					yield* Ref.set(committed, yield* Ref.get(working))
					return result
				}),
		}

		const durableObject: Pick<Cloudflare.DurableObjectState['Service'], 'storage'> = {
			// SAFETY: the fake implements only the storage methods the store calls; any other call throws.
			storage: storage as Cloudflare.DurableObjectStorage,
		}

		return Context.make(
			Cloudflare.DurableObjectState,
			// SAFETY: the store reads nothing but `storage` from the Durable Object state.
			durableObject as Cloudflare.DurableObjectState['Service'],
		).pipe(
			Context.add(
				RuntimeContext,
				RuntimeContext.of({
					Type: 'test',
					id: 'durable-object-fake',
					env: {},
					get: () => Effect.succeed(undefined),
					set: (id) => Effect.succeed(id),
				}),
			),
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
