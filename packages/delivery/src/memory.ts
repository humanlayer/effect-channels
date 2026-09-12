import { Context, Effect, Layer, Schema } from 'effect'

import { deliveryIds, MailboxSnapshot } from './Mailbox.js'
import { DeliveryLocatorStore, MailboxReadiness, MailboxStore, MailboxStoreError, ScanReady } from './MailboxStore.js'

export const MemoryOptions = Schema.Struct({ maxMailboxes: Schema.Int.check(Schema.isGreaterThan(0)) })
export type MemoryOptions = typeof MemoryOptions.Type

export const layer = (options: MemoryOptions) =>
	Layer.effectContext(
		Effect.gen(function* () {
			yield* MemoryOptions.makeEffect(options)
			const entries = new Map<
				string,
				{ readonly revision: number; readonly json: string; readonly readyAt: number | null }
			>()
			const deliveryLocations = new Map<string, string>()
			const codec = Schema.fromJsonString(MailboxSnapshot)
			const store = MailboxStore.of({
				loadMailbox: Effect.fn('delivery.memory.load')(function* (input) {
					const entry = entries.get(input.key)
					if (entry === undefined) return undefined
					return yield* Schema.decodeEffect(codec)(entry.json).pipe(
						Effect.mapError(() => MailboxStoreError.make({ operation: 'load' })),
					)
				}),
				commitMailbox: Effect.fn('delivery.memory.commit')(function* (input) {
					const revision = (input.expectedRevision ?? -1) + 1
					const json = yield* Schema.encodeEffect(codec)({ revision, state: input.nextState }).pipe(
						Effect.mapError(() => MailboxStoreError.make({ operation: 'commit' })),
					)
					return yield* Effect.suspend(() => {
						const current = entries.get(input.key)
						if ((current?.revision ?? null) !== input.expectedRevision)
							return Effect.succeed('conflict' as const)
						if (current === undefined && entries.size >= options.maxMailboxes) {
							return Effect.fail(MailboxStoreError.make({ operation: 'commit' }))
						}
						for (const deliveryId of deliveryIds(input.nextState)) {
							const existing = deliveryLocations.get(deliveryId)
							if (existing !== undefined && existing !== input.key)
								return Effect.fail(MailboxStoreError.make({ operation: 'commit' }))
						}
						entries.set(input.key, { revision, json, readyAt: input.nextState.readyAt })
						for (const deliveryId of deliveryIds(input.nextState))
							deliveryLocations.set(deliveryId, input.key)
						return Effect.succeed('committed' as const)
					})
				}),
			})
			const readiness = MailboxReadiness.of({
				scanReady: Effect.fn('delivery.memory.scan')(function* (input) {
					yield* ScanReady.makeEffect(input).pipe(
						Effect.mapError(() => MailboxStoreError.make({ operation: 'scan' })),
					)
					return [...entries]
						.filter(
							([key, entry]) =>
								key.startsWith(input.prefix) && entry.readyAt !== null && entry.readyAt <= input.now,
						)
						.sort(
							(left, right) =>
								(left[1].readyAt ?? 0) - (right[1].readyAt ?? 0) || left[0].localeCompare(right[0]),
						)
						.slice(0, input.limit)
						.map(([key]) => key)
				}),
			})
			const locator = DeliveryLocatorStore.of({
				locateDelivery: Effect.fn('delivery.memory.locate')((input) =>
					Effect.succeed(deliveryLocations.get(input.deliveryId)),
				),
			})
			return Context.make(MailboxStore, store).pipe(
				Context.add(MailboxReadiness, readiness),
				Context.add(DeliveryLocatorStore, locator),
			)
		}),
	)
