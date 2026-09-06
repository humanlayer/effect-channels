import { layer as deliveryMemory } from '@humanlayer/channels-delivery/memory'
import { Effect, Layer, Schema } from 'effect'

import { SlackConnection, SlackConnectionLookupInput } from './Schema.ts'
import {
	connectionFromConfig,
	SlackConnectionStore,
	SlackConnectionStoreError,
	UpsertSlackConnection,
} from './SlackConnectionStore.ts'
import { SlackSubscriptions } from './SlackSubscriptions.ts'

export interface MemoryConnectionsOptions {
	readonly connections?: ReadonlyArray<UpsertSlackConnection>
	readonly capacity?: number
}

/** Volatile authoritative connections, not a TTL cache. Deletion is explicit and frees capacity. */
export const connections = (options: MemoryConnectionsOptions = {}) =>
	Layer.effect(
		SlackConnectionStore,
		Effect.gen(function* () {
			const capacity = yield* Schema.Int.check(Schema.isGreaterThan(0)).makeEffect(options.capacity ?? 10_000)
			const initial = yield* Schema.Array(UpsertSlackConnection).makeEffect(options.connections ?? [])
			const entries = new Map<string, SlackConnection>()
			if (new Set(initial.map((entry) => entry.workspaceId)).size > capacity)
				return yield* SlackConnectionStoreError.make({ operation: 'initialize' })
			for (const entry of initial) entries.set(entry.workspaceId, entry.connection)
			return SlackConnectionStore.of({
				get: Effect.fn('slack.memory.connections.get')((input) =>
					Effect.sync(() => entries.get(input.workspaceId)),
				),
				upsert: Effect.fn('slack.memory.connections.upsert')(function* (input) {
					const parsed = yield* UpsertSlackConnection.makeEffect(input).pipe(
						Effect.mapError(() => SlackConnectionStoreError.make({ operation: 'upsert' })),
					)
					yield* Effect.suspend(() => {
						if (!entries.has(parsed.workspaceId) && entries.size >= capacity)
							return Effect.fail(SlackConnectionStoreError.make({ operation: 'upsert' }))
						entries.set(parsed.workspaceId, parsed.connection)
						return Effect.void
					})
				}),
				remove: Effect.fn('slack.memory.connections.remove')(function* (input) {
					yield* SlackConnectionLookupInput.makeEffect(input).pipe(
						Effect.mapError(() => SlackConnectionStoreError.make({ operation: 'remove' })),
					)
					yield* Effect.sync(() => entries.delete(input.workspaceId))
				}),
			})
		}),
	)

export const subscriptions = SlackSubscriptions.layerMemory
export const layer = (options: MemoryConnectionsOptions & { readonly maxMailboxes?: number } = {}) =>
	Layer.mergeAll(
		connections(options),
		subscriptions(),
		deliveryMemory({ maxMailboxes: options.maxMailboxes ?? 10_000 }),
	)

export const connectionsFromConfig = Layer.unwrap(
	connectionFromConfig().pipe(Effect.map((connection) => connections({ connections: [connection] }))),
)

export const layerFromConfig = Layer.mergeAll(
	connectionsFromConfig,
	subscriptions(),
	deliveryMemory({ maxMailboxes: 10_000 }),
)
