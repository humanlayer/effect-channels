/**
 * This file defines the store half of delivery control over Redis.
 *
 * The token check and the change are decided together, through the shared `DeliveryLifecycle` rules,
 * on one read of the mailbox, and written only if the mailbox has not changed since: the delivery
 * cannot change between the check and the write. A result, a link, a message, or an activity saves
 * its output operation in the same write and moves the mailbox's due time, so the next poll sends it.
 * No provider is called here.
 *
 * The delivery ID names the mailbox, so a request goes straight to its keys with no lookup.
 */
import {
	DeliveryControlBackend,
	DeliveryControlUnavailable,
	DeliveryNotFound,
	applyDeliverySlotMutation,
	readDeliverySlotStatus,
	type ApplyDeliveryMutation,
	type ReadDeliveryStatus,
} from '@humanlayer/channels-delivery-next'
import { Clock, Effect, Layer, Option, Result, Schema } from 'effect'
import * as Redis from 'effect/unstable/persistence/Redis'

import { changeDeliverySlot, loadDeliverySlot, type SlotChanged } from './DeliverySlot'

/** Log the raw failure, then narrow it to `DeliveryControlUnavailable`. */
const unavailable = <A, R>(effect: Effect.Effect<A, Redis.RedisError | Schema.SchemaError | SlotChanged, R>) =>
	effect.pipe(
		Effect.tapError((error) => Effect.logError('Redis delivery control failed', error)),
		Effect.catchTags({
			RedisError: () => Effect.fail(new DeliveryControlUnavailable({ reason: 'redis_unavailable' })),
			SchemaError: () => Effect.fail(new DeliveryControlUnavailable({ reason: 'redis_codec_unavailable' })),
			SlotChanged: () => Effect.fail(new DeliveryControlUnavailable({ reason: 'redis_contention' })),
		}),
	)

/** Read what a remote worker may see. The read is one script, so it sees one moment of the mailbox. */
const readDeliveryStatus = Effect.fn('delivery.redis.read_delivery_status')(function* (input: ReadDeliveryStatus) {
	const now = yield* Clock.currentTimeMillis
	const loaded = yield* loadDeliverySlot({ mailboxKey: input.reference.mailboxKey }).pipe(unavailable)
	if (Option.isNone(loaded)) return yield* new DeliveryNotFound()
	return yield* Effect.fromResult(readDeliverySlotStatus(loaded.value.slot, { ...input, now }))
})

/** Check the token and apply the change, with the output it needs, in one write. */
const applyDeliveryMutation = Effect.fn('delivery.redis.apply_delivery_mutation')(function* (
	input: ApplyDeliveryMutation,
) {
	const now = yield* Clock.currentTimeMillis
	const recorded = yield* changeDeliverySlot({
		mailboxKey: input.reference.mailboxKey,
		onMissing: new DeliveryNotFound(),
		change: (loaded) =>
			Result.map(
				applyDeliverySlotMutation(loaded.slot, { ...input, now, hasWaiting: loaded.hasWaiting }),
				({ slot, receipt }) => ({ slot, value: receipt }),
			),
	}).pipe(unavailable)
	return yield* Effect.fromResult(recorded)
})

/** Delivery control over the application's `Redis` client. */
export const DeliveryControlBackendRedis = Layer.effect(
	DeliveryControlBackend,
	Effect.gen(function* () {
		const redis = yield* Redis.Redis
		return DeliveryControlBackend.of({
			readDeliveryStatus: (input) => readDeliveryStatus(input).pipe(Effect.provideService(Redis.Redis, redis)),
			applyDeliveryMutation: (input) =>
				applyDeliveryMutation(input).pipe(Effect.provideService(Redis.Redis, redis)),
		})
	}),
)
