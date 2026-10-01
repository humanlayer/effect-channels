/**
 * This file defines the store half of delivery control over Postgres.
 *
 * The token check and the change run in one transaction, through the shared `DeliveryLifecycle`
 * rules, on the mailbox row locked for them: the delivery cannot change between the check and the
 * write. A result, a link, a message, or an activity saves its output operation in the same
 * transaction and moves the mailbox's `ready_at`, so the next poll sends it. No provider is called here.
 *
 * The delivery ID names the mailbox, so a request goes straight to its row with no lookup table.
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
import * as SqlClient from 'effect/unstable/sql/SqlClient'
import * as SqlError from 'effect/unstable/sql/SqlError'

import { changeDeliverySlot, lockDeliverySlot } from './DeliverySlot'

/** Log the raw failure, then narrow it to `DeliveryControlUnavailable`. */
const unavailable = <A, R>(effect: Effect.Effect<A, Schema.SchemaError | SqlError.SqlError, R>) =>
	effect.pipe(
		Effect.tapError((error) => Effect.logError('SQL delivery control failed', error)),
		Effect.catchTags({
			SchemaError: () => Effect.fail(new DeliveryControlUnavailable({ reason: 'sql_codec_unavailable' })),
			SqlError: () => Effect.fail(new DeliveryControlUnavailable({ reason: 'sql_unavailable' })),
		}),
	)

/** Read what a remote worker may see. Waits for a change in progress, but lets other reads run. */
const readDeliveryStatus = Effect.fn('delivery.sql.read_delivery_status')(function* (input: ReadDeliveryStatus) {
	const now = yield* Clock.currentTimeMillis
	const sql = yield* SqlClient.SqlClient
	const loaded = yield* sql
		.withTransaction(
			lockDeliverySlot({ mailboxKey: input.reference.mailboxKey, now, lock: 'Share', withRetained: true }),
		)
		.pipe(unavailable)
	if (Option.isNone(loaded)) return yield* new DeliveryNotFound()
	return yield* Effect.fromResult(readDeliverySlotStatus(loaded.value.slot, { ...input, now }))
})

/** Check the token and apply the change, with the output it needs, in one transaction. */
const applyDeliveryMutation = Effect.fn('delivery.sql.apply_delivery_mutation')(function* (
	input: ApplyDeliveryMutation,
) {
	const now = yield* Clock.currentTimeMillis
	const recorded = yield* changeDeliverySlot({
		mailboxKey: input.reference.mailboxKey,
		withRetained: true,
		onMissing: new DeliveryNotFound(),
		change: (loaded) =>
			Result.map(
				applyDeliverySlotMutation(loaded.slot, { ...input, now, hasWaiting: loaded.hasWaiting }),
				({ slot, receipt }) => ({ slot, value: receipt }),
			),
	}).pipe(unavailable)
	return yield* Effect.fromResult(recorded)
})

/**
 * Delivery control over the application's `SqlClient`. It creates no tables: the mailbox store's
 * other layers run the migrations.
 */
export const DeliveryControlBackendSql = Layer.effect(
	DeliveryControlBackend,
	Effect.gen(function* () {
		const sql = yield* SqlClient.SqlClient
		return DeliveryControlBackend.of({
			readDeliveryStatus: (input) =>
				readDeliveryStatus(input).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
			applyDeliveryMutation: (input) =>
				applyDeliveryMutation(input).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
		})
	}),
)
