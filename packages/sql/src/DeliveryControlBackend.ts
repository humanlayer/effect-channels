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
} from '@humanlayer/channels-delivery'
import { Clock, Effect, Layer, Option, type Schema } from 'effect'
import * as SqlClient from 'effect/sql/SqlClient'
import * as SqlError from 'effect/sql/SqlError'

import { changeDeliverySlot, lockDeliverySlot, type NarrowSqlFailure } from './DeliverySlot'

/** Log a database or codec failure where it happens, then narrow it to `DeliveryControlUnavailable`. */
const narrowSqlFailure: NarrowSqlFailure<DeliveryControlUnavailable> = (reason) => (error) =>
	Effect.logError('SQL delivery control failed', error).pipe(
		Effect.andThen(Effect.fail(new DeliveryControlUnavailable({ reason }))),
	)

const unavailable = <A, R>(effect: Effect.Effect<A, SqlError.SqlError | Schema.SchemaError, R>) =>
	effect.pipe(
		Effect.catchTags({
			SqlError: narrowSqlFailure('sql_unavailable'),
			SchemaError: narrowSqlFailure('sql_codec_unavailable'),
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
	return yield* readDeliverySlotStatus(loaded.value.slot, { ...input, now })
})

/** Check the token and apply the change, with the output it needs, in one transaction. */
const applyDeliveryMutation = Effect.fn('delivery.sql.apply_delivery_mutation')(function* (
	input: ApplyDeliveryMutation,
) {
	const now = yield* Clock.currentTimeMillis
	return yield* changeDeliverySlot({
		mailboxKey: input.reference.mailboxKey,
		withRetained: true,
		onMissing: new DeliveryNotFound(),
		change: (loaded) =>
			applyDeliverySlotMutation(loaded.slot, { ...input, now, hasWaiting: loaded.hasWaiting }).pipe(
				Effect.map(({ slot, receipt }) => ({ slot, value: receipt })),
			),
		narrowSqlFailure,
	})
})

/** Delivery control over the application's `SqlClient`. It creates no tables: see `MigrationsSql`. */
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
