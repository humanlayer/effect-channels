import {
	deliveryMailboxKey,
	DeliveryAdmissionJson,
	type DeliveryAdmission,
	DeliveryReceipt,
	MailboxDelivery,
	MailboxDeliveryUnavailable,
	requestDeliveryInterrupt,
} from '@humanlayer/channels-delivery-next'
import { Clock, Effect, Layer, Option, Schema } from 'effect'
import * as SqlClient from 'effect/unstable/sql/SqlClient'
import * as SqlError from 'effect/unstable/sql/SqlError'

import { loadDeliverySlot, writeDeliverySlot } from './DeliverySlot'

const insertedRows = Schema.Array(Schema.Struct({ event_id: Schema.NonEmptyString })).check(Schema.isMaxLength(1))

const unavailable = <A, R>(effect: Effect.Effect<A, Schema.SchemaError | SqlError.SqlError, R>) =>
	effect.pipe(
		Effect.tapError((error) => Effect.logError('SQL mailbox admission failed', error)),
		Effect.catchTags({
			SchemaError: () => Effect.fail(new MailboxDeliveryUnavailable({ reason: 'sql_codec_unavailable' })),
			SqlError: () => Effect.fail(new MailboxDeliveryUnavailable({ reason: 'sql_unavailable' })),
		}),
	)

/** Mark the active delivery, if there is one, as asked to stop. Runs under the mailbox lock. */
const markInterrupt = (input: { readonly mailboxKey: string; readonly now: number }) =>
	Effect.gen(function* () {
		const loaded = yield* loadDeliverySlot({ mailboxKey: input.mailboxKey, now: input.now, withRetained: false })
		if (Option.isNone(loaded)) return
		const slot = requestDeliveryInterrupt(loaded.value.slot, input.now)
		if (slot !== loaded.value.slot) yield* writeDeliverySlot({ loaded: loaded.value, slot, now: input.now })
	})

/**
 * Accept an event once. The mailbox row is locked before the insert draws its sequence number,
 * so sequence order is commit order within a mailbox.
 *
 * An arrival wakes an idle mailbox now, even a deferred one. A running or retrying mailbox keeps its due time.
 * An interrupting arrival also marks the mailbox's active delivery, in the same transaction, through
 * the shared lifecycle; the event itself still waits its turn.
 */
const deliver = Effect.fn('delivery.sql.deliver')(function* (admission: DeliveryAdmission) {
	const now = yield* Clock.currentTimeMillis
	const mailboxKey = deliveryMailboxKey(admission)
	const admissionJson = yield* Schema.encodeEffect(DeliveryAdmissionJson)(admission)
	const sql = (yield* SqlClient.SqlClient).withoutTransforms()
	return yield* sql.withTransaction(
		Effect.gen(function* () {
			yield* sql`INSERT INTO delivery_next_mailboxes (mailbox_key, provider)
			VALUES (${mailboxKey}, ${admission.provider}) ON CONFLICT (mailbox_key) DO NOTHING`
			yield* sql`SELECT mailbox_key FROM delivery_next_mailboxes
			WHERE mailbox_key = ${mailboxKey} FOR UPDATE`
			const rows = yield* sql`INSERT INTO delivery_next_admissions (
				mailbox_key, namespace, provider, event_id, admission_json, arrived_at
			) VALUES (
				${mailboxKey}, ${admission.namespace}, ${admission.provider}, ${admission.eventId},
				${admissionJson}, ${now}
			) ON CONFLICT (namespace, provider, event_id) DO NOTHING
			RETURNING event_id`
			const inserted = yield* Schema.decodeUnknownEffect(insertedRows)(rows)
			if (inserted.length === 1) {
				yield* sql`UPDATE delivery_next_mailboxes SET ready_at = ${now}
				WHERE mailbox_key = ${mailboxKey} AND status = 'idle'`
			}
			if (inserted.length === 1 && admission.interrupt === true) yield* markInterrupt({ mailboxKey, now })
			return DeliveryReceipt.make({ mailboxKey, accepted: inserted.length === 1 })
		}),
	)
}, unavailable)

/** Admission over the application's `SqlClient`. It creates no tables: see `MigrationsSql`. */
export const MailboxDeliverySql = Layer.effect(
	MailboxDelivery,
	Effect.gen(function* () {
		const sqlClient = yield* SqlClient.SqlClient
		return MailboxDelivery.of({
			deliver: (admission) => deliver(admission).pipe(Effect.provideService(SqlClient.SqlClient, sqlClient)),
		})
	}),
)

export const layer = MailboxDeliverySql
