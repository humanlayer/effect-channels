import {
	deliveryMailboxKey,
	DeliveryAdmission,
	DeliveryReceipt,
	MailboxDelivery,
	MailboxDeliveryUnavailable,
} from '@humanlayer/channels-delivery-next'
import { Clock, Effect, Layer, Schema } from 'effect'
import * as SqlClient from 'effect/unstable/sql/SqlClient'
import * as SqlError from 'effect/unstable/sql/SqlError'

const insertedRows = Schema.Array(Schema.Struct({ event_id: Schema.NonEmptyString })).check(Schema.isMaxLength(1))
const admissionCodec = Schema.fromJsonString(DeliveryAdmission)

const unavailable = <A, R>(effect: Effect.Effect<A, Schema.SchemaError | SqlError.SqlError, R>) =>
	effect.pipe(
		Effect.tapError((error) => Effect.logError('SQL mailbox admission failed', error)),
		Effect.catchTags({
			SchemaError: () => Effect.fail(new MailboxDeliveryUnavailable({ reason: 'sql_codec_unavailable' })),
			SqlError: () => Effect.fail(new MailboxDeliveryUnavailable({ reason: 'sql_unavailable' })),
		}),
	)

/**
 * The mailbox store's tables.
 *
 * - a mailbox row says whether the mailbox is idle, running a claim, or waiting to retry one, and when it is next due;
 * - a claim row is one attempt at one frozen batch, kept as history;
 * - an admission row is one accepted event. It waits while `claim_id` is null.
 *   A frozen batch is the rows sharing a `claim_id`, in `sequence_id` order.
 *
 * All times are milliseconds from Effect's Clock, passed in as parameters, never the database's clock.
 */
export const migrate = Effect.gen(function* () {
	const sql = (yield* SqlClient.SqlClient).withoutTransforms()
	yield* sql`CREATE TABLE IF NOT EXISTS delivery_next_mailboxes (
		mailbox_key text COLLATE "C" PRIMARY KEY,
		provider text NOT NULL,
		status text NOT NULL DEFAULT 'idle' CHECK (status IN ('idle', 'active', 'retry')),
		ready_at double precision
	)`
	yield* sql`CREATE INDEX IF NOT EXISTS delivery_next_mailboxes_ready
		ON delivery_next_mailboxes (ready_at, mailbox_key) WHERE ready_at IS NOT NULL`
	yield* sql`CREATE TABLE IF NOT EXISTS delivery_next_claims (
		claim_id text PRIMARY KEY,
		mailbox_key text COLLATE "C" NOT NULL REFERENCES delivery_next_mailboxes(mailbox_key),
		attempt bigint NOT NULL CHECK (attempt > 0),
		status text NOT NULL CHECK (status IN ('active', 'retry', 'retried', 'completed', 'failed', 'abandoned')),
		lease_expires_at double precision NOT NULL,
		result_json text,
		claimed_at double precision NOT NULL,
		finished_at double precision
	)`
	yield* sql`CREATE INDEX IF NOT EXISTS delivery_next_claims_mailbox
		ON delivery_next_claims (mailbox_key, claimed_at)`
	yield* sql`CREATE UNIQUE INDEX IF NOT EXISTS delivery_next_claims_live
		ON delivery_next_claims (mailbox_key) WHERE status IN ('active', 'retry')`
	yield* sql`CREATE TABLE IF NOT EXISTS delivery_next_admissions (
		sequence_id bigserial PRIMARY KEY,
		mailbox_key text COLLATE "C" NOT NULL REFERENCES delivery_next_mailboxes(mailbox_key),
		namespace text NOT NULL,
		provider text NOT NULL,
		event_id text NOT NULL,
		admission_json text NOT NULL,
		arrived_at double precision NOT NULL,
		claim_id text REFERENCES delivery_next_claims(claim_id),
		UNIQUE (namespace, provider, event_id)
	)`
	yield* sql`CREATE INDEX IF NOT EXISTS delivery_next_admissions_waiting
		ON delivery_next_admissions (mailbox_key, sequence_id) WHERE claim_id IS NULL`
	yield* sql`CREATE INDEX IF NOT EXISTS delivery_next_admissions_claim
		ON delivery_next_admissions (claim_id, sequence_id) WHERE claim_id IS NOT NULL`
}).pipe(unavailable, Effect.asVoid, Effect.withSpan('delivery.sql.migrate'))

/**
 * Accept an event once. The mailbox row is locked before the insert draws its sequence number,
 * so sequence order is commit order within a mailbox.
 *
 * An arrival wakes an idle mailbox now, even a deferred one. A running or retrying mailbox keeps its due time.
 */
const deliver = Effect.fn('delivery.sql.deliver')(function* (admission: DeliveryAdmission) {
	const now = yield* Clock.currentTimeMillis
	const mailboxKey = deliveryMailboxKey(admission)
	const admissionJson = yield* Schema.encodeEffect(admissionCodec)(admission)
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
			return DeliveryReceipt.make({ mailboxKey, accepted: inserted.length === 1 })
		}),
	)
}, unavailable)

export type MailboxDeliverySqlOptions = { readonly runMigrations: boolean }

export const MailboxDeliverySql = (options: MailboxDeliverySqlOptions) =>
	Layer.effect(
		MailboxDelivery,
		Effect.gen(function* () {
			const sqlClient = yield* SqlClient.SqlClient
			if (options.runMigrations) yield* migrate
			return MailboxDelivery.of({
				deliver: (admission) => deliver(admission).pipe(Effect.provideService(SqlClient.SqlClient, sqlClient)),
			})
		}),
	)

export const layer = MailboxDeliverySql
