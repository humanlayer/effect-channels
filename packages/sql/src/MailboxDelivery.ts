import {
	deliveryMailboxKey,
	DeliveryAdmission,
	DeliveryReceipt,
	MailboxDelivery,
	MailboxDeliveryUnavailable,
} from '@humanlayer/channels-delivery-next'
import { Effect, Layer, Schema } from 'effect'
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

export const migrate = Effect.gen(function* () {
	const sql = (yield* SqlClient.SqlClient).withoutTransforms()
	yield* sql`CREATE TABLE IF NOT EXISTS delivery_next_mailboxes (
		mailbox_key text COLLATE "C" PRIMARY KEY,
		status text NOT NULL DEFAULT 'idle' CHECK (status IN ('idle', 'active', 'retry')),
		claim_id text,
		attempt bigint NOT NULL DEFAULT 0,
		active_batch_json text,
		last_result_json text,
		ready_at double precision
	)`
	yield* sql`CREATE INDEX IF NOT EXISTS delivery_next_mailboxes_ready
		ON delivery_next_mailboxes (ready_at, mailbox_key) WHERE ready_at IS NOT NULL`
	yield* sql`ALTER TABLE delivery_next_mailboxes ADD COLUMN IF NOT EXISTS last_result_json text`
	yield* sql`CREATE TABLE IF NOT EXISTS delivery_next_admissions (
		sequence_id bigserial PRIMARY KEY,
		mailbox_key text COLLATE "C" NOT NULL REFERENCES delivery_next_mailboxes(mailbox_key),
		namespace text NOT NULL,
		provider text NOT NULL,
		event_id text NOT NULL,
		admission_json text NOT NULL,
		consumed boolean NOT NULL DEFAULT false,
		created_at timestamp with time zone NOT NULL DEFAULT now(),
		UNIQUE (namespace, provider, event_id)
	)`
	yield* sql`CREATE INDEX IF NOT EXISTS delivery_next_admissions_pending
		ON delivery_next_admissions (mailbox_key, consumed, sequence_id)`
}).pipe(unavailable, Effect.asVoid, Effect.withSpan('delivery.sql.migrate'))

const deliver = Effect.fn('delivery.sql.deliver')(function* (admission: DeliveryAdmission) {
	const mailboxKey = deliveryMailboxKey(admission)
	const admissionJson = yield* Schema.encodeEffect(admissionCodec)(admission)
	const sql = (yield* SqlClient.SqlClient).withoutTransforms()
	return yield* sql.withTransaction(
		Effect.gen(function* () {
			yield* sql`INSERT INTO delivery_next_mailboxes (mailbox_key)
			VALUES (${mailboxKey}) ON CONFLICT (mailbox_key) DO NOTHING`
			yield* sql`SELECT mailbox_key FROM delivery_next_mailboxes
			WHERE mailbox_key = ${mailboxKey} FOR UPDATE`
			const rows = yield* sql`INSERT INTO delivery_next_admissions (
				mailbox_key, namespace, provider, event_id, admission_json
			) VALUES (
				${mailboxKey}, ${admission.namespace}, ${admission.provider}, ${admission.eventId}, ${admissionJson}
			) ON CONFLICT (namespace, provider, event_id) DO NOTHING
			RETURNING event_id`
			const inserted = yield* Schema.decodeUnknownEffect(insertedRows)(rows)
			if (inserted.length === 1) {
				yield* sql`UPDATE delivery_next_mailboxes
				SET ready_at = CASE WHEN status = 'idle' THEN extract(epoch FROM statement_timestamp()) * 1000 ELSE ready_at END
				WHERE mailbox_key = ${mailboxKey}`
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
