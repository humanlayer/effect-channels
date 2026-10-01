/**
 * The mailbox store's tables, and the ordered steps that create them.
 *
 * Each step only adds: new tables, new nullable columns, new indexes, and backfills of rows the step
 * before it left. Every statement is safe to run again, so `migrate` runs every step every time.
 * A database made by an earlier release runs only what it lacks. Steps never edit an earlier
 * `CREATE TABLE`: a database that already has the table would never see the change.
 *
 * All times are milliseconds from Effect's Clock, passed in as parameters, never the database's clock.
 */
import { MailboxDeliveryUnavailable } from '@humanlayer/channels-delivery-next'
import { Effect, Layer, Schema } from 'effect'
import * as SqlClient from 'effect/unstable/sql/SqlClient'
import * as SqlError from 'effect/unstable/sql/SqlError'

const unavailable = <A, R>(effect: Effect.Effect<A, Schema.SchemaError | SqlError.SqlError, R>) =>
	effect.pipe(
		Effect.tapError((error) => Effect.logError('SQL mailbox migration failed', error)),
		Effect.catchTags({
			SchemaError: () => Effect.fail(new MailboxDeliveryUnavailable({ reason: 'sql_codec_unavailable' })),
			SqlError: () => Effect.fail(new MailboxDeliveryUnavailable({ reason: 'sql_unavailable' })),
		}),
	)

/**
 * The first tables.
 *
 * - a mailbox row says whether the mailbox is idle, running a claim, or waiting to retry one, and when it is next due;
 * - a claim row is one attempt at one frozen batch, kept as history;
 * - an admission row is one accepted event. It waits while `claim_id` is null.
 */
export const migrateMailboxTables = Effect.gen(function* () {
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
})

/**
 * Permanent batches.
 *
 * - a batch row is one frozen batch for good: its permanent ID, its remote-worker token, and the
 *   callback choice saved by its first prepare;
 * - each claim row belongs to one batch, and a retry or recovery adds a claim row for the same batch.
 *
 * A claim still live from before this step gets a batch of its own, with a fresh token.
 */
export const migrateBatches = Effect.gen(function* () {
	const sql = (yield* SqlClient.SqlClient).withoutTransforms()
	yield* sql`CREATE TABLE IF NOT EXISTS delivery_next_batches (
		batch_id text COLLATE "C" PRIMARY KEY,
		mailbox_key text COLLATE "C" NOT NULL REFERENCES delivery_next_mailboxes(mailbox_key),
		access_token text NOT NULL,
		prepared_json text,
		created_at double precision NOT NULL,
		prepared_at double precision
	)`
	yield* sql`ALTER TABLE delivery_next_claims
		ADD COLUMN IF NOT EXISTS batch_id text COLLATE "C" REFERENCES delivery_next_batches(batch_id)`
	yield* sql`CREATE INDEX IF NOT EXISTS delivery_next_claims_batch
		ON delivery_next_claims (batch_id, claimed_at)`
	yield* sql`INSERT INTO delivery_next_batches (batch_id, mailbox_key, access_token, created_at)
		SELECT 'legacy-' || md5(claim_id), mailbox_key,
			replace(gen_random_uuid()::text || gen_random_uuid()::text, '-', ''), claimed_at
		FROM delivery_next_claims WHERE batch_id IS NULL AND status IN ('active', 'retry')
		ON CONFLICT (batch_id) DO NOTHING`
	yield* sql`UPDATE delivery_next_claims SET batch_id = 'legacy-' || md5(claim_id)
		WHERE batch_id IS NULL AND status IN ('active', 'retry')`
})

/**
 * Delivery control: handoff, remote results, and provider output.
 *
 * - a mailbox row points at its active batch. `status` stays the scheduler's view: `idle` with no
 *   active batch, `retry` while the batch waits to retry, `active` otherwise;
 * - a batch row keeps its stage. While it is active, `delivery_json` holds the rest of the shared
 *   lifecycle's `ActiveDelivery`: attempt, claim, result, links, and output operations. Once it
 *   retires, `delivery_json` holds the `RetainedDelivery` that status reads, until `retain_until`;
 * - an admission row names its batch for good. Retries add claim rows; admissions never move.
 *
 * A batch that was running or waiting to retry before this step becomes the mailbox's active batch
 * in `Local` or `Retry`, with no links and no output, as the lifecycle would have left it.
 */
export const migrateDeliveryControl = Effect.gen(function* () {
	const sql = (yield* SqlClient.SqlClient).withoutTransforms()
	yield* sql`ALTER TABLE delivery_next_batches
		ADD COLUMN IF NOT EXISTS stage text CHECK (stage IN (
			'Local', 'Retry', 'ExternalCleaning', 'ExternalWaiting', 'Finishing', 'Retired', 'Pruned'
		))`
	yield* sql`ALTER TABLE delivery_next_batches ADD COLUMN IF NOT EXISTS delivery_json text`
	yield* sql`ALTER TABLE delivery_next_batches ADD COLUMN IF NOT EXISTS retain_until double precision`
	yield* sql`ALTER TABLE delivery_next_batches ADD COLUMN IF NOT EXISTS retired_order bigint`
	yield* sql`CREATE SEQUENCE IF NOT EXISTS delivery_next_batches_retired_order`
	yield* sql`CREATE INDEX IF NOT EXISTS delivery_next_batches_retained
		ON delivery_next_batches (mailbox_key, retired_order) WHERE stage = 'Retired'`
	yield* sql`ALTER TABLE delivery_next_mailboxes
		ADD COLUMN IF NOT EXISTS active_batch_id text COLLATE "C" REFERENCES delivery_next_batches(batch_id)`
	yield* sql`ALTER TABLE delivery_next_admissions
		ADD COLUMN IF NOT EXISTS batch_id text COLLATE "C" REFERENCES delivery_next_batches(batch_id)`
	yield* sql`CREATE INDEX IF NOT EXISTS delivery_next_admissions_batch
		ON delivery_next_admissions (batch_id, sequence_id) WHERE batch_id IS NOT NULL`
	yield* sql`UPDATE delivery_next_admissions admission SET batch_id = claim.batch_id
		FROM delivery_next_claims claim
		WHERE admission.claim_id = claim.claim_id AND admission.batch_id IS NULL AND claim.batch_id IS NOT NULL`
	yield* sql`UPDATE delivery_next_batches batch
		SET stage = CASE claim.status WHEN 'active' THEN 'Local' ELSE 'Retry' END,
			delivery_json = json_build_object(
				'attempt', claim.attempt,
				'claimId', CASE claim.status WHEN 'active' THEN claim.claim_id END,
				'links', json_build_array(),
				'operations', json_build_array()
			)::text
		FROM delivery_next_claims claim
		WHERE claim.batch_id = batch.batch_id AND claim.status IN ('active', 'retry') AND batch.stage IS NULL`
	yield* sql`UPDATE delivery_next_mailboxes mailbox SET active_batch_id = claim.batch_id
		FROM delivery_next_claims claim
		WHERE claim.mailbox_key = mailbox.mailbox_key AND claim.status IN ('active', 'retry')
			AND mailbox.status IN ('active', 'retry') AND mailbox.active_batch_id IS NULL`
})

/** Mailbox subscriptions: a row for each mailbox whose conversation the bot follows. */
export const migrateMailboxSubscriptions = Effect.gen(function* () {
	const sql = (yield* SqlClient.SqlClient).withoutTransforms()
	yield* sql`CREATE TABLE IF NOT EXISTS delivery_next_mailbox_subscriptions (
		mailbox_key text COLLATE "C" PRIMARY KEY,
		created_at timestamp with time zone NOT NULL DEFAULT now()
	)`
})

/**
 * Create or bring up to date every mailbox table, in order. Stores that start together, such as
 * several pollers, take turns: a transaction-scoped advisory lock lets one migrate at a time.
 */
export const migrate = Effect.gen(function* () {
	const sql = (yield* SqlClient.SqlClient).withoutTransforms()
	yield* sql.withTransaction(
		Effect.gen(function* () {
			yield* sql`SELECT pg_advisory_xact_lock(hashtext('delivery_next_migrate'))`
			yield* migrateMailboxTables
			yield* migrateBatches
			yield* migrateDeliveryControl
			yield* migrateMailboxSubscriptions
		}),
	)
}).pipe(unavailable, Effect.asVoid, Effect.withSpan('delivery.sql.migrate'))

/**
 * Runs `migrate` once when built. `ChannelsSql.make` provides it ahead of the store's services when
 * `runMigrations` is on, so one application start migrates once.
 */
export const MigrationsSql = Layer.effectDiscard(migrate)
