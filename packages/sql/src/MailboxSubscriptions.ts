/**
 * Implementation of delivery-net's MailboxSubscriptions, tracks the mailbox subscriptions and allows updating
 */
import {
	MailboxSubscriptionCreatedResult,
	MailboxSubscriptionError,
	MailboxSubscriptionAlreadyExistsResult,
	type MailboxSubscriptionOperation,
	MailboxSubscriptions,
} from '@humanlayer/channels-delivery'
import { Effect, Layer, Schema } from 'effect'
import * as SqlClient from 'effect/sql/SqlClient'
import * as SqlError from 'effect/sql/SqlError'

const insertedRows = Schema.Array(
	Schema.Struct({
		mailbox_key: Schema.NonEmptyString,
	}),
).check(Schema.isMaxLength(1))

const subscribedRows = Schema.Tuple([
	Schema.Struct({
		subscribed: Schema.Boolean,
	}),
])

const unavailable = <A, R>(
	operation: MailboxSubscriptionOperation,
	effect: Effect.Effect<A, Schema.SchemaError | SqlError.SqlError, R>,
) =>
	effect.pipe(
		Effect.tapError((error) =>
			Effect.logError('SQL mailbox subscription operation failed', error).pipe(
				Effect.annotateLogs({ operation }),
			),
		),
		Effect.catchTags({
			SchemaError: () =>
				Effect.fail(
					new MailboxSubscriptionError({
						operation,
						reason: 'sql_codec_unavailable',
					}),
				),

			SqlError: () =>
				Effect.fail(
					new MailboxSubscriptionError({
						operation,
						reason: 'sql_unavailable',
					}),
				),
		}),
	)

const subscribe = Effect.fn('delivery.sql.subscriptions.subscribe')(
	function* ({ mailboxKey }: { readonly mailboxKey: string }) {
		const sql = (yield* SqlClient.SqlClient).withoutTransforms()

		const rows = yield* sql`
		INSERT INTO delivery_next_mailbox_subscriptions (
			mailbox_key
		) VALUES (
			${mailboxKey}
		)
		ON CONFLICT (mailbox_key) DO NOTHING
		RETURNING mailbox_key
	`

		const inserted = yield* Schema.decodeUnknownEffect(insertedRows)(rows)

		return inserted.length === 1
			? MailboxSubscriptionCreatedResult.make({})
			: MailboxSubscriptionAlreadyExistsResult.make({})
	},
	(effect) => unavailable('subscribe', effect),
)

const isSubscribed = Effect.fn('delivery.sql.subscriptions.is_subscribed')(
	function* ({ mailboxKey }: { readonly mailboxKey: string }) {
		const sql = (yield* SqlClient.SqlClient).withoutTransforms()

		const rows = yield* sql`
		SELECT EXISTS (
			SELECT 1
			FROM delivery_next_mailbox_subscriptions
			WHERE mailbox_key = ${mailboxKey}
		) AS subscribed
	`

		const [row] = yield* Schema.decodeUnknownEffect(subscribedRows)(rows)

		return row.subscribed
	},
	(effect) => unavailable('is_subscribed', effect),
)

const unsubscribe = Effect.fn('delivery.sql.subscriptions.unsubscribe')(
	function* ({ mailboxKey }: { readonly mailboxKey: string }) {
		const sql = (yield* SqlClient.SqlClient).withoutTransforms()

		yield* sql`
		DELETE FROM delivery_next_mailbox_subscriptions
		WHERE mailbox_key = ${mailboxKey}
	`
	},
	(effect) => unavailable('unsubscribe', effect),
)

/** Mailbox subscriptions over the application's `SqlClient`. It creates no tables: see `MigrationsSql`. */
export const MailboxSubscriptionsSql = Layer.effect(
	MailboxSubscriptions,
	Effect.gen(function* () {
		const sql = yield* SqlClient.SqlClient

		return MailboxSubscriptions.of({
			subscribe: (input) => subscribe(input).pipe(Effect.provideService(SqlClient.SqlClient, sql)),

			isSubscribed: (input) => isSubscribed(input).pipe(Effect.provideService(SqlClient.SqlClient, sql)),

			unsubscribe: (input) => unsubscribe(input).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
		})
	}),
)
