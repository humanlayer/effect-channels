import { Context, Effect, Layer, Schema } from 'effect'
import * as SqlClient from 'effect/unstable/sql/SqlClient'

import { deliveryIds, MailboxState } from '../Mailbox'
import {
	CommitMailbox,
	DeliveryLocatorStore,
	LoadMailbox,
	LocateDelivery,
	MailboxReadiness,
	MailboxStore,
	ScanReady,
} from '../MailboxStore'
import { DeliveryId } from '../protocol'
import { storeErrors } from './errors'
import { migrate } from './migrations'

const stateCodec = Schema.fromJsonString(MailboxState)
const storedRows = Schema.Array(
	Schema.Struct({
		revision: Schema.Natural,
		state_json: stateCodec,
		ready_at: Schema.NullOr(Schema.Finite),
	}).check(Schema.makeFilter((row) => row.ready_at === row.state_json.readyAt)),
).check(Schema.isMaxLength(1))
const changedRows = Schema.Array(Schema.Struct({ key: Schema.String })).check(Schema.isMaxLength(1))
const readyRows = Schema.Array(Schema.Struct({ key: Schema.String }))
const locatorRows = Schema.Array(Schema.Struct({ mailbox_key: Schema.NonEmptyString })).check(Schema.isMaxLength(1))

const loadMailbox = Effect.fn('delivery.postgres.load')(
	function* (input: LoadMailbox) {
		yield* Schema.decodeEffect(LoadMailbox)(input)
		const sql = (yield* SqlClient.SqlClient).withoutTransforms()
		const rows = yield* sql`SELECT revision::double precision AS revision, state_json, ready_at
		FROM humanlayer_delivery_v1_mailboxes WHERE key = ${input.key}`
		const [row] = yield* Schema.decodeUnknownEffect(storedRows)(rows)
		return row === undefined ? undefined : { revision: row.revision, state: row.state_json }
	},
	storeErrors({ operation: 'load' }),
)

const commitMailbox = Effect.fn('delivery.postgres.commit')(
	function* (input: CommitMailbox) {
		yield* Schema.decodeEffect(CommitMailbox)(input)
		const revision = yield* Schema.decodeEffect(Schema.Natural)((input.expectedRevision ?? -1) + 1)
		const json = yield* Schema.encodeEffect(stateCodec)(input.nextState)
		const sql = (yield* SqlClient.SqlClient).withoutTransforms()
		const commit = Effect.gen(function* () {
			const rows =
				input.expectedRevision === null
					? yield* sql`INSERT INTO humanlayer_delivery_v1_mailboxes (key, revision, state_json, ready_at)
			VALUES (${input.key}, ${revision}, ${json}, ${input.nextState.readyAt})
			ON CONFLICT (key) DO NOTHING RETURNING key`
					: yield* sql`UPDATE humanlayer_delivery_v1_mailboxes
			SET revision = ${revision}, state_json = ${json}, ready_at = ${input.nextState.readyAt}
			WHERE key = ${input.key} AND revision = ${input.expectedRevision} RETURNING key`
			const changed = yield* Schema.decodeUnknownEffect(changedRows)(rows)
			if (changed.length === 0) return 'conflict' as const
			const opaqueIds = deliveryIds(input.nextState).filter((deliveryId) => deliveryId.startsWith('delivery:v2:'))
			if (opaqueIds.length > 0) {
				const encodedIds = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Array(DeliveryId)))(
					opaqueIds,
				)
				const indexed = yield* sql`INSERT INTO humanlayer_delivery_v1_locators (delivery_id, mailbox_key)
				SELECT value, ${input.key} FROM jsonb_array_elements_text(${encodedIds}::jsonb)
				ON CONFLICT (delivery_id) DO UPDATE SET mailbox_key = EXCLUDED.mailbox_key
				WHERE humanlayer_delivery_v1_locators.mailbox_key = EXCLUDED.mailbox_key
				RETURNING delivery_id`
				yield* Schema.decodeUnknownEffect(
					Schema.Array(Schema.Struct({ delivery_id: DeliveryId })).check(
						Schema.makeFilter((entries) => entries.length === opaqueIds.length),
					),
				)(indexed)
			}
			return 'committed' as const
		})
		return yield* sql.withTransaction(commit)
	},
	storeErrors({ operation: 'commit' }),
)

const locateDelivery = Effect.fn('delivery.postgres.locate')(
	function* (input: LocateDelivery) {
		yield* Schema.decodeEffect(LocateDelivery)(input)
		const sql = (yield* SqlClient.SqlClient).withoutTransforms()
		const rows = yield* sql`SELECT mailbox_key FROM humanlayer_delivery_v1_locators
		WHERE delivery_id = ${input.deliveryId}`
		return (yield* Schema.decodeUnknownEffect(locatorRows)(rows))[0]?.mailbox_key
	},
	storeErrors({ operation: 'locate' }),
)

const scanReady = Effect.fn('delivery.postgres.scan')(
	function* (input: ScanReady) {
		yield* Schema.decodeEffect(ScanReady)(input)
		const pattern = `${input.prefix.replaceAll('!', '!!').replaceAll('%', '!%').replaceAll('_', '!_')}%`
		const sql = (yield* SqlClient.SqlClient).withoutTransforms()
		const rows = yield* sql`SELECT key FROM humanlayer_delivery_v1_mailboxes
		WHERE key LIKE ${pattern} ESCAPE '!' AND ready_at <= ${input.now}
		ORDER BY ready_at, key LIMIT ${input.limit}`
		return (yield* Schema.decodeUnknownEffect(
			readyRows.check(
				Schema.isMaxLength(input.limit),
				Schema.makeFilter((entries) => entries.every((row) => row.key.startsWith(input.prefix))),
			),
		)(rows)).map((row) => row.key)
	},
	storeErrors({ operation: 'scan' }),
)

export const layer = Layer.effectContext(
	Effect.gen(function* () {
		const sql = yield* SqlClient.SqlClient
		yield* migrate
		return Context.make(
			MailboxStore,
			MailboxStore.of({
				loadMailbox: (input) => loadMailbox(input).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
				commitMailbox: (input) => commitMailbox(input).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
			}),
		).pipe(
			Context.add(
				MailboxReadiness,
				MailboxReadiness.of({
					scanReady: (input) => scanReady(input).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
				}),
			),
			Context.add(
				DeliveryLocatorStore,
				DeliveryLocatorStore.of({
					locateDelivery: (input) =>
						locateDelivery(input).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
				}),
			),
		)
	}),
)
