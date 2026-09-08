import { Context, Effect, Layer, Schema } from 'effect'
import * as SqlClient from 'effect/unstable/sql/SqlClient'

import { MailboxState } from '../Mailbox.js'
import { CommitMailbox, LoadMailbox, MailboxReadiness, MailboxStore, ScanReady } from '../MailboxStore.js'
import { storeErrors } from './errors.js'
import { migrate } from './migrations.js'

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
		const rows =
			input.expectedRevision === null
				? yield* sql`INSERT INTO humanlayer_delivery_v1_mailboxes (key, revision, state_json, ready_at)
			VALUES (${input.key}, ${revision}, ${json}, ${input.nextState.readyAt})
			ON CONFLICT (key) DO NOTHING RETURNING key`
				: yield* sql`UPDATE humanlayer_delivery_v1_mailboxes
			SET revision = ${revision}, state_json = ${json}, ready_at = ${input.nextState.readyAt}
			WHERE key = ${input.key} AND revision = ${input.expectedRevision} RETURNING key`
		const changed = yield* Schema.decodeUnknownEffect(changedRows)(rows)
		return changed.length === 1 ? ('committed' as const) : ('conflict' as const)
	},
	storeErrors({ operation: 'commit' }),
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
		)
	}),
)
