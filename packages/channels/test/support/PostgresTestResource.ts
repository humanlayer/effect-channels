import { PgClient } from '@effect/sql-pg'
import { Clock, Config, DateTime, Effect, Layer, Random, Redacted, Schema } from 'effect'
import { SqlClient } from 'effect/unstable/sql'

import {
	IdempotencyKey,
	Message,
	MessageEvent,
	MessageRef,
	NewMentionDelivery,
	OrgId,
	TenantId,
	Thread,
	type ConversationCoordinatorOptions,
} from '../../src/index.ts'
import { testAuthor, testThreadRefFor } from '../support.ts'

export const postgresTestsEnabled = import.meta.env.DATABASE_URL !== undefined

export const postgresSchemaUrl = (url: string, schema: string) => {
	const parsed = new URL(url)
	parsed.searchParams.set('options', `-c search_path=${schema}`)
	return parsed.toString()
}

const TestSchemaRow = Schema.Struct({ schema_name: Schema.NonEmptyString })

const admin = PgClient.layerConfig({ url: Config.redacted('DATABASE_URL') })

export const postgresTestLayer = Layer.unwrap(
	Effect.gen(function* () {
		const url = yield* Config.string('DATABASE_URL')
		const sql = yield* SqlClient.SqlClient
		const rows = yield* sql`SELECT 'channels_test_' || replace(gen_random_uuid()::text, '-', '') AS schema_name`
		const row = yield* Schema.decodeUnknownEffect(TestSchemaRow)(rows.at(0))
		const schema = row.schema_name
		yield* sql`CREATE SCHEMA ${sql(schema)}`
		yield* Effect.addFinalizer(() =>
			sql`DROP SCHEMA IF EXISTS ${sql(schema)} CASCADE`.pipe(
				Effect.tapError((error) => Effect.logError('failed to drop Postgres test schema', error)),
				Effect.ignore,
			),
		)
		return PgClient.layer({
			url: Redacted.make(postgresSchemaUrl(url, schema)),
			maxConnections: 16,
		})
	}),
).pipe(Layer.provide(admin))

export const postgresCoordinatorOptions = (overrides?: Partial<ConversationCoordinatorOptions>) => ({
	leaseTtlMs: 1_000,
	heartbeatEveryMs: 250,
	acquireTimeoutMs: 2_000,
	retryBaseMs: 50,
	retryMaxMs: 200,
	alertAfterAttempts: 2,
	...overrides,
})

let eventSequence = 0

export const createPostgresMessageEvent = Effect.fnUntraced(function* (input?: {
	readonly threadKey?: string
	readonly idempotencyKey?: string
}) {
	eventSequence += 1
	const now = yield* Clock.currentTimeMillis
	const messageTs = `${now}.${eventSequence}`
	const rootTs = input?.threadKey ?? messageTs
	const threadRef = testThreadRefFor(rootTs, input?.threadKey === undefined)
	const message = Message.make({
		ref: MessageRef.make(messageTs),
		threadRef,
		text: `message ${messageTs}`,
		markdown: `message ${messageTs}`,
		author: testAuthor,
		metadata: { sentAt: DateTime.makeUnsafe({ epochMilliseconds: now }) },
		attachments: [],
		raw: { ts: messageTs },
	})
	let key = input?.idempotencyKey
	if (key === undefined) {
		const parts = yield* Effect.forEach([0, 1, 2, 3], () => Random.nextIntBetween(0, 0xffff_ffff))
		key = parts.map((part) => part.toString(16).padStart(8, '0')).join('')
	}
	return MessageEvent.make({
		orgId: OrgId.make('org_test'),
		provider: 'slack',
		tenant: TenantId.make('T_TEST'),
		idempotencyKey: IdempotencyKey.make(`evt_${key}`),
		thread: Thread.make({ ref: threadRef, currentMessage: message, recentMessages: [message] }),
		message,
		delivery: NewMentionDelivery.make({ location: input?.threadKey === undefined ? 'channel_root' : 'thread' }),
		raw: { type: 'app_mention', eventSequence },
	})
})
