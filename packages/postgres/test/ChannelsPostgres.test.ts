import { assert, describe, it } from '@effect/vitest'
import { ConversationCoordinator, SubscriptionCreated, Subscriptions, ThreadId } from '@humanlayer/channels'
import { Context, Effect, Layer, Random, Schema } from 'effect'
import { SqlClient } from 'effect/unstable/sql'

import { layerConfig } from '../src/index.ts'

const postgresTestsEnabled = import.meta.env.DATABASE_URL !== undefined
const applicationLayer = () =>
	Subscriptions.layer.pipe(
		Layer.provideMerge(
			layerConfig({
				leaseTtlMs: 1_000,
				heartbeatEveryMs: 250,
				acquireTimeoutMs: 2_000,
				retryBaseMs: 50,
				retryMaxMs: 200,
				alertAfterAttempts: 2,
			}),
		),
	)

describe.skipIf(!postgresTestsEnabled)('@humanlayer/channels-postgres', () => {
	it.effect('composes PgClient, coordinator, and shared SQL persistence', () =>
		Effect.gen(function* () {
			const firstContext = yield* Layer.build(applicationLayer())
			const secondContext = yield* Layer.build(applicationLayer())
			const firstSubscriptions = Context.get(firstContext, Subscriptions)
			const secondSubscriptions = Context.get(secondContext, Subscriptions)
			const threadId = ThreadId.make(`slack:v1:T_TEST:C_TEST:${yield* Random.nextInt}`)

			const transition = yield* firstSubscriptions.subscribe({ threadId })
			assert.ok(Schema.is(SubscriptionCreated)(transition))
			assert.strictEqual(yield* secondSubscriptions.isSubscribed({ threadId }), true)

			assert.ok(Context.get(firstContext, ConversationCoordinator))
			const sql = Context.get(firstContext, SqlClient.SqlClient)
			const tables = yield* sql<{ readonly table_name: string }>`
				SELECT table_name
				FROM information_schema.tables
				WHERE table_schema = current_schema()
					AND table_name IN ('channels_conversations', 'channels_conversation_mailbox', 'effect_persistence')
			`
			assert.deepStrictEqual(
				new Set(tables.map((row) => row.table_name)),
				new Set(['channels_conversations', 'channels_conversation_mailbox', 'effect_persistence']),
			)
		}),
	)
})
