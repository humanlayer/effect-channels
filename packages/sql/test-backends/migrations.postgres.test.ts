/**
 * `ChannelsSql.make` sets the database up once per start: building its layer runs `migrate` once,
 * however many of the store's services need the tables, and the subscriptions table comes with it.
 * Each run is counted by its `delivery.sql.migrate` span, through a recording tracer.
 */
import { describe, it } from '@effect/vitest'
import { Effect, Layer, Schema, Tracer } from 'effect'
import * as SqlClient from 'effect/unstable/sql/SqlClient'

import { ChannelsSql } from '../src'
import { client } from './postgres'

/** A tracer that records the name of every span it starts. */
const makeRecordingTracer = () => {
	const names: Array<string> = []
	const tracer = Tracer.make({
		span(options) {
			names.push(options.name)
			return new Tracer.NativeSpan(options)
		},
	})
	return { tracer, migrations: () => names.filter((name) => name.includes('migrate')) }
}

const storage = (runMigrations: boolean) =>
	ChannelsSql.make({ claimLimit: 10, runMigrations, polling: { intervalMs: 10 } })

const subscriptionsTable = Effect.gen(function* () {
	const sql = yield* SqlClient.SqlClient
	const rows = yield* sql`SELECT to_regclass('delivery_next_mailbox_subscriptions')::text AS name`
	return yield* Schema.decodeUnknownEffect(Schema.Tuple([Schema.Struct({ name: Schema.NullOr(Schema.String) })]))(
		rows,
	)
})

describe('sql store: setup', () => {
	it.effect('one ChannelsSql.make migrates once, subscriptions included', ({ expect }) =>
		Effect.gen(function* () {
			const sql = yield* SqlClient.SqlClient
			yield* sql`DROP TABLE IF EXISTS delivery_next_mailbox_subscriptions`
			const recording = makeRecordingTracer()

			yield* Layer.build(storage(true).layer).pipe(Effect.withTracer(recording.tracer))

			expect(recording.migrations()).toEqual(['delivery.sql.migrate'])
			expect(yield* subscriptionsTable).toEqual([{ name: 'delivery_next_mailbox_subscriptions' }])
		}).pipe(Effect.scoped, Effect.provide(client)),
	)

	it.effect('runs no migration when runMigrations is off', ({ expect }) =>
		Effect.gen(function* () {
			const recording = makeRecordingTracer()

			yield* Layer.build(storage(false).layer).pipe(Effect.withTracer(recording.tracer))

			expect(recording.migrations()).toEqual([])
		}).pipe(Effect.scoped, Effect.provide(client)),
	)
})
