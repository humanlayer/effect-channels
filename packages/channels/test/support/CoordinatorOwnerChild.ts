import { NodeRuntime } from '@effect/platform-node'
import { PgClient } from '@effect/sql-pg'
import { Config, Effect, Layer, Redacted } from 'effect'

import { ConversationCoordinator } from '../../src/index.ts'
import { createPostgresMessageEvent, postgresCoordinatorOptions } from './PostgresTestResource.ts'

const program = Effect.gen(function* () {
	const databaseUrl = yield* Config.string('DATABASE_URL')
	const threadKey = yield* Config.string('CHANNELS_TEST_THREAD_KEY')
	const idempotencyKey = yield* Config.string('CHANNELS_TEST_IDEMPOTENCY_KEY')
	const database = PgClient.layer({ url: Redacted.make(databaseUrl), maxConnections: 4 })
	const coordinatorLayer = ConversationCoordinator.layerPostgres(
		postgresCoordinatorOptions({ leaseTtlMs: 2_000, heartbeatEveryMs: 500 }),
	).pipe(Layer.provide(database))
	const child = Effect.gen(function* () {
		const coordinator = yield* ConversationCoordinator
		yield* coordinator.submit(
			yield* createPostgresMessageEvent({
				threadKey,
				idempotencyKey: idempotencyKey.slice(4),
			}),
		)
		return yield* coordinator.run(() =>
			Effect.sync(() => process.stdout.write('CLAIMED\n')).pipe(Effect.andThen(Effect.never)),
		)
	})
	return yield* child.pipe(Effect.provide(coordinatorLayer), Effect.scoped)
})

NodeRuntime.runMain(program)
