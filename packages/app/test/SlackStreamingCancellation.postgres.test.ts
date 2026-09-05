import { assert, describe, it } from '@effect/vitest'
import { ConversationCoordinator, ConversationStoppedEvent, IdempotencyKey } from '@humanlayer/channels'
import { Context, Deferred, Effect, Fiber, Layer, Queue } from 'effect'
import { SqlClient } from 'effect/unstable/sql'

import {
	createPostgresMessageEvent,
	postgresCoordinatorOptions,
	postgresTestLayer,
	postgresTestsEnabled,
} from '../../channels/test/support/PostgresTestResource.ts'

const buildCoordinator = () =>
	Layer.build(ConversationCoordinator.layerPostgres(postgresCoordinatorOptions())).pipe(
		Effect.map((context) => Context.get(context, ConversationCoordinator)),
	)

describe.skipIf(!postgresTestsEnabled)('Slack stream cancellation with Postgres ownership', () => {
	it.live(
		'lets a non-owner cancel the owner and records cancelled_by_provider before the callback',
		() =>
			Effect.gen(function* () {
				const owner = yield* buildCoordinator()
				const receiver = yield* buildCoordinator()
				const event = yield* createPostgresMessageEvent({ threadKey: 'stream-cancellation' })
				const stop = ConversationStoppedEvent.make({
					orgId: event.orgId,
					provider: event.provider,
					tenant: event.tenant,
					idempotencyKey: IdempotencyKey.make(`evt_${'e'.repeat(32)}`),
					threadRef: event.thread.ref,
					raw: {},
				})
				const entered = yield* Deferred.make<void>()
				const order = yield* Queue.unbounded<string>()
				const worker = yield* owner
					.run((current) =>
						current.idempotencyKey === event.idempotencyKey
							? Deferred.succeed(entered, undefined).pipe(
									Effect.andThen(Effect.never),
									Effect.ensuring(Queue.offer(order, 'finalized')),
								)
							: Effect.sleep(600).pipe(Effect.andThen(Queue.offer(order, 'stopped')), Effect.asVoid),
					)
					.pipe(Effect.forkChild)
				yield* owner.submit(event)
				yield* Deferred.await(entered)
				assert.strictEqual(yield* receiver.submitCancellation(stop), true)
				assert.strictEqual(yield* Queue.take(order), 'finalized')
				assert.strictEqual(yield* Queue.take(order), 'stopped')
				const sql = yield* SqlClient.SqlClient
				const rows = yield* sql<{ readonly attempts: number; readonly status: string }>`
				SELECT attempts, status FROM channels_conversation_mailbox
				WHERE idempotency_key = ${event.idempotencyKey}
			`
				assert.deepStrictEqual(rows, [{ attempts: 0, status: 'cancelled_by_provider' }])
				yield* Fiber.interrupt(worker)
			}).pipe(Effect.provide(postgresTestLayer)),
		30_000,
	)
})
