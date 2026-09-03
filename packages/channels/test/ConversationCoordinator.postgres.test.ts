import { fileURLToPath } from 'node:url'

import { NodeServices } from '@effect/platform-node'
import { PgClient } from '@effect/sql-pg'
import { assert, describe, it } from '@effect/vitest'
import {
	Clock,
	Config,
	Context,
	Data,
	Deferred,
	Effect,
	Fiber,
	Layer,
	Option,
	Queue,
	Redacted,
	Ref,
	Schema,
	Stream,
} from 'effect'
import { ChildProcess, ChildProcessSpawner } from 'effect/unstable/process'
import { SqlClient } from 'effect/unstable/sql'

import { ConversationCoordinator, type InboundEvent } from '../src/index.ts'
import {
	createPostgresMessageEvent,
	postgresCoordinatorOptions,
	postgresSchemaUrl,
	postgresTestLayer,
	postgresTestsEnabled,
} from './support/PostgresTestResource.ts'

const CrashIdentityRow = Schema.Struct({ value: Schema.NonEmptyString })

class ChildOwnerFailed extends Data.TaggedError('ChildOwnerFailed')<{ readonly message: string }> {}

const buildCoordinator = (options = postgresCoordinatorOptions()) =>
	Layer.build(ConversationCoordinator.layerPostgres(options)).pipe(
		Effect.map((context) => Context.get(context, ConversationCoordinator)),
	)

describe.skipIf(!postgresTestsEnabled)('ConversationCoordinator.layerPostgres', () => {
	it.effect('deduplicates durably and drains one conversation in FIFO order', () =>
		Effect.gen(function* () {
			const coordinator = yield* buildCoordinator()
			const handled = yield* Queue.unbounded<string>()
			const releaseThird = yield* Deferred.make<void>()
			const threadKey = 'fifo'
			const first = yield* createPostgresMessageEvent({ threadKey })
			const second = yield* createPostgresMessageEvent({ threadKey })
			const third = yield* createPostgresMessageEvent({ threadKey })
			const worker = yield* Effect.forkChild(
				coordinator.run((event) =>
					Queue.offer(handled, event.idempotencyKey).pipe(
						Effect.andThen(
							event.idempotencyKey === third.idempotencyKey ? Deferred.await(releaseThird) : Effect.void,
						),
					),
				),
			)

			yield* coordinator.submit(first)
			yield* coordinator.submit(first)
			yield* coordinator.submit(second)
			yield* coordinator.submit(third)

			assert.strictEqual(yield* Queue.take(handled), first.idempotencyKey)
			assert.strictEqual(yield* Queue.take(handled), second.idempotencyKey)
			assert.strictEqual(yield* Queue.take(handled), third.idempotencyKey)

			const sql = yield* SqlClient.SqlClient
			const completed = yield* sql<{ readonly idempotency_key: string; readonly status: string }>`
				SELECT idempotency_key, status
				FROM channels_conversation_mailbox
				WHERE idempotency_key IN (${first.idempotencyKey}, ${second.idempotencyKey})
				ORDER BY sequence ASC
			`
			assert.deepStrictEqual(completed, [
				{ idempotency_key: first.idempotencyKey, status: 'completed' },
				{ idempotency_key: second.idempotencyKey, status: 'completed' },
			])

			yield* Deferred.succeed(releaseThird, undefined)
			yield* Fiber.interrupt(worker)
		}).pipe(Effect.provide(postgresTestLayer)),
	)

	it.effect('claims different conversations concurrently while excluding a second same-thread owner', () =>
		Effect.gen(function* () {
			const firstCoordinator = yield* buildCoordinator()
			const secondCoordinator = yield* buildCoordinator()
			const entered = yield* Queue.unbounded<string>()
			const sameThreadGate = yield* Deferred.make<void>()
			const otherThreadGate = yield* Deferred.make<void>()
			const activeByThread = yield* Ref.make(new Map<string, number>())
			const overlapDetected = yield* Ref.make(false)
			const sameThreadKey = 'same'
			const otherThreadKey = 'other'
			const first = yield* createPostgresMessageEvent({ threadKey: sameThreadKey })
			const second = yield* createPostgresMessageEvent({ threadKey: sameThreadKey })
			const other = yield* createPostgresMessageEvent({ threadKey: otherThreadKey })

			const handler = (event: InboundEvent) =>
				Effect.gen(function* () {
					const threadId =
						event.idempotencyKey === other.idempotencyKey ? other.thread.ref.id : first.thread.ref.id
					const current = yield* Ref.modify(activeByThread, (active) => {
						const next = new Map(active)
						const count = (next.get(threadId) ?? 0) + 1
						next.set(threadId, count)
						return [count, next]
					})
					if (current > 1) {
						yield* Ref.set(overlapDetected, true)
					}
					yield* Queue.offer(entered, event.idempotencyKey)
					yield* threadId === first.thread.ref.id
						? Deferred.await(sameThreadGate)
						: Deferred.await(otherThreadGate)
					yield* Ref.update(activeByThread, (active) => {
						const next = new Map(active)
						next.delete(threadId)
						return next
					})
				})

			const firstWorker = yield* Effect.forkChild(firstCoordinator.run(handler))
			const secondWorker = yield* Effect.forkChild(secondCoordinator.run(handler))
			yield* firstCoordinator.submit(first)
			yield* firstCoordinator.submit(second)
			yield* firstCoordinator.submit(other)

			const firstEntered = yield* Queue.take(entered)
			const otherEntered = yield* Queue.take(entered)
			assert.ok(
				[firstEntered, otherEntered].includes(first.idempotencyKey) &&
					[firstEntered, otherEntered].includes(other.idempotencyKey),
			)
			assert.strictEqual(yield* Queue.size(entered), 0)
			assert.strictEqual(yield* Ref.get(overlapDetected), false)

			yield* Deferred.succeed(otherThreadGate, undefined)
			yield* Deferred.succeed(sameThreadGate, undefined)
			assert.strictEqual(yield* Queue.take(entered), second.idempotencyKey)
			assert.strictEqual(yield* Ref.get(overlapDetected), false)

			yield* Fiber.interrupt(firstWorker)
			yield* Fiber.interrupt(secondWorker)
		}).pipe(Effect.provide(postgresTestLayer)),
	)

	it.live(
		'renews a live owner beyond the original lease TTL',
		() =>
			Effect.gen(function* () {
				const options = postgresCoordinatorOptions({ leaseTtlMs: 300, heartbeatEveryMs: 75 })
				const firstCoordinator = yield* buildCoordinator(options)
				const secondCoordinator = yield* buildCoordinator(options)
				const entered = yield* Queue.unbounded<string>()
				const release = yield* Deferred.make<void>()
				const event = yield* createPostgresMessageEvent({ threadKey: 'heartbeat' })
				const handler = (value: InboundEvent) =>
					Queue.offer(entered, value.idempotencyKey).pipe(Effect.andThen(Deferred.await(release)))
				const firstWorker = yield* Effect.forkChild(firstCoordinator.run(handler))
				const secondWorker = yield* Effect.forkChild(secondCoordinator.run(handler))

				yield* firstCoordinator.submit(event)
				assert.strictEqual(yield* Queue.take(entered), event.idempotencyKey)
				const sql = yield* SqlClient.SqlClient
				const initial = yield* sql<{ readonly lease_owner: string; readonly lease_expires_at: string }>`
				SELECT lease_owner, lease_expires_at::text AS lease_expires_at
				FROM channels_conversations
				WHERE thread_id = ${event.thread.ref.id}
			`
				const initialLease = initial.at(0)
				assert.ok(initialLease !== undefined)

				yield* Effect.sleep(700)
				assert.strictEqual(yield* Queue.size(entered), 0)
				const renewed = yield* sql<{ readonly lease_owner: string; readonly lease_expires_at: string }>`
				SELECT lease_owner, lease_expires_at::text AS lease_expires_at
				FROM channels_conversations
				WHERE thread_id = ${event.thread.ref.id}
			`
				const renewedLease = renewed.at(0)
				assert.ok(renewedLease !== undefined)
				assert.strictEqual(renewedLease.lease_owner, initialLease.lease_owner)
				assert.ok(Date.parse(renewedLease.lease_expires_at) > Date.parse(initialLease.lease_expires_at))

				yield* Deferred.succeed(release, undefined)
				yield* Fiber.interrupt(firstWorker)
				yield* Fiber.interrupt(secondWorker)
			}).pipe(Effect.provide(postgresTestLayer)),
		5_000,
	)

	it.live(
		'persists attempts and applies capped retry backoff without advancing FIFO',
		() =>
			Effect.gen(function* () {
				const options = postgresCoordinatorOptions({
					retryBaseMs: 50,
					retryMaxMs: 120,
					alertAfterAttempts: 2,
				})
				const coordinator = yield* buildCoordinator(options)
				const attempts = yield* Ref.make(0)
				const entered = yield* Queue.unbounded<number>()
				const release = yield* Deferred.make<void>()
				const threadKey = 'retry'
				const first = yield* createPostgresMessageEvent({ threadKey })
				const second = yield* createPostgresMessageEvent({ threadKey })
				const worker = yield* Effect.forkChild(
					coordinator.run((event) =>
						Effect.gen(function* () {
							if (event.idempotencyKey !== first.idempotencyKey) {
								yield* Queue.offer(entered, 99)
								return
							}
							const attempt = yield* Ref.updateAndGet(attempts, (count) => count + 1)
							yield* Queue.offer(entered, attempt)
							if (attempt <= 3) {
								return yield* Effect.fail('retryable handler failure' as const)
							}
							yield* Deferred.await(release)
						}),
					),
				)

				yield* coordinator.submit(first)
				yield* coordinator.submit(second)
				const observedAt: Array<number> = []
				for (let expected = 1; expected <= 4; expected += 1) {
					assert.strictEqual(yield* Queue.take(entered), expected)
					observedAt.push(yield* Clock.currentTimeMillis)
				}
				assert.ok((observedAt.at(1) ?? 0) - (observedAt.at(0) ?? 0) >= 40)
				assert.ok((observedAt.at(2) ?? 0) - (observedAt.at(1) ?? 0) >= 80)
				assert.ok((observedAt.at(3) ?? 0) - (observedAt.at(2) ?? 0) >= 100)
				assert.strictEqual(yield* Queue.size(entered), 0)

				const sql = yield* SqlClient.SqlClient
				const rows = yield* sql<{ readonly attempts: number; readonly status: string }>`
				SELECT attempts, status
				FROM channels_conversation_mailbox
				WHERE idempotency_key = ${first.idempotencyKey}
			`
				assert.deepStrictEqual(rows, [{ attempts: 3, status: 'pending' }])

				yield* Deferred.succeed(release, undefined)
				assert.strictEqual(yield* Queue.take(entered), 99)
				yield* Fiber.interrupt(worker)
			}).pipe(Effect.provide(postgresTestLayer)),
		5_000,
	)

	it.live(
		'recovers a killed owner only after lease expiry and rejects its stale token',
		() =>
			Effect.gen(function* () {
				const databaseUrl = yield* Config.string('DATABASE_URL')
				const adminSql = yield* SqlClient.SqlClient
				const identity = yield* Effect.acquireRelease(
					Effect.gen(function* () {
						const identityRows = yield* adminSql`
							SELECT replace(gen_random_uuid()::text, '-', '') AS value
						`
						const identity = yield* Schema.decodeUnknownEffect(CrashIdentityRow)(identityRows.at(0))
						const schema = `channels_crash_${identity.value}`
						yield* adminSql`CREATE SCHEMA ${adminSql(schema)}`
						return identity
					}),
					(identity) => {
						const schema = `channels_crash_${identity.value}`
						return adminSql`DROP SCHEMA IF EXISTS ${adminSql(schema)} CASCADE`.pipe(
							Effect.tapError((error) =>
								Effect.logError('failed to drop crash-recovery test schema', error),
							),
							Effect.ignore,
						)
					},
				)
				const schema = `channels_crash_${identity.value}`
				const isolatedUrl = postgresSchemaUrl(databaseUrl, schema)
				const threadKey = `${identity.value}.killed-owner`
				const idempotencyKey = `evt_${identity.value}`
				const childPath = fileURLToPath(new URL('./support/CoordinatorOwnerChild.ts', import.meta.url))
				const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
				const child = yield* spawner.spawn(
					ChildProcess.make('bun', [childPath], {
						env: {
							DATABASE_URL: isolatedUrl,
							CHANNELS_TEST_THREAD_KEY: threadKey,
							CHANNELS_TEST_IDEMPOTENCY_KEY: idempotencyKey,
						},
						extendEnv: true,
						stdin: 'ignore',
					}),
				)
				const claimedLine = yield* child.stdout.pipe(
					Stream.decodeText(),
					Stream.splitLines,
					Stream.filter((line) => line.includes('CLAIMED')),
					Stream.runHead,
				)
				if (Option.isNone(claimedLine)) {
					return yield* new ChildOwnerFailed({ message: 'coordinator owner child exited before claiming' })
				}

				const parent = Effect.gen(function* () {
					const sql = yield* SqlClient.SqlClient
					const oldLeaseRows = yield* sql<{ readonly lease_token: string }>`
					SELECT lease_token
					FROM channels_conversations
					WHERE thread_id = ${`slack:v1:T_TEST:C_TEST:${threadKey}`}
				`
					const oldLease = oldLeaseRows.at(0)
					assert.ok(oldLease !== undefined)
					yield* child.kill({ killSignal: 'SIGKILL' })
					yield* Effect.result(child.exitCode)

					const coordinator = yield* buildCoordinator(
						postgresCoordinatorOptions({ leaseTtlMs: 2_000, heartbeatEveryMs: 500 }),
					)
					const entered = yield* Deferred.make<void>()
					const release = yield* Deferred.make<void>()
					const worker = yield* Effect.forkChild(
						coordinator.run(() =>
							Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release))),
						),
					)

					const tooEarly = yield* Deferred.await(entered).pipe(Effect.timeoutOption(500))
					assert.ok(Option.isNone(tooEarly))
					yield* Deferred.await(entered).pipe(
						Effect.timeoutOrElse({ duration: 3_000, orElse: () => Effect.die('timeout') }),
					)

					const successorRows = yield* sql<{ readonly lease_token: string }>`
					SELECT lease_token
					FROM channels_conversations
					WHERE thread_id = ${`slack:v1:T_TEST:C_TEST:${threadKey}`}
				`
					const successor = successorRows.at(0)
					assert.ok(successor !== undefined)
					assert.notStrictEqual(successor.lease_token, oldLease.lease_token)
					const staleExtend = yield* sql`
					UPDATE channels_conversations
					SET lease_expires_at = now() + interval '1 minute'
					WHERE thread_id = ${`slack:v1:T_TEST:C_TEST:${threadKey}`}
						AND lease_token = ${oldLease.lease_token}
					RETURNING thread_id
				`
					const staleRelease = yield* sql`
					UPDATE channels_conversations
					SET lease_token = NULL, lease_owner = NULL, lease_expires_at = NULL
					WHERE thread_id = ${`slack:v1:T_TEST:C_TEST:${threadKey}`}
						AND lease_token = ${oldLease.lease_token}
					RETURNING thread_id
				`
					assert.strictEqual(staleExtend.length, 0)
					assert.strictEqual(staleRelease.length, 0)

					yield* Deferred.succeed(release, undefined)
					yield* Fiber.interrupt(worker)
				})
				yield* parent.pipe(
					Effect.provide(PgClient.layer({ url: Redacted.make(isolatedUrl), maxConnections: 8 })),
				)
			}).pipe(
				Effect.scoped,
				Effect.provide(
					Layer.merge(PgClient.layerConfig({ url: Config.redacted('DATABASE_URL') }), NodeServices.layer),
				),
			),
		7_000,
	)
})
