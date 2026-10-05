/**
 * What only a real, shared Postgres can show about the SQL store: several processes working on the
 * same tables, a process that stops in the middle of its work, and a database made by an earlier
 * release. Each "process" here is a store with a connection pool of its own over the same database.
 */
import * as NodeCrypto from '@effect/platform-node/NodeCrypto'
import { describe, it } from '@effect/vitest'
import {
	BatchId,
	ClaimFrozenBatch,
	ClaimWaitingEvents,
	CompleteDelivery,
	DeliveryAccessToken,
	DeliveryAdmission,
	DeliveryControlBackend,
	DeliveryOutputApplied,
	DeliveryOutputSettlement,
	MailboxDelivery,
	MailboxProcessing,
	MailboxProcessingAttemptCompleted,
	MailboxProcessingBackend,
	MailboxProcessingLive,
	PreparedDeliveryInvocation,
	ProviderEventDispatcher,
	ProviderEventHandled,
	ProviderOutputDispatcherLive,
	QueueDeliveryMode,
	RecoverableMailbox,
	Timestamp,
	WaitingMailbox,
	deliveryMailboxKey,
	makeDeliveryId,
	parseDeliveryId,
	type ClaimedMailboxBatch,
	type DeliveryId,
	type MailboxProcessingBackendError,
	type ProviderOutputAttempt,
} from '@humanlayer/channels-delivery'
import {
	Array as Arr,
	Clock,
	Context,
	Deferred,
	Effect,
	Fiber,
	Layer,
	Option,
	Queue,
	Redacted,
	Schema,
} from 'effect'
import { TestClock } from 'effect/testing'
import * as SqlClient from 'effect/sql/SqlClient'

import { migrateBatches, migrateMailboxTables, migrate } from '../src/Migrations'
import { client, emptyTables, storeOverClient } from './postgres'

const leaseMs = 1_000

const admission = (eventId: string, resourceId = 'thread-1') =>
	DeliveryAdmission.make({
		namespace: 'sql',
		provider: 'example',
		installationId: 'installation',
		resourceId,
		eventId,
		payload: { eventId },
	})

const keyOf = (resourceId: string) => deliveryMailboxKey(admission('any', resourceId))

const preparation = PreparedDeliveryInvocation.make({
	callback: 'onEvent',
	presentationVersion: 1,
	destination: { thread: 'thread-1' },
	supportedOperations: ['PresentOutcome', 'AddExternalLink'],
})

type Store = MailboxDelivery | MailboxProcessingBackend | DeliveryControlBackend | SqlClient.SqlClient

/** One process: the store's services over a connection pool of its own. */
const startProcess = Layer.build(storeOverClient().pipe(Layer.provideMerge(Layer.fresh(client))))

/** Empty the shared tables through a short-lived pool. */
const emptyDatabase = emptyTables.pipe(Effect.provide(Layer.fresh(client)))

const inProcess =
	(process: Context.Context<Store>) =>
	<A, E>(effect: Effect.Effect<A, E, Store>) =>
		Effect.provideContext(effect, process)

const nextIdentity = (() => {
	let made = 0
	return () => {
		made += 1
		return {
			batchId: BatchId.make(`sql-batch-${made}`),
			accessToken: DeliveryAccessToken.make(`sql-token-${made}`),
		}
	}
})()

/** Claim everything waiting in a mailbox as a new batch. */
const claimWaiting = (mailboxKey: string) =>
	Effect.gen(function* () {
		const identity = nextIdentity()
		return yield* (yield* MailboxProcessingBackend).claimMailbox(
			ClaimWaitingEvents.make({ mailboxKey, upToSequence: Number.MAX_SAFE_INTEGER, leaseMs, ...identity }),
		)
	})

const claimFrozen = (mailboxKey: string) =>
	Effect.gen(function* () {
		return yield* (yield* MailboxProcessingBackend).claimMailbox(ClaimFrozenBatch.make({ mailboxKey, leaseMs }))
	})

const now = Clock.currentTimeMillis.pipe(Effect.map((millis) => Timestamp.make(millis)))

const recordCompleted = (claim: ClaimedMailboxBatch) =>
	Effect.gen(function* () {
		return yield* (yield* MailboxProcessingBackend).recordProcessingAttemptResult({
			claim,
			result: MailboxProcessingAttemptCompleted.make({}),
			finishedAt: yield* now,
		})
	})

const referenceOf = (claim: { readonly mailboxKey: string; readonly batchId: ClaimedMailboxBatch['batchId'] }) =>
	Option.getOrThrow(parseDeliveryId(makeDeliveryId(claim)))

/** Deliver one event, then claim, prepare, and hand off its batch, and let the callback return. */
const handedOffDelivery = (resourceId: string) =>
	Effect.gen(function* () {
		const backend = yield* MailboxProcessingBackend
		yield* (yield* MailboxDelivery).deliver(admission(`event-${resourceId}`, resourceId))
		const claim = Option.getOrThrow(yield* claimWaiting(keyOf(resourceId)))
		yield* backend.prepareDelivery({ mailboxKey: claim.mailboxKey, claimId: claim.claimId, prepared: preparation })
		yield* backend.handOffDelivery({
			mailboxKey: claim.mailboxKey,
			claimId: claim.claimId,
			handedOffAt: yield* now,
			links: [],
		})
		yield* recordCompleted(claim)
		return claim
	})

const complete = (claim: ClaimedMailboxBatch) =>
	Effect.gen(function* () {
		return yield* (yield* DeliveryControlBackend).applyDeliveryMutation({
			reference: referenceOf(claim),
			accessToken: claim.accessToken,
			mutation: CompleteDelivery.make({ markdown: 'done' }),
		})
	})

const status = (claim: ClaimedMailboxBatch) =>
	Effect.gen(function* () {
		return yield* (yield* DeliveryControlBackend).readDeliveryStatus({
			reference: referenceOf(claim),
			accessToken: claim.accessToken,
		})
	})

const claimRows = Schema.Array(Schema.Struct({ batch_id: Schema.String, status: Schema.String }))

/** Every claim row, as `batch_id:status`, in text order. */
const claimHistory = Effect.gen(function* () {
	const sql = (yield* SqlClient.SqlClient).withoutTransforms()
	const rows = yield* Schema.decodeUnknownEffect(claimRows)(
		yield* sql`SELECT batch_id, status FROM delivery_next_claims`,
	)
	return rows.map(({ batch_id, status }) => `${batch_id}:${status}`).toSorted()
})

/**
 * Mailbox processing for one process, as a poller runs it. Every callback prepares and hands off,
 * and reports its delivery; every output attempt is reported and answered by `answer`.
 */
const processingFor = (input: {
	readonly process: Context.Context<Store>
	readonly handedOff: Queue.Queue<{ readonly deliveryId: DeliveryId; readonly accessToken: string }>
	readonly attempts: Queue.Queue<ProviderOutputAttempt>
	readonly answer: (attempt: ProviderOutputAttempt) => Effect.Effect<DeliveryOutputApplied>
}) =>
	Layer.build(
		MailboxProcessingLive({
			concurrency: 4,
			leaseMs,
			deliveryModeFor: () => QueueDeliveryMode.make({}),
			polling: 'disabled',
		}).pipe(
			Layer.provide(
				Layer.succeed(
					ProviderEventDispatcher,
					ProviderEventDispatcher.of({
						process: (_admissions, execution) =>
							Effect.gen(function* () {
								yield* execution.prepare(preparation)
								yield* execution.context.handoff()
								yield* Queue.offer(input.handedOff, {
									deliveryId: execution.deliveryId,
									accessToken: Redacted.value(execution.context.accessToken),
								})
								return ProviderEventHandled.make({})
							}).pipe(Effect.orDie),
					}),
				),
			),
			Layer.provide(
				ProviderOutputDispatcherLive([
					{
						namespace: 'sql',
						providerName: 'example',
						process: (attempt) =>
							Queue.offer(input.attempts, attempt).pipe(Effect.andThen(input.answer(attempt))),
					},
				]),
			),
			Layer.provide(NodeCrypto.layer),
			Layer.provide(Layer.succeedContext(input.process)),
		),
	)

const processReady = (processing: Context.Context<MailboxProcessing>) =>
	Effect.gen(function* () {
		return yield* (yield* MailboxProcessing).processReady
	}).pipe(Effect.provideContext(processing))

type PollTotals = { readonly claimed: number; readonly output: number }

/**
 * Both pollers pass over the store at the same time, again and again, until a pass finds no work.
 * A pass reports at most the store's claim limit, and a poller skips a mailbox the other has locked,
 * so one pass may not reach everything.
 */
const pollTogetherUntilQuiet = (
	first: Context.Context<MailboxProcessing>,
	second: Context.Context<MailboxProcessing>,
): Effect.Effect<PollTotals, MailboxProcessingBackendError> =>
	Effect.all([processReady(first), processReady(second)], { concurrency: 2 }).pipe(
		Effect.flatMap(([a, b]) => {
			const pass = { claimed: a.claimed + b.claimed, output: a.output + b.output }
			if (pass.claimed + pass.output === 0) return Effect.succeed(pass)
			return pollTogetherUntilQuiet(first, second).pipe(
				Effect.map((rest) => ({ claimed: pass.claimed + rest.claimed, output: pass.output + rest.output })),
			)
		}),
	)

const applied = () => Effect.succeed(DeliveryOutputApplied.make({}))

describe('sql store: several processes', () => {
	it.effect(
		'a poller skips a mailbox another transaction has locked, then takes it once the lock is released',
		({ expect }) =>
			Effect.gen(function* () {
				yield* emptyDatabase
				const first = yield* startProcess
				const second = yield* startProcess
				yield* inProcess(first)(MailboxDelivery.use((delivery) => delivery.deliver(admission('a'))))

				const locked = yield* Deferred.make<void>()
				const release = yield* Deferred.make<void>()
				const holder = yield* inProcess(first)(
					Effect.gen(function* () {
						const sql = yield* SqlClient.SqlClient
						yield* sql.withTransaction(
							Effect.gen(function* () {
								yield* sql`SELECT mailbox_key FROM delivery_next_mailboxes WHERE mailbox_key = ${keyOf('thread-1')} FOR UPDATE`
								yield* Deferred.succeed(locked, undefined)
								yield* Deferred.await(release)
							}),
						)
					}),
				).pipe(Effect.forkChild)
				yield* Deferred.await(locked)

				expect(Option.isNone(yield* inProcess(second)(claimWaiting(keyOf('thread-1'))))).toBe(true)
				yield* Deferred.succeed(release, undefined)
				yield* Fiber.join(holder)
				const claim = Option.getOrThrow(yield* inProcess(second)(claimWaiting(keyOf('thread-1'))))
				expect(claim.admissions.map(({ eventId }) => eventId)).toEqual(['a'])
			}).pipe(Effect.scoped),
	)

	it.effect('two pollers running at once run each batch once and send each output once', ({ expect }) =>
		Effect.gen(function* () {
			yield* emptyDatabase
			const first = yield* startProcess
			const second = yield* startProcess
			const handedOff = yield* Queue.unbounded<{
				readonly deliveryId: DeliveryId
				readonly accessToken: string
			}>()
			const attempts = yield* Queue.unbounded<ProviderOutputAttempt>()
			const pollerA = yield* processingFor({ process: first, handedOff, attempts, answer: applied })
			const pollerB = yield* processingFor({ process: second, handedOff, attempts, answer: applied })
			const threads = Arr.makeBy(12, (index) => `thread-${index}`)
			yield* Effect.forEach(threads, (thread) =>
				inProcess(first)(
					MailboxDelivery.use((delivery) => delivery.deliver(admission(`event-${thread}`, thread))),
				),
			)

			expect(yield* pollTogetherUntilQuiet(pollerA, pollerB)).toEqual({ claimed: 12, output: 0 })
			const deliveries = yield* Queue.takeAll(handedOff)
			expect(new Set(deliveries.map(({ deliveryId }) => deliveryId)).size).toBe(12)
			expect(deliveries).toHaveLength(12)

			yield* Effect.forEach(deliveries, ({ deliveryId, accessToken }) =>
				inProcess(second)(
					DeliveryControlBackend.use((control) =>
						control.applyDeliveryMutation({
							reference: Option.getOrThrow(parseDeliveryId(deliveryId)),
							accessToken,
							mutation: CompleteDelivery.make({ markdown: 'done' }),
						}),
					),
				),
			)
			expect(yield* pollTogetherUntilQuiet(pollerA, pollerB)).toEqual({ claimed: 0, output: 12 })
			const sent = yield* Queue.takeAll(attempts)
			expect(sent.map(({ attempt }) => attempt)).toEqual(Arr.makeBy(12, () => 1))
			expect(new Set(sent.map(({ deliveryId }) => deliveryId)).size).toBe(12)

			const history = yield* inProcess(first)(claimHistory)
			expect(history).toHaveLength(12)
			expect(history.every((row) => row.endsWith(':completed'))).toBe(true)
			for (const { deliveryId, accessToken } of deliveries) {
				const retired = yield* inProcess(first)(
					DeliveryControlBackend.use((control) =>
						control.readDeliveryStatus({
							reference: Option.getOrThrow(parseDeliveryId(deliveryId)),
							accessToken,
						}),
					),
				)
				expect(retired.stage).toBe('Retired')
			}
		}).pipe(Effect.scoped),
	)

	it.effect('a claim whose lease ran out is taken over by another process, and the first is refused', ({ expect }) =>
		Effect.gen(function* () {
			yield* emptyDatabase
			const first = yield* startProcess
			const second = yield* startProcess
			yield* inProcess(first)(MailboxDelivery.use((delivery) => delivery.deliver(admission('a'))))
			const stale = Option.getOrThrow(yield* inProcess(first)(claimWaiting(keyOf('thread-1'))))
			yield* TestClock.adjust(leaseMs)

			expect(
				yield* inProcess(second)(MailboxProcessingBackend.use((backend) => backend.findReadyMailboxes)),
			).toEqual([RecoverableMailbox.make({ mailboxKey: keyOf('thread-1') })])
			const taken = Option.getOrThrow(yield* inProcess(second)(claimFrozen(keyOf('thread-1'))))
			expect(taken.batchId).toBe(stale.batchId)
			expect(taken.attempt).toBe(2)

			const lateRenewal = yield* inProcess(first)(
				MailboxProcessingBackend.use((backend) =>
					backend.renewClaim({ mailboxKey: stale.mailboxKey, claimId: stale.claimId, leaseMs }),
				),
			).pipe(Effect.flip)
			expect(lateRenewal._tag).toBe('MailboxProcessingClaimLost')
			expect((yield* inProcess(first)(recordCompleted(stale)).pipe(Effect.flip))._tag).toBe(
				'MailboxProcessingClaimLost',
			)
			yield* inProcess(second)(recordCompleted(taken))
			expect(yield* inProcess(first)(claimHistory)).toEqual([
				`${stale.batchId}:abandoned`,
				`${stale.batchId}:completed`,
			])
		}).pipe(Effect.scoped),
	)

	it.effect(
		'an output lease that ran out is taken over by another process, which sends it again under the same key',
		({ expect }) =>
			Effect.gen(function* () {
				yield* emptyDatabase
				const first = yield* startProcess
				const second = yield* startProcess
				const claim = yield* inProcess(first)(handedOffDelivery('thread-1'))
				yield* inProcess(second)(complete(claim))
				const claimOutput = (idempotencyKey: string) =>
					MailboxProcessingBackend.use((backend) =>
						backend.claimDeliveryOutput({ mailboxKey: claim.mailboxKey, leaseMs, idempotencyKey }),
					)
				const stale = Option.getOrThrow(yield* inProcess(first)(claimOutput('key-1')))
				expect(Option.isNone(yield* inProcess(second)(claimOutput('key-2')))).toBe(true)
				yield* TestClock.adjust(leaseMs)
				const taken = Option.getOrThrow(yield* inProcess(second)(claimOutput('key-2')))
				expect(taken.operationId).toBe(stale.operationId)
				expect(taken.hadAmbiguousAttempt).toBe(true)
				expect(taken.idempotencyKey).toBe('key-1')

				const settle = (output: typeof stale) =>
					Effect.gen(function* () {
						return yield* (yield* MailboxProcessingBackend).settleDeliveryOutput({
							mailboxKey: output.mailboxKey,
							operationId: output.operationId,
							claimId: output.claimId,
							settlement: DeliveryOutputSettlement.cases.Applied.make({}),
							settledAt: yield* now,
						})
					})
				expect((yield* inProcess(first)(settle(stale)).pipe(Effect.flip))._tag).toBe(
					'MailboxProcessingClaimLost',
				)
				yield* inProcess(second)(settle(taken))
				const retired = yield* inProcess(first)(status(claim))
				expect(retired.stage).toBe('Retired')
				expect(retired.output).toEqual([
					{
						operationId: 'outcome',
						kind: 'PresentOutcome',
						state: 'Delivered',
						attempts: 2,
						hadAmbiguousAttempt: true,
					},
				])
			}).pipe(Effect.scoped),
	)

	it.effect(
		'a process that stops between the provider call and the settle leaves the output to be sent again',
		({ expect }) =>
			Effect.gen(function* () {
				yield* emptyDatabase
				const first = yield* startProcess
				const second = yield* startProcess
				const handedOff = yield* Queue.unbounded<{
					readonly deliveryId: DeliveryId
					readonly accessToken: string
				}>()
				const attempts = yield* Queue.unbounded<ProviderOutputAttempt>()
				/** The provider has applied the output, and the process stops before it can record that. */
				const crashing = yield* processingFor({
					process: first,
					handedOff,
					attempts,
					answer: () => Effect.never,
				})
				const recovering = yield* processingFor({ process: second, handedOff, attempts, answer: applied })

				const claim = yield* inProcess(first)(handedOffDelivery('thread-1'))
				yield* inProcess(first)(MailboxDelivery.use((delivery) => delivery.deliver(admission('follow-up'))))
				yield* inProcess(first)(complete(claim))
				const running = yield* processReady(crashing).pipe(Effect.forkChild)
				const firstAttempt = yield* Queue.take(attempts)
				yield* Fiber.interrupt(running)

				const unsettled = yield* inProcess(second)(status(claim))
				expect(unsettled.stage).toBe('Finishing')
				expect(unsettled.output).toEqual([
					{
						operationId: 'outcome',
						kind: 'PresentOutcome',
						state: 'Delivering',
						attempts: 1,
						hadAmbiguousAttempt: false,
					},
				])
				expect((yield* processReady(recovering)).output).toBe(0)

				yield* TestClock.adjust(leaseMs)
				expect((yield* processReady(recovering)).output).toBe(1)
				const secondAttempt = yield* Queue.take(attempts)
				expect(secondAttempt.attempt).toBe(2)
				expect(secondAttempt.hadAmbiguousAttempt).toBe(true)
				expect(secondAttempt.idempotencyKey).toBe(firstAttempt.idempotencyKey)
				const retired = yield* inProcess(second)(status(claim))
				expect(retired.stage).toBe('Retired')
				expect(retired.output[0]).toMatchObject({ state: 'Delivered', attempts: 2, hadAmbiguousAttempt: true })

				expect((yield* processReady(recovering)).claimed).toBe(1)
				const followUp = yield* Queue.take(handedOff)
				expect(followUp.deliveryId === makeDeliveryId(claim)).toBe(false)
			}).pipe(Effect.scoped),
	)
})

/** Rows as the releases before delivery control wrote them. */
const oldAdmission = (input: {
	readonly sql: SqlClient.SqlClient
	readonly admission: DeliveryAdmission
	readonly claimId: string | null
	readonly arrivedAt: number
}) =>
	Effect.gen(function* () {
		const sql = input.sql.withoutTransforms()
		const mailboxKey = deliveryMailboxKey(input.admission)
		const admissionJson = yield* Schema.encodeEffect(Schema.fromJsonString(DeliveryAdmission))(input.admission)
		yield* sql`INSERT INTO delivery_next_admissions (
				mailbox_key, namespace, provider, event_id, admission_json, arrived_at, claim_id
			) VALUES (
				${mailboxKey}, ${input.admission.namespace}, ${input.admission.provider}, ${input.admission.eventId},
				${admissionJson}, ${input.arrivedAt}, ${input.claimId}
			)`
	})

const oldMailbox = (input: {
	readonly sql: SqlClient.SqlClient
	readonly resourceId: string
	readonly status: 'idle' | 'active' | 'retry'
	readonly readyAt: number | null
}) =>
	input.sql.withoutTransforms()`INSERT INTO delivery_next_mailboxes (mailbox_key, provider, status, ready_at)
		VALUES (${keyOf(input.resourceId)}, 'example', ${input.status}, ${input.readyAt})`

const oldClaim = (input: {
	readonly sql: SqlClient.SqlClient
	readonly resourceId: string
	readonly claimId: string
	readonly status: 'active' | 'retry' | 'completed'
	readonly leaseExpiresAt: number
}) =>
	input.sql.withoutTransforms()`INSERT INTO delivery_next_claims (
			claim_id, mailbox_key, attempt, status, lease_expires_at, claimed_at
		) VALUES (
			${input.claimId}, ${keyOf(input.resourceId)}, 1, ${input.status}, ${input.leaseExpiresAt}, 0
		)`

/** A claim row as the releases with permanent batches wrote it. */
const oldBatchClaim = (input: Parameters<typeof oldClaim>[0] & { readonly batchId: string }) =>
	input.sql.withoutTransforms()`INSERT INTO delivery_next_claims (
			claim_id, mailbox_key, attempt, status, lease_expires_at, claimed_at, batch_id
		) VALUES (
			${input.claimId}, ${keyOf(input.resourceId)}, 1, ${input.status}, ${input.leaseExpiresAt}, 0, ${input.batchId}
		)`

describe('sql store: layout', () => {
	it.effect('keeps the token only in its own column: no delivery JSON holds it, active or retired', ({ expect }) =>
		Effect.gen(function* () {
			yield* emptyDatabase
			const process = yield* startProcess
			const claim = yield* inProcess(process)(handedOffDelivery('thread-1'))
			yield* inProcess(process)(complete(claim))
			const deliveryJson = inProcess(process)(
				Effect.gen(function* () {
					const sql = (yield* SqlClient.SqlClient).withoutTransforms()
					const [row] = yield* Schema.decodeUnknownEffect(
						Schema.Tuple([Schema.Struct({ stage: Schema.String, delivery_json: Schema.String })]),
					)(
						yield* sql`SELECT stage, delivery_json FROM delivery_next_batches WHERE batch_id = ${claim.batchId}`,
					)
					return row
				}),
			)
			const active = yield* deliveryJson
			expect(active.stage).toBe('Finishing')
			expect(active.delivery_json).not.toContain(claim.accessToken)
			expect(active.delivery_json).not.toContain('admissions')

			const output = Option.getOrThrow(
				yield* inProcess(process)(
					MailboxProcessingBackend.use((backend) =>
						backend.claimDeliveryOutput({
							mailboxKey: claim.mailboxKey,
							leaseMs,
							idempotencyKey: '00000000-0000-4000-8000-000000000001',
						}),
					),
				),
			)
			yield* inProcess(process)(
				Effect.gen(function* () {
					yield* (yield* MailboxProcessingBackend).settleDeliveryOutput({
						mailboxKey: output.mailboxKey,
						operationId: output.operationId,
						claimId: output.claimId,
						settlement: DeliveryOutputSettlement.cases.Applied.make({}),
						settledAt: yield* now,
					})
				}),
			)
			const retired = yield* deliveryJson
			expect(retired.stage).toBe('Retired')
			expect(retired.delivery_json).not.toContain(claim.accessToken)
			expect((yield* inProcess(process)(status(claim))).stage).toBe('Retired')
		}).pipe(Effect.scoped),
	)
})

describe('sql store: migration', () => {
	it.effect('brings a database made by the earlier migrations up to date, and its work carries on', ({ expect }) =>
		Effect.gen(function* () {
			const sql = yield* SqlClient.SqlClient
			yield* sql`DROP TABLE IF EXISTS delivery_next_admissions, delivery_next_claims, delivery_next_batches,
				delivery_next_mailboxes CASCADE`
			yield* sql`DROP SEQUENCE IF EXISTS delivery_next_batches_retired_order`

			/** Before permanent batches: a claim running with no batch. */
			yield* migrateMailboxTables
			yield* oldMailbox({ sql, resourceId: 'before-batches', status: 'active', readyAt: leaseMs })
			yield* oldClaim({
				sql,
				resourceId: 'before-batches',
				claimId: 'c0',
				status: 'active',
				leaseExpiresAt: leaseMs,
			})
			yield* oldAdmission({ sql, admission: admission('e0', 'before-batches'), claimId: 'c0', arrivedAt: 0 })

			/** After permanent batches: a prepared batch running, one waiting to retry, and an idle mailbox. */
			const preparedJson = yield* Schema.encodeEffect(Schema.fromJsonString(PreparedDeliveryInvocation))(
				preparation,
			)
			yield* migrateBatches
			yield* oldMailbox({ sql, resourceId: 'running', status: 'active', readyAt: leaseMs })
			yield* sql`INSERT INTO delivery_next_batches (batch_id, mailbox_key, access_token, prepared_json, created_at, prepared_at)
				VALUES ('b1', ${keyOf('running')}, 'token1', ${preparedJson}, 0, 0)`
			yield* oldBatchClaim({
				sql,
				resourceId: 'running',
				claimId: 'c1',
				batchId: 'b1',
				status: 'active',
				leaseExpiresAt: leaseMs,
			})
			yield* oldAdmission({ sql, admission: admission('e1', 'running'), claimId: 'c1', arrivedAt: 0 })
			yield* oldAdmission({ sql, admission: admission('e1-later', 'running'), claimId: null, arrivedAt: 1 })

			yield* oldMailbox({ sql, resourceId: 'retrying', status: 'retry', readyAt: 5_000 })
			yield* sql`INSERT INTO delivery_next_batches (batch_id, mailbox_key, access_token, created_at)
				VALUES ('b2', ${keyOf('retrying')}, 'token2', 0)`
			yield* oldBatchClaim({
				sql,
				resourceId: 'retrying',
				claimId: 'c2',
				batchId: 'b2',
				status: 'retry',
				leaseExpiresAt: leaseMs,
			})
			yield* oldAdmission({ sql, admission: admission('e2', 'retrying'), claimId: 'c2', arrivedAt: 0 })

			yield* oldMailbox({ sql, resourceId: 'idle', status: 'idle', readyAt: 0 })
			yield* sql`INSERT INTO delivery_next_batches (batch_id, mailbox_key, access_token, created_at)
				VALUES ('b3', ${keyOf('idle')}, 'token3', 0)`
			yield* oldBatchClaim({
				sql,
				resourceId: 'idle',
				claimId: 'c3',
				batchId: 'b3',
				status: 'completed',
				leaseExpiresAt: leaseMs,
			})
			yield* oldAdmission({ sql, admission: admission('e3', 'idle'), claimId: 'c3', arrivedAt: 0 })
			yield* oldAdmission({ sql, admission: admission('e3-waiting', 'idle'), claimId: null, arrivedAt: 0 })

			yield* migrate
			yield* migrate

			yield* Effect.gen(function* () {
				const backend = yield* MailboxProcessingBackend
				const ready = yield* backend.findReadyMailboxes
				expect(ready.filter(Schema.is(WaitingMailbox)).map(({ mailboxKey }) => mailboxKey)).toEqual([
					keyOf('idle'),
				])
				expect(ready.filter(Schema.is(RecoverableMailbox))).toEqual([])

				const waiting = Option.getOrThrow(yield* claimWaiting(keyOf('idle')))
				expect(waiting.admissions.map(({ eventId }) => eventId)).toEqual(['e3-waiting'])
				yield* recordCompleted(waiting)

				yield* TestClock.adjust(leaseMs)
				expect((yield* backend.findReadyMailboxes).map(({ mailboxKey }) => mailboxKey).toSorted()).toEqual(
					[keyOf('before-batches'), keyOf('running')].toSorted(),
				)

				const beforeBatches = Option.getOrThrow(yield* claimFrozen(keyOf('before-batches')))
				expect(beforeBatches.attempt).toBe(2)
				expect(beforeBatches.batchId).toMatch(/^legacy-[0-9a-f]{32}$/)
				expect(beforeBatches.admissions.map(({ eventId }) => eventId)).toEqual(['e0'])
				yield* recordCompleted(beforeBatches)

				const running = Option.getOrThrow(yield* claimFrozen(keyOf('running')))
				expect(running.attempt).toBe(2)
				expect(running.batchId).toBe('b1')
				expect(running.accessToken).toBe('token1')
				expect(running.prepared).toEqual(preparation)
				expect(running.admissions.map(({ eventId }) => eventId)).toEqual(['e1'])
				yield* backend.handOffDelivery({
					mailboxKey: running.mailboxKey,
					claimId: running.claimId,
					handedOffAt: yield* now,
					links: [],
				})
				yield* recordCompleted(running)
				expect((yield* status(running)).stage).toBe('ExternalWaiting')
				expect((yield* complete(running)).status).toBe('accepted')
				const output = Option.getOrThrow(
					yield* backend.claimDeliveryOutput({
						mailboxKey: running.mailboxKey,
						leaseMs,
						idempotencyKey: '00000000-0000-4000-8000-000000000001',
					}),
				)
				yield* backend.settleDeliveryOutput({
					mailboxKey: running.mailboxKey,
					operationId: output.operationId,
					claimId: output.claimId,
					settlement: DeliveryOutputSettlement.cases.Applied.make({}),
					settledAt: yield* now,
				})
				expect((yield* status(running)).stage).toBe('Retired')
				const later = Option.getOrThrow(yield* claimWaiting(keyOf('running')))
				expect(later.admissions.map(({ eventId }) => eventId)).toEqual(['e1-later'])

				yield* TestClock.adjust(5_000)
				const retrying = Option.getOrThrow(yield* claimFrozen(keyOf('retrying')))
				expect(retrying.batchId).toBe('b2')
				expect(retrying.attempt).toBe(2)
				expect(retrying.prepared).toBeUndefined()

				expect(yield* claimHistory).toEqual(
					[
						'b1:abandoned',
						'b1:completed',
						'b2:retried',
						'b2:active',
						`${beforeBatches.batchId}:abandoned`,
						`${beforeBatches.batchId}:completed`,
						'b3:completed',
						`${waiting.batchId}:completed`,
						`${later.batchId}:active`,
					].toSorted(),
				)
			}).pipe(Effect.provide(storeOverClient()))
		}).pipe(Effect.provide(Layer.fresh(client))),
	)
})
