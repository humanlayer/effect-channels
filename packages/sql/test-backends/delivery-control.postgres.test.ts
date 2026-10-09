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
import { Array as Arr, Clock, Context, Deferred, Effect, Fiber, Layer, Option, Queue, Redacted, Schema } from 'effect'
import * as SqlClient from 'effect/sql/SqlClient'
import { TestClock } from 'effect/testing'

import { migrate } from '../src/Migrations'
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
	callbacks: [
		{
			name: 'onEvent',
			presentationVersion: 1,
			destination: { thread: 'thread-1' },
			supportedOperations: ['PresentOutcome', 'AddExternalLink'],
		},
	],
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
		yield* backend.prepareDelivery({
			mailboxKey: claim.mailboxKey,
			claimId: claim.claimId,
			prepared: preparation,
			callbackAccessTokens: [claim.accessToken],
		})
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

describe('sql store: layout', () => {
	it.effect('persists the callback token list in active JSON and omits it from retired batch JSON', ({ expect }) =>
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
			expect(active.delivery_json).toContain(claim.accessToken)
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
	it.effect('is idempotent and preserves a current prepared delivery', ({ expect }) =>
		Effect.gen(function* () {
			yield* emptyDatabase
			const process = yield* startProcess
			const claim = yield* inProcess(process)(handedOffDelivery('thread-1'))
			yield* inProcess(process)(migrate)
			yield* inProcess(process)(migrate)
			expect((yield* inProcess(process)(status(claim))).stage).toBe('ExternalWaiting')
		}).pipe(Effect.scoped),
	)
})
