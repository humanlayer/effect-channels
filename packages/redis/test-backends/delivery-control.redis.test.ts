/**
 * What only a real, shared Redis can show about the Redis store: several processes working on the
 * same keys, a process that stops in the middle of its work, a Redis that forgot its scripts, and
 * data written by an earlier release. Each "process" here is a store with a connection of its own.
 */
import * as NodeCrypto from '@effect/platform-node/NodeCrypto'
import { describe, it } from '@effect/vitest'
import {
	BatchId,
	ClaimFrozenBatch,
	ClaimWaitingEvents,
	CompleteDelivery,
	DELIVERY_RETENTION_MS,
	DeliveryAccessToken,
	DeliveryAdmission,
	DeliveryAdmissionBatch,
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
} from '@humanlayer/channels-delivery-next'
import { Array as Arr, Clock, Context, Effect, Fiber, Layer, Option, Queue, Redacted, Schema } from 'effect'
import { TestClock } from 'effect/testing'
import * as Redis from 'effect/unstable/persistence/Redis'

import { commitDeliverySlot, loadDeliverySlot } from '../src/DeliverySlot'
import {
	mailboxEventsKey,
	mailboxPendingKey,
	mailboxRetainedKey,
	mailboxStateKey,
	readyMailboxesKey,
} from '../src/Keys'
import { client, flushDatabase, storeOverClient } from './redis'

const leaseMs = 1_000

const admission = (eventId: string, resourceId = 'thread-1') =>
	DeliveryAdmission.make({
		namespace: 'redis',
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

type Store = MailboxDelivery | MailboxProcessingBackend | DeliveryControlBackend | Redis.Redis

/** One process: the store's services over a connection of its own. */
const startProcess = Layer.build(storeOverClient().pipe(Layer.provideMerge(Layer.fresh(client))))

/** Empty the shared database through a short-lived connection. */
const emptyDatabase = flushDatabase.pipe(Effect.provide(Layer.fresh(client)))

const inProcess =
	(process: Context.Context<Store>) =>
	<A, E>(effect: Effect.Effect<A, E, Store>) =>
		Effect.provideContext(effect, process)

const nextIdentity = (() => {
	let made = 0
	return () => {
		made += 1
		return {
			batchId: BatchId.make(`redis-batch-${made}`),
			accessToken: DeliveryAccessToken.make(`redis-token-${made}`),
		}
	}
})()

/** Claim everything waiting in a mailbox as a new batch. */
const claimWaiting = (mailboxKey: string) =>
	Effect.gen(function* () {
		return yield* (yield* MailboxProcessingBackend).claimMailbox(
			ClaimWaitingEvents.make({
				mailboxKey,
				upToSequence: Number.MAX_SAFE_INTEGER,
				leaseMs,
				...nextIdentity(),
			}),
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

/** Claim the mailbox's next due output and settle it as applied. Dies when none is due. */
const sendOutput = (mailboxKey: string) =>
	Effect.gen(function* () {
		const backend = yield* MailboxProcessingBackend
		const output = Option.getOrThrow(
			yield* backend.claimDeliveryOutput({
				mailboxKey,
				leaseMs,
				idempotencyKey: '00000000-0000-4000-8000-000000000001',
			}),
		)
		yield* backend.settleDeliveryOutput({
			mailboxKey,
			operationId: output.operationId,
			claimId: output.claimId,
			settlement: DeliveryOutputSettlement.cases.Applied.make({}),
			settledAt: yield* now,
		})
		return output
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
						namespace: 'redis',
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

/** Both pollers pass over the store at the same time, again and again, until a pass finds no work. */
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

describe('redis store: several processes', () => {
	it.effect('a write decided on a read another process has since changed is refused, and nothing is written', ({ expect }) =>
		Effect.gen(function* () {
			yield* emptyDatabase
			const first = yield* startProcess
			const second = yield* startProcess
			yield* inProcess(first)(MailboxDelivery.use((delivery) => delivery.deliver(admission('a'))))
			const stale = Option.getOrThrow(yield* inProcess(first)(loadDeliverySlot({ mailboxKey: keyOf('thread-1') })))

			const claim = Option.getOrThrow(yield* inProcess(second)(claimWaiting(keyOf('thread-1'))))
			const refused = yield* inProcess(first)(
				commitDeliverySlot({
					loaded: stale,
					provider: 'example',
					slot: { ...stale.slot, readyAt: null },
				}),
			).pipe(Effect.flip)
			expect(refused._tag).toBe('SlotChanged')
			const current = Option.getOrThrow(yield* inProcess(first)(loadDeliverySlot({ mailboxKey: keyOf('thread-1') })))
			expect(current.slot.active?.claimId).toBe(claim.claimId)
			expect(current.slot.readyAt).toBe(leaseMs)
		}).pipe(Effect.scoped),
	)

	it.effect('many claimers racing for one mailbox: exactly one takes the batch', ({ expect }) =>
		Effect.gen(function* () {
			yield* emptyDatabase
			const first = yield* startProcess
			const second = yield* startProcess
			yield* inProcess(first)(MailboxDelivery.use((delivery) => delivery.deliver(admission('a'))))
			const claims = yield* Effect.all(
				Arr.makeBy(12, (index) => inProcess(index % 2 === 0 ? first : second)(claimWaiting(keyOf('thread-1')))),
				{ concurrency: 'unbounded' },
			)
			expect(Arr.getSomes(claims)).toHaveLength(1)
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
				inProcess(first)(MailboxDelivery.use((delivery) => delivery.deliver(admission(`event-${thread}`, thread)))),
			)

			expect(yield* pollTogetherUntilQuiet(pollerA, pollerB)).toEqual({ claimed: 12, output: 0 })
			const deliveries = yield* Queue.takeAll(handedOff)
			expect(new Set(deliveries.map(({ deliveryId }) => deliveryId)).size).toBe(12)
			expect(deliveries).toHaveLength(12)

			yield* Effect.forEach(
				deliveries,
				({ deliveryId, accessToken }) =>
					inProcess(second)(
						DeliveryControlBackend.use((control) =>
							control.applyDeliveryMutation({
								reference: Option.getOrThrow(parseDeliveryId(deliveryId)),
								accessToken,
								mutation: CompleteDelivery.make({ markdown: 'done' }),
							}),
						),
					),
				{ concurrency: 'unbounded' },
			)
			expect(yield* pollTogetherUntilQuiet(pollerA, pollerB)).toEqual({ claimed: 0, output: 12 })
			const sent = yield* Queue.takeAll(attempts)
			expect(sent.map(({ attempt }) => attempt)).toEqual(Arr.makeBy(12, () => 1))
			expect(new Set(sent.map(({ deliveryId }) => deliveryId)).size).toBe(12)

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

			expect(yield* inProcess(second)(MailboxProcessingBackend.use((backend) => backend.findReadyMailboxes))).toEqual([
				RecoverableMailbox.make({ mailboxKey: keyOf('thread-1') }),
			])
			const taken = Option.getOrThrow(yield* inProcess(second)(claimFrozen(keyOf('thread-1'))))
			expect(taken.batchId).toBe(stale.batchId)
			expect(taken.attempt).toBe(2)
			expect(taken.claimId === stale.claimId).toBe(false)

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
			expect(yield* inProcess(first)(MailboxProcessingBackend.use((backend) => backend.findReadyMailboxes))).toEqual([])
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
				expect((yield* inProcess(first)(settle(stale)).pipe(Effect.flip))._tag).toBe('MailboxProcessingClaimLost')
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

describe('redis store: scripts', () => {
	it.effect('loads its scripts again after SCRIPT FLUSH, as after a Redis restart, and carries on', ({ expect }) =>
		Effect.gen(function* () {
			yield* emptyDatabase
			const process = yield* startProcess
			const claim = yield* inProcess(process)(handedOffDelivery('thread-1'))
			const redis = Context.get(process, Redis.Redis)
			yield* redis.send('SCRIPT', 'FLUSH')

			expect((yield* inProcess(process)(complete(claim))).status).toBe('accepted')
			yield* redis.send('SCRIPT', 'FLUSH')
			expect((yield* inProcess(process)(sendOutput(claim.mailboxKey))).operation._tag).toBe('PresentOutcome')
			yield* redis.send('SCRIPT', 'FLUSH')
			expect((yield* inProcess(process)(status(claim))).stage).toBe('Retired')
			yield* redis.send('SCRIPT', 'FLUSH')
			yield* inProcess(process)(MailboxDelivery.use((delivery) => delivery.deliver(admission('b'))))
			expect(Option.isSome(yield* inProcess(process)(claimWaiting(keyOf('thread-1'))))).toBe(true)
		}).pipe(Effect.scoped),
	)
})

const stateFieldRows = Schema.Array(Schema.NullOr(Schema.String))

describe('redis store: layout', () => {
	it.effect(
		'keeps the token out of the delivery JSON, and lets the finished deliveries expire with their retention',
		({ expect }) =>
			Effect.gen(function* () {
				yield* emptyDatabase
				const process = yield* startProcess
				const redis = Context.get(process, Redis.Redis)
				const claim = yield* inProcess(process)(handedOffDelivery('thread-1'))
				yield* inProcess(process)(complete(claim))
				const [delivery, stage] = yield* Schema.decodeUnknownEffect(stateFieldRows)(
					yield* redis.send('HMGET', mailboxStateKey(claim.mailboxKey), 'delivery', 'stage'),
				)
				expect(stage).toBe('Finishing')
				expect(delivery).not.toContain(claim.accessToken)
				expect(delivery).not.toContain('admissions')

				yield* inProcess(process)(sendOutput(claim.mailboxKey))
				expect((yield* inProcess(process)(status(claim))).stage).toBe('Retired')
				const ttl = yield* Schema.decodeUnknownEffect(Schema.Finite)(
					yield* redis.send('PTTL', mailboxRetainedKey(claim.mailboxKey)),
				)
				expect(ttl).toBeGreaterThan(DELIVERY_RETENTION_MS - 60_000)
				expect(ttl).toBeLessThanOrEqual(DELIVERY_RETENTION_MS)
				const [remaining] = yield* Schema.decodeUnknownEffect(stateFieldRows)(
					yield* redis.send('HMGET', mailboxStateKey(claim.mailboxKey), 'delivery'),
				)
				expect(remaining).toBeNull()
			}).pipe(Effect.scoped),
	)
})

const batchCodec = Schema.fromJsonString(DeliveryAdmissionBatch)
const admissionCodec = Schema.fromJsonString(DeliveryAdmission)
const preparedCodec = Schema.fromJsonString(PreparedDeliveryInvocation)

/**
 * A mailbox as the release before handoff wrote it: its state hash with a frozen batch and no stage
 * or delivery JSON, its waiting list, its accepted events, and its place in the ready set.
 */
const writeOldMailbox = (input: {
	readonly redis: Redis.Redis['Service']
	readonly resourceId: string
	readonly fields: ReadonlyArray<readonly [string, string]>
	readonly frozen: DeliveryAdmissionBatch
	readonly waiting: ReadonlyArray<DeliveryAdmission>
	readonly readyAt: number
}) =>
	Effect.gen(function* () {
		const mailboxKey = keyOf(input.resourceId)
		const batch = yield* Schema.encodeEffect(batchCodec)(input.frozen)
		const entries = yield* Effect.forEach(input.waiting, (waiting, index) =>
			Schema.encodeEffect(admissionCodec)(waiting).pipe(
				Effect.map((json) => `${input.frozen.length + index}|${index}|${json}`),
			),
		)
		yield* input.redis.send(
			'HSET',
			mailboxStateKey(mailboxKey),
			'provider',
			'example',
			'next_sequence',
			String(input.frozen.length + input.waiting.length),
			'claims_made',
			'1',
			'ready_at',
			String(input.readyAt),
			'batch',
			batch,
			...input.fields.flat(),
		)
		if (Arr.isReadonlyArrayNonEmpty(entries)) yield* input.redis.send('RPUSH', mailboxPendingKey(mailboxKey), ...entries)
		yield* input.redis.send(
			'SADD',
			mailboxEventsKey(mailboxKey),
			...[...input.frozen, ...input.waiting].map(({ eventId }) => eventId),
		)
		yield* input.redis.send('ZADD', readyMailboxesKey, String(input.readyAt), mailboxKey)
		return mailboxKey
	})

describe('redis store: data from earlier releases', () => {
	it.effect(
		'a running batch, a batch waiting to retry, and a batch saved before batches had IDs all carry on',
		({ expect }) =>
			Effect.gen(function* () {
				yield* emptyDatabase
				const process = yield* startProcess
				const redis = Context.get(process, Redis.Redis)
				const preparedJson = yield* Schema.encodeEffect(preparedCodec)(preparation)

				const running = yield* writeOldMailbox({
					redis,
					resourceId: 'running',
					frozen: [admission('e1', 'running')],
					waiting: [admission('e1-later', 'running')],
					readyAt: leaseMs,
					fields: [
						['status', 'active'],
						['claim_id', 'old-claim-1'],
						['attempt', '1'],
						['batch_id', 'b1'],
						['access_token', 'token1'],
						['prepared', preparedJson],
					],
				})
				const retrying = yield* writeOldMailbox({
					redis,
					resourceId: 'retrying',
					frozen: [admission('e2', 'retrying')],
					waiting: [],
					readyAt: 5_000,
					fields: [
						['status', 'retry'],
						['attempt', '1'],
						['batch_id', 'b2'],
						['access_token', 'token2'],
						['last_result', '{"_tag":"RetryableFailure","safeCode":"temporary"}'],
					],
				})
				const beforeBatches = yield* writeOldMailbox({
					redis,
					resourceId: 'before-batches',
					frozen: [admission('e0', 'before-batches')],
					waiting: [],
					readyAt: leaseMs,
					fields: [
						['status', 'active'],
						['claim_id', 'old-claim-0'],
						['attempt', '1'],
					],
				})

				yield* inProcess(process)(
					Effect.gen(function* () {
						const backend = yield* MailboxProcessingBackend
						expect(yield* backend.findReadyMailboxes).toEqual([])
						yield* TestClock.adjust(leaseMs)
						expect((yield* backend.findReadyMailboxes).map(({ mailboxKey }) => mailboxKey).toSorted()).toEqual(
							[running, beforeBatches].toSorted(),
						)

						/** The old process is gone: the batch is taken over with its ID, token and preparation, and the old claim is refused. */
						const resumed = Option.getOrThrow(yield* claimFrozen(running))
						const lost = yield* backend
							.renewClaim({ mailboxKey: running, claimId: 'old-claim-1', leaseMs })
							.pipe(Effect.flip)
						expect(lost._tag).toBe('MailboxProcessingClaimLost')
						expect(resumed.attempt).toBe(2)
						expect(resumed.batchId).toBe('b1')
						expect(resumed.accessToken).toBe('token1')
						expect(resumed.prepared).toEqual(preparation)
						expect(resumed.admissions.map(({ eventId }) => eventId)).toEqual(['e1'])
						yield* backend.handOffDelivery({
							mailboxKey: running,
							claimId: resumed.claimId,
							handedOffAt: yield* now,
							links: [],
						})
						yield* recordCompleted(resumed)
						expect((yield* status(resumed)).stage).toBe('ExternalWaiting')
						expect((yield* complete(resumed)).status).toBe('accepted')
						yield* sendOutput(running)
						expect((yield* status(resumed)).stage).toBe('Retired')
						const later = Option.getOrThrow(yield* claimWaiting(running))
						expect(later.admissions.map(({ eventId }) => eventId)).toEqual(['e1-later'])
						yield* recordCompleted(later)

						/** A batch saved before IDs gets one ID and token, kept from then on. */
						const legacy = Option.getOrThrow(yield* claimFrozen(beforeBatches))
						expect(legacy.attempt).toBe(2)
						expect(legacy.batchId).toMatch(/^legacy-[0-9a-f]{32}$/)
						expect(legacy.accessToken).toMatch(/^[0-9a-f]{40}$/)
						expect(legacy.admissions.map(({ eventId }) => eventId)).toEqual(['e0'])
						yield* TestClock.adjust(leaseMs)
						const legacyAgain = Option.getOrThrow(yield* claimFrozen(beforeBatches))
						expect(legacyAgain.batchId).toBe(legacy.batchId)
						expect(legacyAgain.accessToken).toBe(legacy.accessToken)
						yield* recordCompleted(legacyAgain)

						/** A batch waiting to retry runs again when its time comes, with no preparation yet. */
						yield* TestClock.adjust(5_000)
						expect(yield* backend.findReadyMailboxes).toEqual([RecoverableMailbox.make({ mailboxKey: retrying })])
						const retry = Option.getOrThrow(yield* claimFrozen(retrying))
						expect(retry.batchId).toBe('b2')
						expect(retry.attempt).toBe(2)
						expect(retry.prepared).toBeUndefined()
						yield* recordCompleted(retry)
						expect(yield* backend.findReadyMailboxes).toEqual([])

						/** An old idle mailbox takes new events as before. */
						yield* (yield* MailboxDelivery).deliver(admission('fresh', 'retrying'))
						expect((yield* backend.findReadyMailboxes).filter(Schema.is(WaitingMailbox))).toHaveLength(1)
					}),
				)
			}).pipe(Effect.scoped),
	)

	it.effect('hands back a claimed admission exactly as delivered, with no cjson round trip turning [] into {}', ({ expect }) =>
		Effect.gen(function* () {
			yield* emptyDatabase
			const process = yield* startProcess
			const odd = DeliveryAdmission.make({
				namespace: 'redis',
				provider: 'example',
				installationId: 'installation',
				resourceId: 'thread|with|pipes',
				eventId: 'event|1',
				payload: { empty: [], nested: { list: [[]], text: 'a|b', big: 9007199254740991, ratio: 0.1 } },
				interrupt: true,
			})
			const { mailboxKey } = yield* inProcess(process)(MailboxDelivery.use((delivery) => delivery.deliver(odd)))
			const claim = Option.getOrThrow(
				yield* inProcess(process)(
					MailboxProcessingBackend.use((backend) =>
						backend.claimMailbox(
							ClaimWaitingEvents.make({ mailboxKey, upToSequence: 0, leaseMs, ...nextIdentity() }),
						),
					),
				),
			)
			expect(claim.admissions).toEqual([odd])
		}).pipe(Effect.scoped),
	)
})
