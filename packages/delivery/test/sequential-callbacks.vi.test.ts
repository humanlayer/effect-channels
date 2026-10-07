import * as NodeCrypto from '@effect/platform-node/NodeCrypto'
import { describe, it } from '@effect/vitest'
import { Effect, Option, Queue, Redacted } from 'effect'

import {
	BatchId,
	CompleteDelivery,
	DeliveryAccessToken,
	FailDelivery,
	DeliveryOutcome,
	DeliveryOutputSettlement,
	DeliveryTerminal,
	MailboxBackendMemory,
	MailboxProcessingBackend,
	PreparedDeliveryInvocation,
	ProviderEventDispatcher,
	ProviderEventHandled,
	applyDeliverySlotMutation,
	claimDeliveryOutput,
	claimFrozenBatch,
	emptyDeliverySlot,
	handOffDeliverySlot,
	makeDeliveryId,
	parseDeliveryId,
	prepareDeliverySlot,
	processClaim,
	readDeliverySlotStatus,
	recordDeliveryAttempt,
	settleDeliveryOutput,
	startDeliveryBatch,
	toClaimedDeliveryOutput,
	type DeliveryContext,
	type DeliveryMutation,
	type DeliverySlot,
} from '../src'
import { claimAll, claimFrozen, deliver, event, findWaiting, mailboxKey } from './backend-contract'

const tokens = ['first-token', 'second-token', 'third-token'] as const
const callback = (name: string) => ({
	name,
	presentationVersion: 1,
	destination: { name },
	supportedOperations: ['PresentOutcome'] as const,
})
const prepared = PreparedDeliveryInvocation.make({
	callbacks: [callback('inspect'), { ...callback('implement'), supportedOperations: [] }, callback('review')],
})

const initialSlot = () => {
	const started = startDeliveryBatch(emptyDeliverySlot, {
		batchId: BatchId.make('sequence'),
		accessToken: DeliveryAccessToken.make(tokens[0]),
		admissions: [event('first')],
		claimId: 'claim-0',
		leaseMs: 100,
		now: 0,
	})
	if (started === null) throw new Error('expected idle slot')
	return started.slot
}

const prepare = (slot: DeliverySlot) =>
	prepareDeliverySlot(slot, {
		claimId: 'claim-0',
		prepared,
		callbackAccessTokens: tokens,
	}).pipe(Effect.map(({ slot: next }) => next))

const finish = (slot: DeliverySlot, succeeded = true, claimId = 'claim-0') =>
	recordDeliveryAttempt(slot, {
		claimId,
		succeeded,
		retryAfterMs: null,
		now: 10,
		hasWaiting: true,
	})

const reference = (callbackIndex: number) =>
	Option.getOrThrow(
		parseDeliveryId(
			makeDeliveryId({
				mailboxKey,
				batchId: BatchId.make('sequence'),
				callbackIndex,
			}),
		),
	)

const status = (slot: DeliverySlot, callbackIndex = 0, accessToken: string = tokens[callbackIndex] ?? '') =>
	readDeliverySlotStatus(slot, { reference: reference(callbackIndex), accessToken, now: 10 })

const remoteResult = (slot: DeliverySlot, outcome: DeliveryOutcome = DeliveryOutcome.cases.Completed.make({})) =>
	applyDeliverySlotMutation(slot, {
		reference: reference(0),
		accessToken: tokens[0],
		now: 5,
		hasWaiting: true,
		mutation: DeliveryOutcome.match(outcome, {
			Completed: (): DeliveryMutation => CompleteDelivery.make({}),
			Failed: () => FailDelivery.make({}),
			AwaitingInput: () => CompleteDelivery.make({ awaitingInput: {} }),
		}),
	}).pipe(Effect.map(({ slot: next }) => next))

const handoff = (slot: DeliverySlot) =>
	handOffDeliverySlot(slot, {
		claimId: 'claim-0',
		handedOffAt: 1,
		links: [],
		failAfterMs: 100,
	})

const send = (slot: DeliverySlot, failed = false) =>
	Effect.gen(function* () {
		const output = claimDeliveryOutput(slot, {
			claimId: 'output-0',
			leaseMs: 100,
			now: 10,
			idempotencyKey: 'output-key',
		})
		if (output.claimed === null) return yield* Effect.die(new Error('expected output'))
		return yield* settleDeliveryOutput(output.slot, {
			operationId: output.claimed.operation.operationId,
			claimId: 'output-0',
			now: 10,
			hasWaiting: true,
			settlement: failed
				? DeliveryOutputSettlement.cases.Failed.make({ safeCode: 'unavailable' })
				: DeliveryOutputSettlement.cases.Applied.make({}),
		})
	})

describe('sequential callbacks', () => {
	it.effect('requires one distinct access token per step with the initial token first', ({ expect }) =>
		Effect.gen(function* () {
			for (const callbackAccessTokens of [
				['first-token'],
				['wrong-token', 'second-token', 'third-token'],
				['first-token', 'first-token', 'third-token'],
			] as const) {
				const failure = yield* prepareDeliverySlot(initialSlot(), {
					claimId: 'claim-0',
					prepared,
					callbackAccessTokens,
				}).pipe(Effect.flip)
				expect(failure._tag).toBe('PreparationMismatch')
			}
		}),
	)
	it.effect(
		'checkpoints ordered steps, retries only the current step, and isolates retained identities',
		({ expect }) =>
			Effect.gen(function* () {
				let slot = yield* prepare(initialSlot())
				const replay = yield* prepareDeliverySlot(slot, {
					claimId: 'claim-0',
					prepared,
					callbackAccessTokens: ['candidate'],
				})
				expect(replay.slot.active?.callbackAccessTokens).toEqual(tokens)
				slot = yield* finish(slot)
				expect(slot.active).toMatchObject({
					callbackIndex: 1,
					accessToken: tokens[1],
					stage: 'Retry',
					attempt: 0,
					claimId: null,
				})
				expect(slot.readyAt).toBe(10)
				expect((yield* status(slot)).stage).toBe('Retired')
				expect((yield* status(slot, 1)).supportedOperations).toEqual([])
				const unsupported = yield* applyDeliverySlotMutation(slot, {
					reference: reference(1),
					accessToken: tokens[1],
					now: 10,
					hasWaiting: true,
					mutation: CompleteDelivery.make({}),
				}).pipe(Effect.flip)
				expect(unsupported._tag).toBe('DeliveryOperationUnsupported')
				expect((yield* status(slot, 1, tokens[0]).pipe(Effect.flip))._tag).toBe('DeliveryNotFound')
				expect((yield* status(slot, 0, tokens[1]).pipe(Effect.flip))._tag).toBe('DeliveryNotFound')
				const oldReplay = yield* applyDeliverySlotMutation(slot, {
					reference: reference(0),
					accessToken: tokens[0],
					now: 11,
					hasWaiting: false,
					mutation: CompleteDelivery.make({}),
				}).pipe(Effect.flip)
				expect(oldReplay._tag).toBe('DeliveryClosed')
				const claimed = claimFrozenBatch(slot, { claimId: 'claim-1', leaseMs: 100, now: 10, hasWaiting: true })
				expect(claimed.claimed).toMatchObject({ callbackIndex: 1, attempt: 1 })
				slot = yield* recordDeliveryAttempt(claimed.slot, {
					claimId: 'claim-1',
					succeeded: false,
					retryAfterMs: 20,
					now: 10,
					hasWaiting: true,
				})
				expect(
					claimFrozenBatch(slot, { claimId: 'early', leaseMs: 100, now: 29, hasWaiting: true }).claimed,
				).toBeNull()
				const retry = claimFrozenBatch(slot, { claimId: 'retry-1', leaseMs: 100, now: 30, hasWaiting: true })
				expect(retry.claimed).toMatchObject({ callbackIndex: 1, attempt: 2 })
				slot = yield* finish(retry.slot, true, 'retry-1')
				expect(slot.active).toMatchObject({ callbackIndex: 2, accessToken: tokens[2], attempt: 0 })
				expect(slot.retained.map(({ callbackIndex }) => callbackIndex)).toEqual([0, 1])
				const last = claimFrozenBatch(slot, { claimId: 'claim-2', leaseMs: 100, now: 30, hasWaiting: true })
				slot = yield* finish(last.slot, true, 'claim-2')
				expect(slot.active).toBeNull()
				expect(slot.retained.map(({ callbackIndex }) => callbackIndex)).toEqual([0, 1, 2])
			}),
	)

	it.effect(
		'waits for handoff result, callback cleanup, and output before rotating and clears per-step state',
		({ expect }) =>
			Effect.gen(function* () {
				let slot = yield* handoff(yield* prepare(initialSlot()))
				slot = yield* remoteResult(slot)
				expect(slot.active?.stage).toBe('ExternalCleaning')
				expect(
					claimDeliveryOutput(slot, { claimId: 'too-early', leaseMs: 100, now: 10, idempotencyKey: 'early' })
						.claimed,
				).toBeNull()
				slot = yield* finish(slot)
				expect(slot.active).toMatchObject({ callbackIndex: 0, stage: 'Finishing', continuationAllowed: true })
				const output = claimDeliveryOutput(slot, {
					claimId: 'projection',
					leaseMs: 100,
					now: 10,
					idempotencyKey: 'projection-key',
				})
				if (output.claimed === null) return yield* Effect.die(new Error('expected output'))
				const projected = toClaimedDeliveryOutput({ mailboxKey, claimId: 'projection', ...output.claimed })
				expect(projected.callbackIndex).toBe(0)
				expect(projected.prepared).toEqual(prepared.callbacks[0])
				expect(
					claimFrozenBatch(slot, { claimId: 'too-early', leaseMs: 100, now: 10, hasWaiting: true }).claimed,
				).toBeNull()
				slot = yield* send(slot)
				expect(slot.active).toMatchObject({
					callbackIndex: 1,
					stage: 'Retry',
					attempt: 0,
					links: [],
					operations: [],
				})
				for (const key of [
					'terminal',
					'handedOffAt',
					'failAfterMs',
					'failAt',
					'plan',
					'continuationAllowed',
					'interruptRequestedAt',
				]) {
					expect(slot.active).not.toHaveProperty(key)
				}
				expect((yield* status(slot)).outcome).toEqual(DeliveryOutcome.cases.Completed.make({}))
				const repeated = yield* applyDeliverySlotMutation(slot, {
					reference: reference(0),
					accessToken: tokens[0],
					now: 15,
					hasWaiting: true,
					mutation: CompleteDelivery.make({}),
				})
				expect(repeated.receipt.status).toBe('already_recorded')
				expect(repeated.slot).toEqual(slot)
			}),
	)

	it.effect('waits for a remote result after successful callback return', ({ expect }) =>
		Effect.gen(function* () {
			let slot = yield* finish(yield* handoff(yield* prepare(initialSlot())))
			expect(slot.active).toMatchObject({ callbackIndex: 0, stage: 'ExternalWaiting' })
			expect(slot.readyAt).toBe(101)
			slot = yield* remoteResult(slot)
			expect(slot.active?.stage).toBe('Finishing')
			slot = yield* send(slot)
			expect(slot.active?.callbackIndex).toBe(1)
		}),
	)

	it.effect(
		'does not advance failed callbacks, failed output, failed or awaiting-input remote results, or uncertain cleanup recovery',
		({ expect }) =>
			Effect.gen(function* () {
				expect((yield* finish(yield* prepare(initialSlot()), false)).active).toBeNull()
				let slot = yield* finish(yield* remoteResult(yield* handoff(yield* prepare(initialSlot()))))
				expect((yield* send(slot, true)).active).toBeNull()
				const failedCleanup = yield* finish(
					yield* remoteResult(yield* handoff(yield* prepare(initialSlot()))),
					false,
				)
				expect((yield* send(failedCleanup)).active).toBeNull()
				for (const outcome of [
					DeliveryOutcome.cases.Failed.make({}),
					DeliveryOutcome.cases.AwaitingInput.make({}),
				]) {
					slot = yield* finish(yield* remoteResult(yield* handoff(yield* prepare(initialSlot())), outcome))
					expect((yield* send(slot)).active).toBeNull()
				}
				slot = yield* remoteResult(yield* handoff(yield* prepare(initialSlot())))
				const recovery = claimFrozenBatch(slot, {
					claimId: 'recovered',
					leaseMs: 100,
					now: 100,
					hasWaiting: true,
				})
				expect(recovery.claimed).toBeNull()
				expect((yield* send(recovery.slot)).active).toBeNull()
			}),
	)

	it.effect('times out remote work without advancing', ({ expect }) =>
		Effect.gen(function* () {
			const waiting = yield* finish(yield* handoff(yield* prepare(initialSlot())))
			const output = claimDeliveryOutput(waiting, {
				claimId: 'timeout',
				leaseMs: 100,
				now: 101,
				idempotencyKey: 'timeout-key',
			})
			expect(output.claimed?.active.terminal).toEqual(
				DeliveryTerminal.make({
					outcome: DeliveryOutcome.cases.Failed.make({ reason: 'TimedOut' }),
					markdown: 'The remote worker stopped responding, so this was ended.',
				}),
			)
			const slot = yield* settleDeliveryOutput(output.slot, {
				operationId: 'outcome',
				claimId: 'timeout',
				now: 101,
				hasWaiting: true,
				settlement: DeliveryOutputSettlement.cases.Applied.make({}),
			})
			expect(slot.active).toBeNull()
		}),
	)

	it.effect(
		'generates and persists a unique random token for each remaining callback and uses indexed execution IDs',
		({ expect }) =>
			Effect.gen(function* () {
				const contexts = yield* Queue.unbounded<{ readonly index: number; readonly context: DeliveryContext }>()
				const dispatcher = ProviderEventDispatcher.of({
					process: (_admissions, execution) =>
						Effect.gen(function* () {
							yield* execution.prepare(prepared)
							yield* Queue.offer(contexts, { index: execution.callbackIndex, context: execution.context })
							return ProviderEventHandled.make({})
						}).pipe(Effect.orDie),
				})
				yield* deliver('first')
				let claim = yield* claimAll(yield* findWaiting)
				const initialToken = claim.accessToken
				const seenTokens = new Set<string>()
				const seenIds = new Set<string>()
				for (const index of [0, 1, 2]) {
					expect(claim.callbackIndex).toBe(index)
					yield* processClaim({ claim, maxAttempts: 5, leaseMs: 100 }).pipe(
						Effect.provideService(ProviderEventDispatcher, dispatcher),
					)
					const recorded = yield* Queue.take(contexts)
					expect(recorded.index).toBe(index)
					expect(recorded.context.deliveryId).toBe(
						makeDeliveryId({ mailboxKey, batchId: claim.batchId, callbackIndex: index }),
					)
					const token = Redacted.value(recorded.context.accessToken)
					expect(token).toBe(claim.accessToken)
					if (index === 0) expect(token).toBe(initialToken)
					else expect(token).toMatch(/^[A-Za-z0-9_-]{43}$/)
					seenTokens.add(token)
					seenIds.add(recorded.context.deliveryId)
					if (index < 2) claim = Option.getOrThrow(yield* claimFrozen)
				}
				expect(seenTokens.size).toBe(3)
				expect(seenIds.size).toBe(3)
				expect(yield* (yield* MailboxProcessingBackend).findReadyMailboxes).toEqual([])
			}).pipe(Effect.provide(MailboxBackendMemory), Effect.provide(NodeCrypto.layer)),
	)
})
