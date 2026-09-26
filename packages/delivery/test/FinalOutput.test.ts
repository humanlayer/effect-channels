import { assert, it } from '@effect/vitest'
import { Context, Deferred, Effect, Fiber, Layer, Predicate, Queue, Ref, Schema } from 'effect'
import { TestClock } from 'effect/testing'

import { bind, DeliveryError, HandlerFailure } from '../src/Delivery'
import { DeliveryAdmin } from '../src/DeliveryAdmin'
import { DeliveryControl } from '../src/DeliveryControl'
import {
	DeliveryOutputError,
	DeliveryOutputReceipt,
	FINAL_MESSAGE_MARKDOWN_MAX_BYTES,
	FINAL_MESSAGE_MARKDOWN_MAX_LENGTH,
	type FinalMessageOperation,
} from '../src/DeliveryOperation'
import { DeliveryPolicy } from '../src/DeliveryPolicy'
import type { EventDefinition } from '../src/EventDefinition'
import { activeBatches } from '../src/Mailbox'
import { layerMailboxStoreServices } from '../src/MailboxServices'
import { MailboxStore } from '../src/MailboxStore'
import { layer as memoryLayer } from '../src/memory'
import { DeliveryTerminalCapacityExceeded, DeliveryTerminalRequestInvalid } from '../src/protocol'

const Event = Schema.Struct({ id: Schema.String, installation: Schema.String, resource: Schema.String })
const definition: EventDefinition<typeof Event, typeof Schema.String> = {
	name: 'test.final-output',
	version: '1',
	provider: 'test',
	event: Event,
	resource: Schema.String,
	identify: (event) => ({ eventId: event.id, installation: event.installation, resource: event.resource }),
	resourceKey: (resource) => resource,
}
const policy = DeliveryPolicy.make({
	mode: 'serial',
	maxPayloadBytes: 4096,
	maxEnvelopes: 16,
	maxOutcomes: 64,
	retentionMs: 1000,
	maxAttempts: 2,
	retryBaseMs: 100,
	retryMaxMs: 200,
	leaseMs: 1000,
	heartbeatMs: 100,
	conflictRetries: 8,
})

interface TestOutputService {
	readonly calls: Queue.Queue<FinalMessageOperation>
	readonly responses: Queue.Queue<Effect.Effect<DeliveryOutputReceipt, DeliveryOutputError>>
}
class TestOutput extends Context.Service<TestOutput, TestOutputService>()('test/FinalOutput') {}

const testOutputLayer = Layer.effect(
	TestOutput,
	Effect.gen(function* () {
		return TestOutput.of({
			calls: yield* Queue.unbounded<FinalMessageOperation>(),
			responses: yield* Queue.unbounded<Effect.Effect<DeliveryOutputReceipt, DeliveryOutputError>>(),
		})
	}),
)
const storage = memoryLayer({ maxMailboxes: 20 })
const memory = layerMailboxStoreServices.pipe(Layer.provide(storage))
const services = Layer.mergeAll(
	memory,
	DeliveryControl.layer.pipe(Layer.provide(memory)),
	DeliveryAdmin.layer.pipe(Layer.provide(memory)),
	testOutputLayer,
)

const makeDelivery = (namespace: string, starts?: Ref.Ref<number>) =>
	bind({
		namespace,
		handlerId: 'investigate',
		definition,
		policy,
		handler: (_event, context) =>
			(starts === undefined ? Effect.void : Ref.update(starts, (count) => count + 1)).pipe(
				Effect.andThen(context.handoff()),
			),
		deliverFinalMessage: (operation) =>
			Effect.gen(function* () {
				const output = yield* TestOutput
				yield* Queue.offer(output.calls, operation)
				return yield* Queue.take(output.responses).pipe(Effect.flatten)
			}),
	})

const admitHandoff = Effect.fn('test.final_output.admit_handoff')(function* (
	namespace: string,
	starts?: Ref.Ref<number>,
) {
	const delivery = makeDelivery(namespace, starts)
	const receipt = yield* delivery.admit({
		event: Event.make({ id: 'A', installation: 'one', resource: 'thread' }),
		organizationId: 'org-one',
	})
	yield* delivery.processMailbox(receipt)
	const snapshot = yield* (yield* MailboxStore).loadMailbox(receipt)
	assert(snapshot?.state.active?.deliveryId !== undefined)
	return { delivery, receipt, deliveryId: snapshot.state.active.deliveryId }
})

it.effect('accepts final Markdown before provider delivery and completes it after reconstruction', () =>
	Effect.gen(function* () {
		const starts = yield* Ref.make(0)
		const first = yield* admitHandoff('held-provider', starts)
		const control = yield* DeliveryControl
		const accepted = yield* control.finish({
			deliveryId: first.deliveryId,
			outcome: 'completed',
			markdown: 'Final **answer**',
		})
		assert.strictEqual(accepted.finalMessage?.status, 'pending')

		const output = yield* TestOutput
		const release = yield* Deferred.make<void>()
		yield* Queue.offer(
			output.responses,
			Deferred.await(release).pipe(
				Effect.as(DeliveryOutputReceipt.make({ providerReceipt: '{"message":"one"}' })),
			),
		)
		const reconstructed = makeDelivery('held-provider', starts)
		const processing = yield* reconstructed.processMailbox(first.receipt).pipe(Effect.forkChild)
		const call = yield* Queue.take(output.calls)
		assert.strictEqual(call.markdown, 'Final **answer**')
		const whileHeld = yield* (yield* DeliveryAdmin).inspect({ operationId: call.operationId })
		assert.strictEqual(whileHeld.state._tag, 'Delivering')
		assert.strictEqual(yield* Ref.get(starts), 1)
		yield* Deferred.succeed(release, undefined)
		yield* Fiber.join(processing)
		const delivered = yield* (yield* DeliveryAdmin).inspect({ operationId: call.operationId })
		assert.strictEqual(delivered.state._tag, 'Delivered')
		const finalState = yield* (yield* MailboxStore).loadMailbox(first.receipt)
		assert.strictEqual(activeBatches(finalState?.state ?? assert.fail('missing mailbox')).length, 0)
		assert.strictEqual(yield* Ref.get(starts), 1)
	}).pipe(Effect.provide(services)),
)

it.effect('retries transient output with the same operation and respects a provider deadline', () =>
	Effect.gen(function* () {
		const starts = yield* Ref.make(0)
		const test = yield* admitHandoff('retry-provider', starts)
		const accepted = yield* (yield* DeliveryControl).finish({
			deliveryId: test.deliveryId,
			outcome: 'failed',
			markdown: 'Unable to finish.',
		})
		assert(accepted.finalMessage !== undefined)
		const output = yield* TestOutput
		yield* Queue.offer(
			output.responses,
			Effect.fail(DeliveryOutputError.make({ retryable: true, retryAfterMs: 500, safeCode: 'rate_limited' })),
		)
		assert.strictEqual(yield* test.delivery.processMailbox(test.receipt), true)
		const firstCall = yield* Queue.take(output.calls)
		assert.strictEqual(
			(yield* (yield* DeliveryAdmin).inspect({ operationId: firstCall.operationId })).state._tag,
			'Pending',
		)
		yield* TestClock.adjust(499)
		assert.strictEqual(yield* test.delivery.processMailbox(test.receipt), false)
		yield* TestClock.adjust(1)
		yield* Queue.offer(
			output.responses,
			Effect.succeed(DeliveryOutputReceipt.make({ providerReceipt: '{"message":"retry"}' })),
		)
		assert.strictEqual(yield* test.delivery.processMailbox(test.receipt), true)
		const secondCall = yield* Queue.take(output.calls)
		assert.strictEqual(secondCall.operationId, firstCall.operationId)
		assert.strictEqual(yield* Ref.get(starts), 1)
	}).pipe(Effect.provide(services)),
)

it.effect('records an ambiguous interrupted attempt without rerunning the investigation', () =>
	Effect.gen(function* () {
		const starts = yield* Ref.make(0)
		const test = yield* admitHandoff('ambiguous-provider', starts)
		const accepted = yield* (yield* DeliveryControl).finish({
			deliveryId: test.deliveryId,
			outcome: 'completed',
			markdown: 'Possibly delivered.',
		})
		assert(accepted.finalMessage !== undefined)
		const output = yield* TestOutput
		yield* Queue.offer(output.responses, Effect.never)
		const first = yield* test.delivery.processMailbox(test.receipt).pipe(Effect.forkChild)
		yield* Queue.take(output.calls)
		yield* Fiber.interrupt(first)
		yield* TestClock.adjust(policy.leaseMs)
		yield* Queue.offer(
			output.responses,
			Effect.succeed(DeliveryOutputReceipt.make({ providerReceipt: '{"message":"reconciled"}' })),
		)
		assert.strictEqual(yield* makeDelivery('ambiguous-provider', starts).processMailbox(test.receipt), true)
		const repeated = yield* Queue.take(output.calls)
		const operation = yield* (yield* DeliveryAdmin).inspect({ operationId: repeated.operationId })
		assert.strictEqual(operation.state._tag, 'Delivered')
		assert.strictEqual(operation.state.hadAmbiguousAttempt, true)
		assert.strictEqual(yield* Ref.get(starts), 1)
	}).pipe(Effect.provide(services)),
)

it.effect('retains failed output for explicit admin redelivery after terminal outcome expiry', () =>
	Effect.gen(function* () {
		const test = yield* admitHandoff('admin-redelivery')
		const accepted = yield* (yield* DeliveryControl).finish({
			deliveryId: test.deliveryId,
			outcome: 'completed',
			markdown: 'Retained answer.',
		})
		assert(accepted.finalMessage !== undefined)
		const output = yield* TestOutput
		yield* Queue.offer(
			output.responses,
			Effect.fail(DeliveryOutputError.make({ retryable: false, safeCode: 'permission_denied' })),
		)
		yield* test.delivery.processMailbox(test.receipt)
		const failed = yield* (yield* DeliveryAdmin).inspect({ operationId: accepted.finalMessage.operationId })
		assert.strictEqual(failed.state._tag, 'DeliveryFailed')
		yield* TestClock.adjust(policy.retentionMs)
		const redelivery = yield* (yield* DeliveryAdmin).redeliver({ operationId: failed.operationId })
		assert.strictEqual(redelivery.state._tag, 'Pending')
		yield* Queue.offer(
			output.responses,
			Effect.succeed(DeliveryOutputReceipt.make({ providerReceipt: '{"message":"redelivered"}' })),
		)
		yield* makeDelivery('admin-redelivery').processMailbox(test.receipt)
		assert.strictEqual(
			(yield* (yield* DeliveryAdmin).inspect({ operationId: failed.operationId })).state._tag,
			'Delivered',
		)
	}).pipe(Effect.provide(services)),
)

it.effect('posts nothing without Markdown and conflicts on changed terminal Markdown', () =>
	Effect.gen(function* () {
		const without = yield* admitHandoff('without-markdown')
		const control = yield* DeliveryControl
		const accepted = yield* control.finish({ deliveryId: without.deliveryId, outcome: 'completed' })
		assert.strictEqual(accepted.finalMessage, undefined)
		yield* without.delivery.processMailbox(without.receipt)
		assert.strictEqual(yield* Queue.size((yield* TestOutput).calls), 0)

		const withMessage = yield* admitHandoff('changed-markdown')
		yield* control.finish({ deliveryId: withMessage.deliveryId, outcome: 'completed', markdown: 'one' })
		const replay = yield* control.finish({
			deliveryId: withMessage.deliveryId,
			outcome: 'completed',
			markdown: 'one',
		})
		assert.strictEqual(replay.status, 'already_recorded')
		const conflict = yield* Effect.flip(
			control.finish({ deliveryId: withMessage.deliveryId, outcome: 'completed', markdown: 'two' }),
		)
		assert.strictEqual(conflict._tag, 'DeliveryTerminalRequestConflict')
	}).pipe(Effect.provide(services)),
)

it.effect('keeps an early terminal local delivery active and does not advance queued work until output settles', () =>
	Effect.gen(function* () {
		const entered = yield* Deferred.make<void>()
		const release = yield* Deferred.make<void>()
		const cleaned = yield* Deferred.make<void>()
		const starts = yield* Queue.unbounded<string>()
		const control = yield* DeliveryControl
		const output = yield* TestOutput
		const delivery = bind({
			namespace: 'early-local-terminal',
			handlerId: 'investigate',
			definition,
			policy,
			handler: (event, context) =>
				Queue.offer(starts, event.id).pipe(
					Effect.andThen(
						event.id === 'A'
							? Effect.gen(function* () {
									yield* Effect.addFinalizer(() => Deferred.succeed(cleaned, undefined))
									yield* control
										.finish({
											deliveryId: context.deliveryId,
											outcome: 'completed',
											markdown: 'Answer before cleanup.',
										})
										.pipe(Effect.orDie)
									yield* Deferred.succeed(entered, undefined)
									yield* Deferred.await(release)
								})
							: Effect.void,
					),
				),
			deliverFinalMessage: (operation) =>
				Queue.offer(output.calls, operation).pipe(Effect.andThen(Queue.take(output.responses)), Effect.flatten),
		})
		const first = yield* delivery.admit({
			event: Event.make({ id: 'A', installation: 'one', resource: 'thread' }),
			organizationId: 'org-one',
		})
		const local = yield* delivery.processMailbox(first).pipe(Effect.forkChild)
		assert.strictEqual(yield* Queue.take(starts), 'A')
		yield* Deferred.await(entered)
		yield* delivery.admit({
			event: Event.make({ id: 'B', installation: 'one', resource: 'thread' }),
			organizationId: 'org-one',
		})
		yield* Deferred.succeed(release, undefined)
		yield* Fiber.join(local)
		assert.strictEqual(yield* Deferred.isDone(cleaned), true)
		const held = yield* (yield* MailboxStore).loadMailbox(first)
		assert.strictEqual(activeBatches(held?.state ?? assert.fail('missing mailbox')).length, 1)
		assert.strictEqual(held?.state.pending.length, 1)
		assert.strictEqual(yield* Queue.size(starts), 0)

		yield* Queue.offer(
			output.responses,
			Effect.succeed(DeliveryOutputReceipt.make({ providerReceipt: '{"message":"early"}' })),
		)
		yield* delivery.processMailbox(first)
		const settled = yield* (yield* MailboxStore).loadMailbox(first)
		assert.strictEqual(activeBatches(settled?.state ?? assert.fail('missing mailbox')).length, 0)
		assert.strictEqual(settled?.state.pending.length, 1)
	}).pipe(Effect.provide(services)),
)

it.effect('does not retire an early terminal delivery or advance its queue when the local handler fails', () =>
	Effect.gen(function* () {
		const starts = yield* Queue.unbounded<string>()
		const control = yield* DeliveryControl
		const output = yield* TestOutput
		const delivery = bind({
			namespace: 'early-local-failure',
			handlerId: 'investigate',
			definition,
			policy,
			handler: (event, context) =>
				Queue.offer(starts, event.id).pipe(
					Effect.andThen(
						event.id === 'A'
							? control
									.finish({
										deliveryId: context.deliveryId,
										outcome: 'failed',
										markdown: 'Failed after accepting output.',
									})
									.pipe(
										Effect.orDie,
										Effect.andThen(Effect.fail(HandlerFailure.make({ retryable: false }))),
									)
							: Effect.void,
					),
				),
			deliverFinalMessage: (operation) =>
				Queue.offer(output.calls, operation).pipe(Effect.andThen(Queue.take(output.responses)), Effect.flatten),
		})
		const first = yield* delivery.admit({
			event: Event.make({ id: 'A', installation: 'one', resource: 'thread' }),
			organizationId: 'org-one',
		})
		yield* delivery.admit({
			event: Event.make({ id: 'B', installation: 'one', resource: 'thread' }),
			organizationId: 'org-one',
		})
		yield* delivery.processMailbox(first)
		assert.strictEqual(yield* Queue.take(starts), 'A')
		assert.strictEqual(yield* Queue.size(starts), 0)
		const held = yield* (yield* MailboxStore).loadMailbox(first)
		assert.strictEqual(activeBatches(held?.state ?? assert.fail('missing mailbox')).length, 1)
		assert.strictEqual(held?.state.pending.length, 1)
	}).pipe(Effect.provide(services)),
)

it.effect('renews an in-flight output lease so a second runner cannot duplicate the provider write', () =>
	Effect.gen(function* () {
		const test = yield* admitHandoff('renew-output-lease')
		yield* (yield* DeliveryControl).finish({
			deliveryId: test.deliveryId,
			outcome: 'completed',
			markdown: 'One provider write.',
		})
		const output = yield* TestOutput
		const release = yield* Deferred.make<void>()
		yield* Queue.offer(
			output.responses,
			Deferred.await(release).pipe(
				Effect.as(DeliveryOutputReceipt.make({ providerReceipt: '{"message":"one"}' })),
			),
		)
		const first = yield* test.delivery.processMailbox(test.receipt).pipe(Effect.forkChild)
		yield* Queue.take(output.calls)
		yield* TestClock.adjust(policy.leaseMs * 3)
		assert.strictEqual(yield* makeDelivery('renew-output-lease').processMailbox(test.receipt), false)
		assert.strictEqual(yield* Queue.size(output.calls), 0)
		yield* Deferred.succeed(release, undefined)
		yield* Fiber.join(first)
		assert.strictEqual(yield* Queue.size(output.calls), 0)
	}).pipe(Effect.provide(services)),
)

it.effect('keeps output ownership through a delayed durable provider-result commit', () =>
	Effect.gen(function* () {
		const test = yield* admitHandoff('renew-output-through-commit')
		const accepted = yield* (yield* DeliveryControl).finish({
			deliveryId: test.deliveryId,
			outcome: 'completed',
			markdown: 'Commit one provider write.',
		})
		assert(accepted.finalMessage !== undefined)
		const output = yield* TestOutput
		yield* Queue.offer(
			output.responses,
			Effect.succeed(DeliveryOutputReceipt.make({ providerReceipt: '{"message":"committed"}' })),
		)
		const store = yield* MailboxStore
		const commitReached = yield* Deferred.make<void>()
		const releaseCommit = yield* Deferred.make<void>()
		const delayNextDelivered = yield* Ref.make(true)
		const delayedStore = Layer.succeed(
			MailboxStore,
			MailboxStore.of({
				loadMailbox: store.loadMailbox,
				commitMailbox: (input) =>
					Effect.gen(function* () {
						const delivered =
							input.nextState.version === 1 || input.nextState.version === 2
								? false
								: input.nextState.operations?.some((operation) =>
										Predicate.isTagged('Delivered')(operation.state),
									)
						if (delivered === true && (yield* Ref.getAndSet(delayNextDelivered, false))) {
							yield* Deferred.succeed(commitReached, undefined)
							yield* Deferred.await(releaseCommit)
						}
						return yield* store.commitMailbox(input)
					}),
			}),
		)
		const first = yield* test.delivery
			.processMailbox(test.receipt)
			.pipe(Effect.provide(delayedStore), Effect.forkChild)
		yield* Queue.take(output.calls)
		yield* Deferred.await(commitReached)
		yield* TestClock.adjust(policy.leaseMs * 3)
		assert.strictEqual(yield* makeDelivery('renew-output-through-commit').processMailbox(test.receipt), false)
		assert.strictEqual(yield* Queue.size(output.calls), 0)
		yield* Deferred.succeed(releaseCommit, undefined)
		yield* Fiber.join(first)
		assert.strictEqual(
			(yield* (yield* DeliveryAdmin).inspect({ operationId: accepted.finalMessage.operationId })).state._tag,
			'Delivered',
		)
		assert.strictEqual(yield* Queue.size(output.calls), 0)
	}).pipe(Effect.provide(services)),
)

it.effect('preserves cancellation when final output settles', () =>
	Effect.gen(function* () {
		const test = yield* admitHandoff('cancelled-final-output')
		yield* (yield* DeliveryControl).finish({
			deliveryId: test.deliveryId,
			outcome: 'completed',
			markdown: 'Output after cancellation.',
		})
		assert.strictEqual(
			yield* test.delivery.cancelActive({ ...test.receipt, controlId: 'cancel-final-output' }),
			true,
		)
		const output = yield* TestOutput
		yield* Queue.offer(
			output.responses,
			Effect.succeed(DeliveryOutputReceipt.make({ providerReceipt: '{"message":"cancelled"}' })),
		)
		yield* test.delivery.processMailbox(test.receipt)
		const snapshot = yield* (yield* MailboxStore).loadMailbox(test.receipt)
		assert.strictEqual(
			snapshot?.state.outcomes.find((outcome) => outcome.deliveryId === test.deliveryId)?.kind,
			'cancelled',
		)
	}).pipe(Effect.provide(services)),
)

it.effect('rejects terminal Markdown atomically when its durable output record exceeds mailbox capacity', () =>
	Effect.gen(function* () {
		const capacityPolicy = DeliveryPolicy.make({ ...policy, maxOutcomes: 1 })
		const delivery = bind({
			namespace: 'terminal-output-capacity',
			handlerId: 'investigate',
			definition,
			policy: capacityPolicy,
			handler: (_event, context) => context.handoff(),
			deliverFinalMessage: (operation) =>
				Effect.gen(function* () {
					const output = yield* TestOutput
					yield* Queue.offer(output.calls, operation)
					return yield* Queue.take(output.responses).pipe(Effect.flatten)
				}),
		})
		const receipt = yield* delivery.admit({
			event: Event.make({ id: 'A', installation: 'one', resource: 'thread' }),
			organizationId: 'org-one',
		})
		yield* delivery.processMailbox(receipt)
		const store = yield* MailboxStore
		const before = yield* store.loadMailbox(receipt)
		assert(before?.state.active?.deliveryId !== undefined)
		const error = yield* Effect.flip(
			(yield* DeliveryControl).finish({
				deliveryId: before.state.active.deliveryId,
				outcome: 'completed',
				markdown: 'No capacity for this operation.',
			}),
		)
		assert(Schema.is(DeliveryTerminalCapacityExceeded)(error))
		assert.deepStrictEqual(yield* store.loadMailbox(receipt), before)
		assert.strictEqual(yield* Queue.size((yield* TestOutput).calls), 0)
	}).pipe(Effect.provide(services)),
)

it.effect('rejects a mailbox owned by another registration before mutation or provider invocation', () =>
	Effect.gen(function* () {
		const test = yield* admitHandoff('right-registration')
		yield* (yield* DeliveryControl).finish({
			deliveryId: test.deliveryId,
			outcome: 'completed',
			markdown: 'Do not post this.',
		})
		const before = yield* (yield* MailboxStore).loadMailbox(test.receipt)
		const error = yield* Effect.flip(makeDelivery('wrong-registration').processMailbox(test.receipt))
		assert(Schema.is(DeliveryError)(error))
		assert.strictEqual(error.reason, 'definition')
		const after = yield* (yield* MailboxStore).loadMailbox(test.receipt)
		assert.deepStrictEqual(after, before)
		assert.strictEqual(yield* Queue.size((yield* TestOutput).calls), 0)
	}).pipe(Effect.provide(services)),
)

it.effect('accepts final Markdown at the explicit bound and rejects direct calls above it', () =>
	Effect.gen(function* () {
		const control = yield* DeliveryControl
		const exact = yield* admitHandoff('markdown-exact-bound')
		const accepted = yield* control.finish({
			deliveryId: exact.deliveryId,
			outcome: 'completed',
			markdown: 'x'.repeat(FINAL_MESSAGE_MARKDOWN_MAX_LENGTH),
		})
		assert.strictEqual(accepted.finalMessage?.status, 'pending')
		const over = yield* admitHandoff('markdown-over-bound')
		const error = yield* Effect.flip(
			control.finish({
				deliveryId: over.deliveryId,
				outcome: 'completed',
				markdown: '😀'.repeat(FINAL_MESSAGE_MARKDOWN_MAX_BYTES / 2),
			}),
		)
		assert(Schema.is(DeliveryTerminalRequestInvalid)(error))
		assert.strictEqual(error._tag, 'DeliveryTerminalRequestInvalid')
		assert.strictEqual(error.reason, 'markdown_too_large')
	}).pipe(Effect.provide(services)),
)
