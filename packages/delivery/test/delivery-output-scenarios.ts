/**
 * Output processing over a store: how mailbox processing sends a delivery's saved output, retries it,
 * and gives up, apart from the callback. Every store that supports handoff runs these scenarios:
 * memory here, Postgres and Redis in their backend suites.
 */
import * as NodeCrypto from '@effect/platform-node/NodeCrypto'
import { it } from '@effect/vitest'
import { Effect, Layer, Option, Queue, Redacted, Ref } from 'effect'
import { TestClock } from 'effect/testing'

import {
	CompleteDelivery,
	DeliveryAdmission,
	DeliveryControlBackend,
	DeliveryOutputApplied,
	DeliveryOutputFailed,
	ExternalLink,
	MailboxDelivery,
	MailboxProcessing,
	type MailboxProcessingBackend,
	MailboxProcessingLive,
	PreparedDeliveryInvocation,
	ProviderEventDispatcher,
	ProviderEventHandled,
	ProviderOutputDispatcher,
	ProviderOutputDispatcherLive,
	QueueDeliveryMode,
	parseDeliveryId,
	type DeliveryId,
	type ProviderOutputAttempt,
	type ProviderOutputProcessor,
} from '../src'

const admission = DeliveryAdmission.make({
	namespace: 'test',
	provider: 'example',
	installationId: 'installation',
	resourceId: 'thread-1',
	eventId: 'a',
	payload: null,
})

/** Every batch prepares, unless told not to, then hands itself off and reports its delivery. */
const handingOffDispatcher = (input: {
	readonly deliveries: Queue.Queue<{ readonly deliveryId: DeliveryId; readonly accessToken: string }>
	readonly callbacks: Ref.Ref<number>
	readonly prepare: boolean
}) =>
	Layer.succeed(
		ProviderEventDispatcher,
		ProviderEventDispatcher.of({
			process: (_admissions, execution) =>
				Effect.gen(function* () {
					yield* Ref.update(input.callbacks, (count) => count + 1)
					if (input.prepare) {
						yield* execution.prepare(
							PreparedDeliveryInvocation.make({
								callback: 'onEvent',
								presentationVersion: 1,
								destination: { thread: 'thread-1' },
								supportedOperations: ['PresentOutcome'],
							}),
						)
					}
					/** An unprepared delivery hands off with a link, the one output it can still owe. */
					yield* execution.context.handoff(
						input.prepare
							? undefined
							: { links: [ExternalLink.make({ label: 'Run', url: 'https://example.com/run/1' })] },
					)
					yield* Queue.offer(input.deliveries, {
						deliveryId: execution.deliveryId,
						accessToken: Redacted.value(execution.context.accessToken),
					})
					return ProviderEventHandled.make({})
				}).pipe(Effect.orDie),
		}),
	)

/** An output processor that records attempts and answers each with the next scripted result. */
const scriptedProcessor = (
	attempts: Queue.Queue<ProviderOutputAttempt>,
	script: Ref.Ref<ReadonlyArray<'applied' | 'retryable' | 'permanent'>>,
): ProviderOutputProcessor => ({
	namespace: 'test',
	providerName: 'example',
	process: (attempt) =>
		Effect.gen(function* () {
			yield* Queue.offer(attempts, attempt)
			const next = yield* Ref.modify(script, ([head, ...rest]) => [head ?? 'applied', rest])
			if (next === 'applied') return DeliveryOutputApplied.make({})
			return yield* new DeliveryOutputFailed({
				provider: 'example',
				retryable: next === 'retryable',
				safeCode: next === 'retryable' ? 'example_unavailable' : 'example_refused',
			})
		}),
})

type OutputStore = MailboxDelivery | MailboxProcessingBackend | DeliveryControlBackend

const setup = <E>(options: {
	readonly store: Layer.Layer<OutputStore, E>
	readonly prepare?: boolean
	readonly output: (attempts: Queue.Queue<ProviderOutputAttempt>) => Layer.Layer<ProviderOutputDispatcher>
	readonly script?: ReadonlyArray<'applied' | 'retryable' | 'permanent'>
}) =>
	Effect.gen(function* () {
		const deliveries = yield* Queue.unbounded<{ readonly deliveryId: DeliveryId; readonly accessToken: string }>()
		const attempts = yield* Queue.unbounded<ProviderOutputAttempt>()
		const callbacks = yield* Ref.make(0)
		const layer = MailboxProcessingLive({
			concurrency: 1,
			leaseMs: 30_000,
			outputMaxAttempts: 3,
			deliveryModeFor: () => QueueDeliveryMode.make({}),
			polling: 'disabled',
		}).pipe(
			Layer.provide(handingOffDispatcher({ deliveries, callbacks, prepare: options.prepare ?? true })),
			Layer.provide(options.output(attempts)),
			Layer.provide(NodeCrypto.layer),
			Layer.provideMerge(options.store),
		)
		return { deliveries, attempts, callbacks, layer }
	})

/** Deliver one event, run its callback, and complete the delivery it hands off. */
const handOffAndComplete = (
	deliveries: Queue.Queue<{ readonly deliveryId: DeliveryId; readonly accessToken: string }>,
) =>
	Effect.gen(function* () {
		yield* (yield* MailboxDelivery).deliver(admission)
		yield* (yield* MailboxProcessing).processReady
		const delivery = yield* Queue.take(deliveries)
		const reference = Option.getOrThrow(parseDeliveryId(delivery.deliveryId))
		const control = yield* DeliveryControlBackend
		yield* control.applyDeliveryMutation({
			reference,
			accessToken: delivery.accessToken,
			mutation: CompleteDelivery.make({ markdown: 'done' }),
		})
		const status = control.readDeliveryStatus({ reference, accessToken: delivery.accessToken })
		return { status }
	})

const processReady = Effect.gen(function* () {
	return yield* (yield* MailboxProcessing).processReady
})

export const deliveryOutputScenarios = <E>(storeName: string, makeEmptyStore: () => Layer.Layer<OutputStore, E>) => {
	it.effect(
		`${storeName}: retries a retryable failure after a growing wait, then gives up after the last attempt`,
		({ expect }) =>
			Effect.gen(function* () {
				const script = yield* Ref.make<ReadonlyArray<'applied' | 'retryable' | 'permanent'>>([
					'retryable',
					'retryable',
					'retryable',
				])
				const { deliveries, attempts, callbacks, layer } = yield* setup({
					store: makeEmptyStore(),
					output: (attempts) => ProviderOutputDispatcherLive([scriptedProcessor(attempts, script)]),
				})
				yield* Effect.gen(function* () {
					const { status } = yield* handOffAndComplete(deliveries)
					expect((yield* processReady).output).toBe(1)
					expect((yield* Queue.take(attempts)).attempt).toBe(1)
					expect((yield* processReady).output).toBe(0)
					yield* TestClock.adjust(1_000)
					expect((yield* processReady).output).toBe(1)
					expect((yield* Queue.take(attempts)).attempt).toBe(2)
					yield* TestClock.adjust(999)
					expect((yield* processReady).output).toBe(0)
					yield* TestClock.adjust(1_001)
					yield* processReady
					expect((yield* Queue.take(attempts)).attempt).toBe(3)
					const retired = yield* status
					expect(retired.stage).toBe('Retired')
					expect(retired.outcome?._tag).toBe('Completed')
					expect(retired.output).toEqual([
						{
							operationId: 'outcome',
							kind: 'PresentOutcome',
							state: 'Failed',
							attempts: 3,
							hadAmbiguousAttempt: false,
						},
					])
					expect(yield* Ref.get(callbacks)).toBe(1)
				}).pipe(Effect.provide(layer))
			}),
	)

	it.effect(`${storeName}: gives up at once on a failure that is not retryable`, ({ expect }) =>
		Effect.gen(function* () {
			const script = yield* Ref.make<ReadonlyArray<'applied' | 'retryable' | 'permanent'>>(['permanent'])
			const { deliveries, attempts, layer } = yield* setup({
				store: makeEmptyStore(),
				output: (attempts) => ProviderOutputDispatcherLive([scriptedProcessor(attempts, script)]),
			})
			yield* Effect.gen(function* () {
				const { status } = yield* handOffAndComplete(deliveries)
				yield* processReady
				yield* Queue.take(attempts)
				expect((yield* status).output[0]?.state).toBe('Failed')
				expect((yield* status).stage).toBe('Retired')
			}).pipe(Effect.provide(layer))
		}),
	)

	it.effect(
		`${storeName}: fails the output of a provider that sends none, and still retires the delivery`,
		({ expect }) =>
			Effect.gen(function* () {
				const { deliveries, layer } = yield* setup({
					store: makeEmptyStore(),
					output: () => ProviderOutputDispatcherLive([]),
				})
				yield* Effect.gen(function* () {
					const { status } = yield* handOffAndComplete(deliveries)
					yield* processReady
					expect((yield* status).stage).toBe('Retired')
					expect((yield* status).output[0]?.state).toBe('Failed')
				}).pipe(Effect.provide(layer))
			}),
	)

	it.effect(
		`${storeName}: a delivery that was never prepared takes no result, and fails its output without calling the provider`,
		({ expect }) =>
			Effect.gen(function* () {
				const script = yield* Ref.make<ReadonlyArray<'applied' | 'retryable' | 'permanent'>>([])
				const { deliveries, attempts, layer } = yield* setup({
					store: makeEmptyStore(),
					prepare: false,
					output: (attempts) => ProviderOutputDispatcherLive([scriptedProcessor(attempts, script)]),
				})
				yield* Effect.gen(function* () {
					yield* (yield* MailboxDelivery).deliver(admission)
					yield* processReady
					const delivery = yield* Queue.take(deliveries)
					const reference = Option.getOrThrow(parseDeliveryId(delivery.deliveryId))
					const control = yield* DeliveryControlBackend
					const refused = yield* control
						.applyDeliveryMutation({
							reference,
							accessToken: delivery.accessToken,
							mutation: CompleteDelivery.make({}),
						})
						.pipe(Effect.flip)
					expect(refused).toMatchObject({ _tag: 'DeliveryOperationUnsupported', operation: 'PresentOutcome' })
					yield* processReady
					const status = yield* control.readDeliveryStatus({ reference, accessToken: delivery.accessToken })
					expect(status.output.map(({ kind, state }) => `${kind}:${state}`)).toEqual([
						'AddExternalLink:Failed',
					])
					expect(yield* Queue.size(attempts)).toBe(0)
				}).pipe(Effect.provide(layer))
			}),
	)
}
