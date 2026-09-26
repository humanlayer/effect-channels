import { assert, it } from '@effect/vitest'
import {
	DeliveryInterruption,
	DeliveryQueue,
	IngressAttributionStore,
	MailboxStore,
} from '@humanlayer/channels-delivery'
import { layer as deliveryMemory } from '@humanlayer/channels-delivery/memory'
import { Array as Arr, Effect, Exit, Fiber, Layer, Queue } from 'effect'

import { PostFailed, SlackIngress, SlackSubscriptions } from '../src/index'
import { nativeIngressLayer, nativeMessage, nativePolicy, nativeRunner } from './nativeSupport'
import { unusedSlack } from './support'

for (const failure of [false, true]) {
	it.effect(`closes the handler scope before recording ${failure ? 'failure' : 'success'}`, () =>
		Effect.gen(function* () {
			const observed = yield* Queue.unbounded<boolean>()
			let cleaned = false
			const checkedStore = Layer.effect(
				MailboxStore,
				Effect.gen(function* () {
					const store = yield* MailboxStore
					return MailboxStore.of({
						loadMailbox: store.loadMailbox,
						commitMailbox: (input) =>
							store
								.commitMailbox(input)
								.pipe(
									Effect.tap((result) =>
										result === 'committed' && Arr.isReadonlyArrayNonEmpty(input.nextState.outcomes)
											? Queue.offer(observed, cleaned)
											: Effect.void,
									),
								),
					})
				}),
			)
			const layer = nativeIngressLayer(
				{
					onNewMention: [
						{
							id: 'scoped-handler',
							handler: ({ thread }) =>
								Effect.gen(function* () {
									yield* Effect.addFinalizer(() =>
										Effect.sync(() => {
											cleaned = true
										}),
									)
									if (failure)
										return yield* PostFailed.make({
											provider: 'slack',
											threadId: thread.ref.id,
											message: 'fixture',
											retryability: 'non_retryable',
										})
								}),
						},
					],
				},
				checkedStore.pipe(Layer.provideMerge(deliveryMemory({ maxMailboxes: 10 }))),
			)
			yield* Effect.gen(function* () {
				const ingress = yield* SlackIngress
				yield* ingress.acceptMessage(nativeMessage(failure ? 'f' : 'a'))
				const worker = yield* ingress.run(nativeRunner).pipe(Effect.forkChild)
				assert.strictEqual(yield* Queue.take(observed), true)
				yield* Fiber.interrupt(worker)
			}).pipe(Effect.provide(layer))
		}),
	)
}

it.effect('rejects an invalid delivery policy during ingress Layer acquisition', () =>
	Effect.gen(function* () {
		const invalid = SlackIngress.layer({
			namespace: 'invalid',
			policy: { ...nativePolicy, heartbeatMs: nativePolicy.leaseMs },
			handlers: {},
		}).pipe(
			Layer.provide(unusedSlack),
			Layer.provide(
				Layer.mergeAll(
					Layer.mock(DeliveryQueue, {}),
					Layer.mock(DeliveryInterruption, {}),
					Layer.mock(IngressAttributionStore, {}),
					Layer.mock(SlackSubscriptions, {}),
				),
			),
		)
		const result = yield* Layer.build(invalid).pipe(Effect.exit)
		assert.ok(Exit.isFailure(result))
		const error = yield* Effect.failCause(result.cause).pipe(Effect.flip)
		assert.strictEqual(error.operation, 'configuration')
	}),
)
