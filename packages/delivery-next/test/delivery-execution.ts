/**
 * A `ProviderDeliveryExecution` for provider tests that run a processor without mailbox storage.
 *
 * It records every preparation and handoff, and saves the first preparation the way a store does, so
 * a test can run a processor twice and check that the second attempt reuses the saved callback.
 */
import { Effect, Option, Redacted, Ref } from 'effect'

import {
	BatchId,
	DeliveryContext,
	DeliveryHandoff,
	DeliveryPreparationConflict,
	ProviderDeliveryExecution,
	makeConversationId,
	makeDeliveryId,
	type HandoffOptions,
	type PreparedDeliveryInvocation,
} from '../src'

export type TestDeliveryExecution = {
	/** Pass this to `process`. Its `prepared` is what the previous attempt saved. */
	readonly execution: ProviderDeliveryExecution
	/** Every preparation proposed, in order. */
	readonly preparations: Ref.Ref<ReadonlyArray<PreparedDeliveryInvocation>>
	/** Every handoff, in order. */
	readonly handoffs: Ref.Ref<ReadonlyArray<HandoffOptions | undefined>>
	/** An execution for the next attempt at the same batch, carrying what this one saved. */
	readonly retry: Effect.Effect<TestDeliveryExecution>
}

const make = (input: {
	readonly mailboxKey: string
	readonly saved: Ref.Ref<Option.Option<PreparedDeliveryInvocation>>
}): Effect.Effect<TestDeliveryExecution> =>
	Effect.gen(function* () {
		const deliveryId = makeDeliveryId({ mailboxKey: input.mailboxKey, batchId: BatchId.make('test-batch') })
		const preparations = yield* Ref.make<ReadonlyArray<PreparedDeliveryInvocation>>([])
		const handoffs = yield* Ref.make<ReadonlyArray<HandoffOptions | undefined>>([])
		const prepared = yield* Ref.get(input.saved)
		const execution = new ProviderDeliveryExecution({
			deliveryId,
			prepared,
			prepare: (proposed) =>
				Effect.gen(function* () {
					yield* Ref.update(preparations, (all) => [...all, proposed])
					const current = yield* Ref.get(input.saved)
					if (Option.isNone(current)) {
						yield* Ref.set(input.saved, Option.some(proposed))
						return proposed
					}
					return current.value.callback === proposed.callback
						? current.value
						: yield* new DeliveryPreparationConflict({ deliveryId })
				}),
			context: new DeliveryContext({
				deliveryId,
				conversationId: makeConversationId(input.mailboxKey),
				accessToken: Redacted.make('test-access-token'),
				handoff: (options) =>
					Ref.update(handoffs, (all) => [...all, options]).pipe(Effect.as(DeliveryHandoff.make({ deliveryId }))),
			}),
		})
		return { execution, preparations, handoffs, retry: make(input) }
	})

/** A fresh execution for the first attempt at a batch. */
export const makeTestDeliveryExecution = (mailboxKey = 'test-mailbox') =>
	Effect.gen(function* () {
		const saved = yield* Ref.make(Option.none<PreparedDeliveryInvocation>())
		return yield* make({ mailboxKey, saved })
	})
