import { describe, it } from '@effect/vitest'
import { Effect, Queue } from 'effect'

import {
	DeliveryAdmission,
	DeliveryAdmissionBatch,
	processProviderEvent,
	ProviderEventHandled,
	ProviderEventInvalid,
	type ProviderEventProcessor,
} from '../src'
import { makeTestDeliveryExecution } from './delivery-execution'

const admission = (eventId: string, overrides: Partial<DeliveryAdmission> = {}) =>
	DeliveryAdmission.make({
		namespace: 'test-app',
		provider: 'example',
		installationId: 'installation',
		resourceId: 'resource',
		eventId,
		payload: { eventId },
		...overrides,
	})

describe('provider event batch processing', () => {
	it.effect('passes the complete ordered non-empty batch to the selected processor', ({ expect }) =>
		Effect.gen(function* () {
			const calls = yield* Queue.unbounded<DeliveryAdmissionBatch>()
			const processor: ProviderEventProcessor = {
				namespace: 'test-app',
				providerName: 'example',
				process: (batch) => Queue.offer(calls, batch).pipe(Effect.as(ProviderEventHandled.make({}))),
			}
			const batch = DeliveryAdmissionBatch.make([admission('first'), admission('second')])

			expect(yield* processProviderEvent([processor])(batch, (yield* makeTestDeliveryExecution()).execution)).toEqual(ProviderEventHandled.make({}))
			expect(yield* Queue.take(calls)).toEqual(batch)
		}),
	)

	it.effect('rejects mixed namespace or provider batches before invoking a processor', ({ expect }) =>
		Effect.gen(function* () {
			const calls = yield* Queue.unbounded<DeliveryAdmissionBatch>()
			const processor: ProviderEventProcessor = {
				namespace: 'test-app',
				providerName: 'example',
				process: (batch) => Queue.offer(calls, batch).pipe(Effect.as(ProviderEventHandled.make({}))),
			}
			const batch = DeliveryAdmissionBatch.make([admission('first'), admission('second', { provider: 'other' })])

			expect(yield* processProviderEvent([processor])(batch, (yield* makeTestDeliveryExecution()).execution).pipe(Effect.flip)).toEqual(
				ProviderEventInvalid.make({ provider: 'example', reason: 'identity_mismatch' }),
			)
			expect(yield* Queue.size(calls)).toBe(0)
		}),
	)
})
