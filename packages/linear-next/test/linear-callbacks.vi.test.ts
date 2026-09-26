import { describe, it } from '@effect/vitest'
import { MailboxSubscriptions, ProviderEventExecutionFailed } from '@humanlayer/channels-delivery-next'
import { Cause, Effect, Exit, Layer } from 'effect'

import { LinearApi } from '../src/LinearApi'
import { LinearCallbacks } from '../src/LinearCallbacks'
import { makeLinearEventProcessor } from '../src/LinearEventProcessor'
import { linearIssueCreateAdmission } from './fixtures'

const namespace = 'linear-processing-test'
const layer = <E>(handler: () => Effect.Effect<void, E>) =>
	Layer.mergeAll(
		LinearCallbacks.layer({ onIssueCreated: handler }),
		Layer.mock(LinearApi, {}),
		Layer.mock(MailboxSubscriptions, {}),
	)

describe('Linear callbacks', () => {
	it.effect('maps declared retryability to a stable processor failure', ({ expect }) =>
		Effect.gen(function* () {
			const admission = linearIssueCreateAdmission(namespace)
			const error = yield* makeLinearEventProcessor({ namespace })
				.process([admission])
				.pipe(Effect.provide(layer(() => Effect.fail({ retryability: 'non_retryable' as const }))), Effect.flip)
			expect(error).toEqual(
				ProviderEventExecutionFailed.make({
					provider: 'linear',
					retryable: false,
					safeCode: 'callback_failed',
				}),
			)
		}),
	)

	it.effect('preserves interruption', ({ expect }) =>
		Effect.gen(function* () {
			const admission = linearIssueCreateAdmission(namespace)
			const exit = yield* makeLinearEventProcessor({ namespace })
				.process([admission])
				.pipe(Effect.provide(layer(() => Effect.interrupt)), Effect.exit)
			expect(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)).toBe(true)
		}),
	)
})
