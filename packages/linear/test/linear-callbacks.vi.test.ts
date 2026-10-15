import { describe, it } from '@effect/vitest'
import { MailboxSubscriptions, ProviderEventExecutionFailed } from '@humanlayer/channels-delivery'
import { Cause, Effect, Exit, Layer } from 'effect'

import { LinearApi } from '../src/LinearApi'
import { makeLinearEventProcessor } from '../src/LinearEventProcessor'
import { firstAttempt, linearIssueCreateAdmission } from './fixtures'

const namespace = 'linear-processing-test'
const layer = Layer.mergeAll(Layer.mock(LinearApi, {}), Layer.mock(MailboxSubscriptions, {}))
const processor = <E>(handler: () => Effect.Effect<void, E>) =>
	makeLinearEventProcessor({ namespace, handlers: { onIssueCreated: handler } })

describe('Linear callbacks', () => {
	it.effect('maps declared retryability to a stable processor failure', ({ expect }) =>
		Effect.gen(function* () {
			const admission = linearIssueCreateAdmission(namespace)
			const error = yield* processor(() => Effect.fail({ retryability: 'non_retryable' as const }))
				.process([admission], yield* firstAttempt())
				.pipe(Effect.provide(layer), Effect.flip)
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
			const exit = yield* processor(() => Effect.interrupt)
				.process([admission], yield* firstAttempt())
				.pipe(Effect.provide(layer), Effect.exit)
			expect(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)).toBe(true)
		}),
	)
})
