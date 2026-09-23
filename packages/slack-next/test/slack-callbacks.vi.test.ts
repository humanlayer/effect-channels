import { describe, it } from '@effect/vitest'
import { Cause, Context, Effect, Exit, Layer } from 'effect'

import { SlackCallbackError, SlackCallbacks, type SlackCallbackHandlers } from '../src/SlackCallbacks'

class CallbackDependency extends Context.Service<CallbackDependency, { readonly value: string }>()(
	'@humanlayer/channels-slack-next/test/CallbackDependency',
) {}

const dependencyLayer = Layer.succeed(CallbackDependency, CallbackDependency.of({ value: 'captured' }))
const ignoredEvent = undefined as never

const buildCallbacks = <E, R>(handlers: SlackCallbackHandlers<E, R>) =>
	SlackCallbacks.pipe(Effect.provide(SlackCallbacks.layer(handlers)))

const requireNewMention = (callbacks: SlackCallbacks['Service']) =>
	callbacks.onNewMention ?? (() => Effect.die(new Error('Expected onNewMention callback')))

const requireSubscribed = (callbacks: SlackCallbacks['Service']) =>
	callbacks.onSubscribedThreadEvents ?? (() => Effect.die(new Error('Expected onSubscribedThreadEvents callback')))

describe('SlackCallbacks', () => {
	it.effect('captures the application context when its layer is built', ({ expect }) =>
		Effect.gen(function* () {
			let captured: string | undefined
			const callbacks = yield* buildCallbacks({
				onNewMention: () =>
					Effect.flatMap(CallbackDependency, ({ value }) =>
						Effect.sync(() => {
							captured = value
						}),
					),
			}).pipe(Effect.provide(dependencyLayer))

			yield* requireNewMention(callbacks)(ignoredEvent)
			expect(captured).toBe('captured')
		}),
	)

	it.effect('turns typed failures into retryable callback errors', ({ expect }) =>
		Effect.gen(function* () {
			const callbacks = yield* buildCallbacks({
				onNewMention: () => Effect.fail(new Error('application failure')),
			})
			const error = yield* requireNewMention(callbacks)(ignoredEvent).pipe(Effect.flip)
			expect(error).toEqual(
				SlackCallbackError.make({ callback: 'onNewMention', reason: 'failed', retryable: true }),
			)
		}),
	)

	it.effect('honors retryability metadata and boolean flags', ({ expect }) =>
		Effect.gen(function* () {
			const metadataCallbacks = yield* buildCallbacks({
				onNewMention: () => Effect.fail({ retryability: 'non_retryable' as const }),
			})
			const metadataError = yield* requireNewMention(metadataCallbacks)(ignoredEvent).pipe(Effect.flip)
			expect(metadataError).toEqual(
				SlackCallbackError.make({ callback: 'onNewMention', reason: 'failed', retryable: false }),
			)

			const flagCallbacks = yield* buildCallbacks({
				onSubscribedThreadEvents: () => Effect.fail({ retryable: false }),
			})
			const flagError = yield* requireSubscribed(flagCallbacks)(ignoredEvent).pipe(Effect.flip)
			expect(flagError).toEqual(
				SlackCallbackError.make({ callback: 'onSubscribedThreadEvents', reason: 'failed', retryable: false }),
			)
		}),
	)

	it.effect('classifies defects as unexpected retryable callback errors', ({ expect }) =>
		Effect.gen(function* () {
			const callbacks = yield* buildCallbacks({
				onNewMention: () => Effect.die(new Error('callback defect')),
			})
			const error = yield* requireNewMention(callbacks)(ignoredEvent).pipe(Effect.flip)
			expect(error).toEqual(
				SlackCallbackError.make({ callback: 'onNewMention', reason: 'unexpected', retryable: true }),
			)
		}),
	)

	it.effect('preserves interruption instead of converting it to a callback error', ({ expect }) =>
		Effect.gen(function* () {
			const callbacks = yield* buildCallbacks({ onNewMention: () => Effect.interrupt })
			const exit = yield* requireNewMention(callbacks)(ignoredEvent).pipe(Effect.exit)
			expect(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)).toBe(true)
		}),
	)
})
