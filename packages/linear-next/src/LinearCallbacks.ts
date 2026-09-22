import { Cause, Context, Effect, Layer, Option, Schema } from 'effect'

import type { LinearIssueCreated } from './LinearCallbackEvents'

export const LinearCallbackName = Schema.Literal('onIssueCreated')
export type LinearCallbackName = typeof LinearCallbackName.Type

export const LinearCallbackRetryability = Schema.Struct({
	retryability: Schema.Literals(['retryable', 'non_retryable']),
})
const LinearCallbackRetryableFlag = Schema.Struct({ retryable: Schema.Boolean })

export class LinearCallbackError extends Schema.TaggedError<LinearCallbackError>()('LinearCallbackError', {
	callback: LinearCallbackName,
	retryable: Schema.Boolean,
	reason: Schema.Literals(['failed', 'unexpected']),
}) {}

export type LinearCallbackHandlers<E, R> = {
	readonly onIssueCreated?: (event: LinearIssueCreated) => Effect.Effect<void, E, R>
}

const retryableFromCause = (cause: Cause.Cause<unknown>): boolean =>
	Option.match(Cause.findErrorOption(cause), {
		onNone: () => true,
		onSome: (error) => {
			if (Schema.is(LinearCallbackRetryability)(error)) return error.retryability === 'retryable'
			if (Schema.is(LinearCallbackRetryableFlag)(error)) return error.retryable
			return true
		},
	})

const narrowCause = (cause: Cause.Cause<unknown>) => {
	if (Cause.hasInterrupts(cause)) {
		const reasons = cause.reasons.filter((reason) => Cause.isInterruptReason(reason) || Cause.isDieReason(reason))
		const preserve = Effect.failCause(Cause.fromReasons<never>(reasons))
		return Cause.hasInterruptsOnly(cause)
			? preserve
			: Effect.logError('Linear application callback was interrupted with additional failures', cause).pipe(
					Effect.andThen(preserve),
				)
	}
	const unexpected = Cause.hasDies(cause)
	return Effect.logError('Linear application callback failed', cause).pipe(
		Effect.annotateLogs({ callback: 'onIssueCreated', classification: unexpected ? 'unexpected_defect' : 'failed' }),
		Effect.andThen(
			Effect.fail(
				LinearCallbackError.make({
					callback: 'onIssueCreated',
					reason: unexpected ? 'unexpected' : 'failed',
					retryable: unexpected ? true : retryableFromCause(cause),
				}),
			),
		),
	)
}

export class LinearCallbacks extends Context.Service<
	LinearCallbacks,
	LinearCallbackHandlers<LinearCallbackError, never>
>()(
	'@humanlayer/channels-linear-next/LinearCallbacks',
) {
	static readonly layer = <E, R>(handlers: LinearCallbackHandlers<E, R>): Layer.Layer<LinearCallbacks, never, R> =>
		Layer.effect(
			LinearCallbacks,
			Effect.gen(function* () {
				const context = yield* Effect.context<R>()
				return LinearCallbacks.of({
					...(handlers.onIssueCreated === undefined
						? {}
						: {
								onIssueCreated: Effect.fn('linear.callbacks.onIssueCreated')((event: LinearIssueCreated) =>
									Effect.suspend(() => handlers.onIssueCreated?.(event) ?? Effect.void).pipe(
										Effect.provide(context),
										Effect.catchCause(narrowCause),
									),
								),
							}),
				})
			}),
		)
}
