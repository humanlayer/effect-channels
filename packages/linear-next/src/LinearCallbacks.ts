import type { DeliveryCallbackResult, DeliveryContext } from '@humanlayer/channels-delivery-next'
import { Cause, Context, Effect, Layer, Option, Predicate, Schema } from 'effect'

import type { LinearCallbackEventMap } from './LinearCallbackEvents'

export const LinearCallbackName = Schema.Literals([
	'onAgentSessionCreated',
	'onAgentSessionPrompted',
	'onIssueCreated',
	'onMentioned',
	'onAssigned',
	'onSubscribedEvent',
])
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

/**
 * The application handler for one Linear callback.
 *
 * `delivery` names this delivery and can hand it to a remote worker; return what `delivery.handoff()`
 * returned, or nothing.
 */
export type LinearCallbackHandler<Name extends keyof LinearCallbackEventMap, E = never, R = never> = (
	event: LinearCallbackEventMap[Name],
	delivery: DeliveryContext,
) => Effect.Effect<DeliveryCallbackResult, E, R>

export type LinearCallbackHandlers<E, R> = {
	readonly [Name in keyof LinearCallbackEventMap]?: LinearCallbackHandler<Name, E, R>
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

const narrowCause = (callback: LinearCallbackName, cause: Cause.Cause<unknown>) => {
	if (Cause.hasInterrupts(cause)) {
		const reasons = cause.reasons.filter((reason) => Cause.isInterruptReason(reason) || Cause.isDieReason(reason))
		const preserve = Effect.failCause(Cause.fromReasons<never>(reasons))
		return Cause.hasInterruptsOnly(cause)
			? preserve
			: Effect.logError('Linear application callback was interrupted with additional failures', cause).pipe(
					Effect.annotateLogs({ callback, classification: 'interrupted_with_failure' }),
					Effect.andThen(preserve),
				)
	}
	const unexpected = Cause.hasDies(cause)
	return Effect.logError('Linear application callback failed', cause).pipe(
		Effect.annotateLogs({ callback, classification: unexpected ? 'unexpected_defect' : 'failed' }),
		Effect.andThen(
			Effect.fail(
				LinearCallbackError.make({
					callback,
					reason: unexpected ? 'unexpected' : 'failed',
					retryable: unexpected ? true : retryableFromCause(cause),
				}),
			),
		),
	)
}

const wrapCallback = <Name extends keyof LinearCallbackEventMap, E, R>(
	context: Context.Context<R>,
	callback: Name,
	handler: LinearCallbackHandler<Name, E, R> | undefined,
) =>
	Predicate.isUndefined(handler)
		? undefined
		: Effect.fn(`linear.callbacks.${callback}`)((event: LinearCallbackEventMap[Name], delivery: DeliveryContext) =>
				Effect.suspend(() => handler(event, delivery)).pipe(
					Effect.provide(context),
					Effect.catchCause((cause) => narrowCause(callback, cause)),
				),
			)

export class LinearCallbacks extends Context.Service<
	LinearCallbacks,
	LinearCallbackHandlers<LinearCallbackError, never>
>()('@humanlayer/channels-linear-next/LinearCallbacks') {
	static readonly layer = <E, R>(handlers: LinearCallbackHandlers<E, R>): Layer.Layer<LinearCallbacks, never, R> =>
		Layer.effect(
			LinearCallbacks,
			Effect.gen(function* () {
				const context = yield* Effect.context<R>()
				return LinearCallbacks.of({
					onAgentSessionCreated: wrapCallback(
						context,
						'onAgentSessionCreated',
						handlers.onAgentSessionCreated,
					),
					onAgentSessionPrompted: wrapCallback(
						context,
						'onAgentSessionPrompted',
						handlers.onAgentSessionPrompted,
					),
					onIssueCreated: wrapCallback(context, 'onIssueCreated', handlers.onIssueCreated),
					onMentioned: wrapCallback(context, 'onMentioned', handlers.onMentioned),
					onAssigned: wrapCallback(context, 'onAssigned', handlers.onAssigned),
					onSubscribedEvent: wrapCallback(context, 'onSubscribedEvent', handlers.onSubscribedEvent),
				})
			}),
		)
}
