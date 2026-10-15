import type { DeliveryCallbackResult, DeliveryContext } from '@humanlayer/channels-delivery'
import { Cause, Effect, Option, Predicate, Schema } from 'effect'

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
	callback: Name,
	handler: LinearCallbackHandler<Name, E, R> | undefined,
): LinearCallbackHandler<Name, LinearCallbackError, R> | undefined =>
	Predicate.isUndefined(handler)
		? undefined
		: Effect.fn(`linear.callbacks.${callback}`)((event: LinearCallbackEventMap[Name], delivery: DeliveryContext) =>
				Effect.suspend(() => handler(event, delivery)).pipe(
					Effect.catchCause((cause) => narrowCause(callback, cause)),
				),
			)

/** The application's handlers after `makeLinearCallbacks` has narrowed their failures to `LinearCallbackError`. */
export type LinearCallbackOperations<R = never> = LinearCallbackHandlers<LinearCallbackError, R>

export const makeLinearCallbacks = <E, R>(handlers: LinearCallbackHandlers<E, R>) =>
	({
		onAgentSessionCreated: wrapCallback('onAgentSessionCreated', handlers.onAgentSessionCreated),
		onAgentSessionPrompted: wrapCallback('onAgentSessionPrompted', handlers.onAgentSessionPrompted),
		onIssueCreated: wrapCallback('onIssueCreated', handlers.onIssueCreated),
		onMentioned: wrapCallback('onMentioned', handlers.onMentioned),
		onAssigned: wrapCallback('onAssigned', handlers.onAssigned),
		onSubscribedEvent: wrapCallback('onSubscribedEvent', handlers.onSubscribedEvent),
	}) satisfies LinearCallbackOperations<R>
