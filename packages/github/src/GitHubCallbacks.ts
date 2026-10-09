import type { DeliveryCallbackResult, DeliveryContext } from '@humanlayer/channels-delivery'
import { Cause, Context, Effect, Layer, Option, Predicate, Schema } from 'effect'

import type {
	GitHubIssueCreated,
	GitHubMentioned,
	GitHubPrCreated,
	GitHubSubscribedIssueEvents,
	GitHubSubscribedPrEvents,
} from './GitHubCallbackEvents'

export const GitHubCallbackName = Schema.Literals([
	'onIssueCreated',
	'onPrCreated',
	'onMentioned',
	'onSubscribedIssueEvents',
	'onSubscribedPrEvents',
])
export type GitHubCallbackName = typeof GitHubCallbackName.Type

export const GitHubCallbackRetryability = Schema.Struct({
	retryability: Schema.Literals(['retryable', 'non_retryable']),
})
export interface GitHubCallbackRetryability extends Schema.Schema.Type<typeof GitHubCallbackRetryability> {}

const GitHubCallbackRetryableFlag = Schema.Struct({ retryable: Schema.Boolean })

export class GitHubCallbackError extends Schema.TaggedError<GitHubCallbackError>()('GitHubCallbackError', {
	callback: GitHubCallbackName,
	retryable: Schema.Boolean,
	reason: Schema.Literals(['failed', 'unexpected']),
}) {}

/**
 * One application callback. `delivery` names the delivery and can hand it to a remote worker; a callback
 * that did so may return the `DeliveryHandoff`. The saved handoff, not the return value, is authoritative.
 */
export type GitHubCallbackHandler<A, E, R> = (
	event: A,
	delivery: DeliveryContext,
) => Effect.Effect<DeliveryCallbackResult, E, R>

export type GitHubCallbackHandlers<E, R> = {
	readonly onIssueCreated?: GitHubCallbackHandler<GitHubIssueCreated, E, R>
	readonly onPrCreated?: GitHubCallbackHandler<GitHubPrCreated, E, R>
	readonly onMentioned?: GitHubCallbackHandler<GitHubMentioned, E, R>
	readonly onSubscribedIssueEvents?: GitHubCallbackHandler<GitHubSubscribedIssueEvents, E, R>
	readonly onSubscribedPrEvents?: GitHubCallbackHandler<GitHubSubscribedPrEvents, E, R>
}

const retryableFromCause = (cause: Cause.Cause<unknown>): boolean =>
	Option.match(Cause.findErrorOption(cause), {
		onNone: () => true,
		onSome: (error) => {
			if (Schema.is(GitHubCallbackRetryability)(error)) return error.retryability === 'retryable'
			if (Schema.is(GitHubCallbackRetryableFlag)(error)) return error.retryable
			return true
		},
	})

const narrowGitHubCallbackCause = (input: {
	readonly callback: GitHubCallbackName
	readonly cause: Cause.Cause<unknown>
}) => {
	if (Cause.hasInterrupts(input.cause)) {
		const preservedReasons = input.cause.reasons.filter(
			(reason) => Cause.isInterruptReason(reason) || Cause.isDieReason(reason),
		)
		const preserveCause = Effect.failCause(Cause.fromReasons<never>(preservedReasons))
		if (Cause.hasInterruptsOnly(input.cause)) return preserveCause
		return Effect.logError(
			'GitHub application callback was interrupted with additional failures',
			input.cause,
		).pipe(
			Effect.annotateLogs({ callback: input.callback, classification: 'interrupted_with_failure' }),
			Effect.andThen(preserveCause),
		)
	}
	const unexpected = Cause.hasDies(input.cause)
	return Effect.logError('GitHub application callback failed', input.cause).pipe(
		Effect.annotateLogs({ callback: input.callback, classification: unexpected ? 'unexpected_defect' : 'failed' }),
		Effect.andThen(
			Effect.fail(
				GitHubCallbackError.make({
					callback: input.callback,
					reason: unexpected ? 'unexpected' : 'failed',
					retryable: unexpected ? true : retryableFromCause(input.cause),
				}),
			),
		),
	)
}

const wrapCallback = <A, E, R>(callback: GitHubCallbackName, handler: GitHubCallbackHandler<A, E, R> | undefined) => {
	if (Predicate.isUndefined(handler)) return undefined
	return Effect.fn(`github.callbacks.${callback}`)((event: A, delivery: DeliveryContext) =>
		Effect.suspend(() => handler(event, delivery)).pipe(
			Effect.catchCause((cause) => narrowGitHubCallbackCause({ callback, cause })),
		),
	)
}

export type GitHubCallbackOperations<R = never> = GitHubCallbackHandlers<GitHubCallbackError, R>

export const makeGitHubCallbacks = <E, R>(handlers: GitHubCallbackHandlers<E, R>): GitHubCallbackOperations<R> => ({
	onIssueCreated: wrapCallback('onIssueCreated', handlers.onIssueCreated),
	onPrCreated: wrapCallback('onPrCreated', handlers.onPrCreated),
	onMentioned: wrapCallback('onMentioned', handlers.onMentioned),
	onSubscribedIssueEvents: wrapCallback('onSubscribedIssueEvents', handlers.onSubscribedIssueEvents),
	onSubscribedPrEvents: wrapCallback('onSubscribedPrEvents', handlers.onSubscribedPrEvents),
})

export class GitHubCallbacks extends Context.Service<
	GitHubCallbacks,
	GitHubCallbackHandlers<GitHubCallbackError, never>
>()('@humanlayer/channels-github/GitHubCallbacks') {
	static readonly layer = <E, R>(handlers: GitHubCallbackHandlers<E, R>): Layer.Layer<GitHubCallbacks, never, R> =>
		Layer.effect(
			GitHubCallbacks,
			Effect.gen(function* () {
				const context = yield* Effect.context<R>()
				const callbacks = makeGitHubCallbacks(handlers)
				const provide = <A, Error>(
					callback:
						| ((event: A, delivery: DeliveryContext) => Effect.Effect<DeliveryCallbackResult, Error, R>)
						| undefined,
				) =>
					Predicate.isUndefined(callback)
						? undefined
						: (event: A, delivery: DeliveryContext) =>
								callback(event, delivery).pipe(Effect.provide(context))
				return GitHubCallbacks.of({
					onIssueCreated: provide(callbacks.onIssueCreated),
					onPrCreated: provide(callbacks.onPrCreated),
					onMentioned: provide(callbacks.onMentioned),
					onSubscribedIssueEvents: provide(callbacks.onSubscribedIssueEvents),
					onSubscribedPrEvents: provide(callbacks.onSubscribedPrEvents),
				})
			}),
		)
}
