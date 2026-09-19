import { Cause, Context, Effect, Layer, Option, Schema } from 'effect'

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

export type GitHubCallbackHandlers<E, R> = {
	readonly onIssueCreated?: (event: GitHubIssueCreated) => Effect.Effect<void, E, R>
	readonly onPrCreated?: (event: GitHubPrCreated) => Effect.Effect<void, E, R>
	readonly onMentioned?: (event: GitHubMentioned) => Effect.Effect<void, E, R>
	readonly onSubscribedIssueEvents?: (event: GitHubSubscribedIssueEvents) => Effect.Effect<void, E, R>
	readonly onSubscribedPrEvents?: (event: GitHubSubscribedPrEvents) => Effect.Effect<void, E, R>
}

export type GitHubCallbacksShape = {
	readonly onIssueCreated?: (event: GitHubIssueCreated) => Effect.Effect<void, GitHubCallbackError>
	readonly onPrCreated?: (event: GitHubPrCreated) => Effect.Effect<void, GitHubCallbackError>
	readonly onMentioned?: (event: GitHubMentioned) => Effect.Effect<void, GitHubCallbackError>
	readonly onSubscribedIssueEvents?: (event: GitHubSubscribedIssueEvents) => Effect.Effect<void, GitHubCallbackError>
	readonly onSubscribedPrEvents?: (event: GitHubSubscribedPrEvents) => Effect.Effect<void, GitHubCallbackError>
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

const wrapCallback = <A, E, R>(
	context: Context.Context<R>,
	callback: GitHubCallbackName,
	handler: (event: A) => Effect.Effect<void, E, R>,
) =>
	Effect.fn(`github.callbacks.${callback}`)((event: A) =>
		Effect.suspend(() => handler(event)).pipe(
			Effect.provide(context),
			Effect.catchCause((cause) => narrowGitHubCallbackCause({ callback, cause })),
		),
	)

export class GitHubCallbacks extends Context.Service<GitHubCallbacks, GitHubCallbacksShape>()(
	'@humanlayer/channels-github-next/GitHubCallbacks',
) {
	static readonly layer = <E, R>(handlers: GitHubCallbackHandlers<E, R>): Layer.Layer<GitHubCallbacks, never, R> =>
		Layer.effect(
			GitHubCallbacks,
			Effect.gen(function* () {
				const context = yield* Effect.context<R>()
				return GitHubCallbacks.of({
					...(handlers.onIssueCreated === undefined
						? {}
						: {
								onIssueCreated: wrapCallback(context, 'onIssueCreated', handlers.onIssueCreated),
							}),
					...(handlers.onPrCreated === undefined
						? {}
						: { onPrCreated: wrapCallback(context, 'onPrCreated', handlers.onPrCreated) }),
					...(handlers.onMentioned === undefined
						? {}
						: { onMentioned: wrapCallback(context, 'onMentioned', handlers.onMentioned) }),
					...(handlers.onSubscribedIssueEvents === undefined
						? {}
						: {
								onSubscribedIssueEvents: wrapCallback(
									context,
									'onSubscribedIssueEvents',
									handlers.onSubscribedIssueEvents,
								),
							}),
					...(handlers.onSubscribedPrEvents === undefined
						? {}
						: {
								onSubscribedPrEvents: wrapCallback(
									context,
									'onSubscribedPrEvents',
									handlers.onSubscribedPrEvents,
								),
							}),
				})
			}),
		)
}
