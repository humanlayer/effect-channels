import { Cause, Context, Effect, Layer, Option, Schema } from 'effect'

import type { SlackNewMention, SlackSubscribedThreadEvents } from './SlackCallbackEvents'

export const SlackCallbackName = Schema.Literals(['onNewMention', 'onSubscribedThreadEvents'])
export type SlackCallbackName = typeof SlackCallbackName.Type

export const SlackCallbackRetryability = Schema.Struct({
	retryability: Schema.Literals(['retryable', 'non_retryable']),
})
export interface SlackCallbackRetryability extends Schema.Schema.Type<typeof SlackCallbackRetryability> {}

const SlackCallbackRetryableFlag = Schema.Struct({ retryable: Schema.Boolean })

export class SlackCallbackError extends Schema.TaggedError<SlackCallbackError>()('SlackCallbackError', {
	callback: SlackCallbackName,
	retryable: Schema.Boolean,
	reason: Schema.Literals(['failed', 'unexpected']),
}) {}

export type SlackCallbackHandlers<E, R> = {
	readonly onNewMention?: (event: SlackNewMention) => Effect.Effect<void, E, R>
	readonly onSubscribedThreadEvents?: (event: SlackSubscribedThreadEvents) => Effect.Effect<void, E, R>
}

const retryableFromCause = (cause: Cause.Cause<unknown>): boolean =>
	Option.match(Cause.findErrorOption(cause), {
		onNone: () => true,
		onSome: (error) => {
			if (Schema.is(SlackCallbackRetryability)(error)) return error.retryability === 'retryable'
			if (Schema.is(SlackCallbackRetryableFlag)(error)) return error.retryable
			return true
		},
	})

const narrowSlackCallbackCause = (input: {
	readonly callback: SlackCallbackName
	readonly cause: Cause.Cause<unknown>
}) => {
	if (Cause.hasInterrupts(input.cause)) {
		const preservedReasons = input.cause.reasons.filter(
			(reason) => Cause.isInterruptReason(reason) || Cause.isDieReason(reason),
		)
		const preserveCause = Effect.failCause(Cause.fromReasons<never>(preservedReasons))
		if (Cause.hasInterruptsOnly(input.cause)) return preserveCause
		return Effect.logError('Slack application callback was interrupted with additional failures', input.cause).pipe(
			Effect.annotateLogs({ callback: input.callback, classification: 'interrupted_with_failure' }),
			Effect.andThen(preserveCause),
		)
	}
	const unexpected = Cause.hasDies(input.cause)
	return Effect.logError('Slack application callback failed', input.cause).pipe(
		Effect.annotateLogs({ callback: input.callback, classification: unexpected ? 'unexpected_defect' : 'failed' }),
		Effect.andThen(
			Effect.fail(
				SlackCallbackError.make({
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
	callback: SlackCallbackName,
	handler: (event: A) => Effect.Effect<void, E, R>,
) =>
	Effect.fn(`slack.callbacks.${callback}`)((event: A) =>
		Effect.suspend(() => handler(event)).pipe(
			Effect.provide(context),
			Effect.catchCause((cause) => narrowSlackCallbackCause({ callback, cause })),
		),
	)

export class SlackCallbacks extends Context.Service<SlackCallbacks, SlackCallbackHandlers<SlackCallbackError, never>>()(
	'@humanlayer/channels-slack-next/SlackCallbacks',
) {
	static readonly layer = <E, R>(handlers: SlackCallbackHandlers<E, R>): Layer.Layer<SlackCallbacks, never, R> =>
		Layer.effect(
			SlackCallbacks,
			Effect.gen(function* () {
				const context = yield* Effect.context<R>()
				return SlackCallbacks.of({
					...(handlers.onNewMention === undefined
						? {}
						: { onNewMention: wrapCallback(context, 'onNewMention', handlers.onNewMention) }),
					...(handlers.onSubscribedThreadEvents === undefined
						? {}
						: {
								onSubscribedThreadEvents: wrapCallback(
									context,
									'onSubscribedThreadEvents',
									handlers.onSubscribedThreadEvents,
								),
							}),
				})
			}),
		)
}
