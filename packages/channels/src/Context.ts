import { Effect, Match, Stream } from 'effect'

import type { ChannelProvider } from './ChannelProvider.ts'
import { ContextLoadFailed, UnsupportedContextScope } from './Errors.ts'
import type { MessageEvent } from './Events.ts'
import { channelMessageStream, threadMessageStream } from './History.ts'
import type { Message } from './Message.ts'
import { ConversationContext, type LoadContextInput } from './Operations.ts'

const collectThreadMessages = (input: {
	readonly provider: ChannelProvider
	readonly event: MessageEvent
	readonly threadLimit: number
}): Effect.Effect<ReadonlyArray<Message>, ContextLoadFailed | UnsupportedContextScope> => {
	if (!input.provider.capabilities.history.thread) {
		return Effect.fail(
			UnsupportedContextScope.make({
				provider: input.provider.name,
				scope: 'thread',
				retryability: 'non_retryable',
			}),
		)
	}
	return threadMessageStream(input.provider, {
		threadId: input.event.thread.ref.id,
		options: { direction: 'forward' },
	}).pipe(
		Stream.take(input.threadLimit),
		Stream.runCollect,
		Effect.tapError((error) => Effect.logError('conversation context thread history failed', error)),
		Effect.catchTag('HistoryFailed', (error) =>
			Effect.fail(
				ContextLoadFailed.make({
					provider: error.provider,
					message: 'thread history fetch failed',
					retryability: error.retryability ?? 'retryable',
				}),
			),
		),
	)
}

const collectContainerMessages = (input: {
	readonly provider: ChannelProvider
	readonly event: MessageEvent
	readonly containerLimit: number
}): Effect.Effect<ReadonlyArray<Message>, ContextLoadFailed | UnsupportedContextScope> =>
	channelMessageStream(input.provider, {
		channel: input.event.thread.ref.channel,
		before: input.event.message.ref,
		options: { direction: 'backward' },
	}).pipe(
		Stream.take(input.containerLimit),
		Stream.runCollect,
		Effect.map((newestFirst) => [...newestFirst].reverse()),
		Effect.tapError((error) => Effect.logError('conversation context container history failed', error)),
		Effect.catchTag('HistoryFailed', (error) =>
			Effect.fail(
				ContextLoadFailed.make({
					provider: error.provider,
					message: 'container history fetch failed',
					retryability: error.retryability ?? 'retryable',
				}),
			),
		),
	)

export const loadConversationContext = (
	provider: ChannelProvider,
	input: LoadContextInput,
): Effect.Effect<ConversationContext, ContextLoadFailed | UnsupportedContextScope> =>
	Match.value(input.policy).pipe(
		Match.tagsExhaustive({
			ThreadContext: (policy) =>
				Effect.map(
					collectThreadMessages({ provider, event: input.event, threadLimit: policy.threadLimit }),
					(threadMessages) =>
						ConversationContext.make({ event: input.event, threadMessages, containerMessages: [] }),
				),
			ContainerAndThreadContext: (policy) =>
				Effect.gen(function* () {
					const threadMessages = yield* collectThreadMessages({
						provider,
						event: input.event,
						threadLimit: policy.threadLimit,
					})
					const containerMessages = yield* collectContainerMessages({
						provider,
						event: input.event,
						containerLimit: policy.containerLimit,
					})
					return ConversationContext.make({ event: input.event, threadMessages, containerMessages })
				}),
		}),
	)
