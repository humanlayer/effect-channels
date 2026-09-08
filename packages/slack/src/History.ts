import { Effect, Option, Stream } from 'effect'

import { HistoryFailed, UnsupportedContextScope } from './DomainErrors.js'
import type { Message } from './Message.js'
import type {
	ChannelThreadsInput,
	ContainerMessagesInput,
	MessageHistoryOptions,
	MessagePage,
	MessagesInput,
	ThreadPage,
	ThreadSummary,
} from './Operations.js'
import type { SlackService } from './SlackService.js'

interface HistoryOptionsDraft {
	limit?: number
	cursor?: string
	direction?: 'forward' | 'backward'
}

interface ContainerMessagesDraft {
	channel: ContainerMessagesInput['channel']
	before?: ContainerMessagesInput['before']
	options?: MessageHistoryOptions
}

export const historyOptionsWithCursor = (
	options: MessageHistoryOptions | undefined,
	cursor: string | undefined,
): MessageHistoryOptions | undefined => {
	if (options === undefined && cursor === undefined) {
		return undefined
	}
	const next: HistoryOptionsDraft = {}
	if (options?.limit !== undefined) {
		next.limit = options.limit
	}
	if (options?.direction !== undefined) {
		next.direction = options.direction
	}
	if (cursor !== undefined) {
		next.cursor = cursor
	}
	return next
}

export const messagePageStream = <E, R>(
	initialOptions: MessageHistoryOptions | undefined,
	page: (options: MessageHistoryOptions | undefined) => Effect.Effect<MessagePage, E, R>,
): Stream.Stream<Message, E, R> =>
	Stream.paginate<string | undefined, Message, E, R>(initialOptions?.cursor, (cursor) =>
		Effect.map(
			page(historyOptionsWithCursor(initialOptions, cursor)),
			(result) =>
				[
					result.messages,
					result.nextCursor === undefined || result.messages.length === 0
						? Option.none<string | undefined>()
						: Option.some(result.nextCursor),
				] as const,
		),
	)

export const threadSummaryPageStream = <E, R>(
	initialOptions: MessageHistoryOptions | undefined,
	page: (options: MessageHistoryOptions | undefined) => Effect.Effect<ThreadPage, E, R>,
): Stream.Stream<ThreadSummary, E, R> =>
	Stream.paginate<string | undefined, ThreadSummary, E, R>(initialOptions?.cursor, (cursor) =>
		Effect.map(
			page(historyOptionsWithCursor(initialOptions, cursor)),
			(result) =>
				[
					result.threads,
					result.nextCursor === undefined || result.threads.length === 0
						? Option.none<string | undefined>()
						: Option.some(result.nextCursor),
				] as const,
		),
	)

export const threadMessagePage = (
	provider: SlackService,
	input: MessagesInput,
): Effect.Effect<MessagePage, HistoryFailed> => {
	const page: Effect.Effect<MessagePage, HistoryFailed> = provider.capabilities.history.thread
		? provider.messages(input)
		: Effect.fail(
				HistoryFailed.make({
					provider: 'slack',
					message: 'provider does not support thread history',
					retryability: 'non_retryable',
				}),
			)
	return page.pipe(
		Effect.withSpan('slack.history.thread_page', {
			attributes: { provider: 'slack', thread_id: input.threadId, operation: 'thread_page' },
		}),
	)
}

export const threadMessageStream = (
	provider: SlackService,
	input: MessagesInput,
): Stream.Stream<Message, HistoryFailed> =>
	messagePageStream(input.options, (options) =>
		threadMessagePage(
			provider,
			options === undefined ? { threadId: input.threadId } : { threadId: input.threadId, options },
		),
	)

export const containerInputWithOptions = (
	input: ContainerMessagesInput,
	options: MessageHistoryOptions | undefined,
): ContainerMessagesInput => {
	const next: ContainerMessagesDraft = { channel: input.channel }
	if (input.before !== undefined) {
		next.before = input.before
	}
	if (options !== undefined) {
		next.options = options
	}
	return next
}

export const channelMessagePage = (
	provider: SlackService,
	input: ContainerMessagesInput,
): Effect.Effect<MessagePage, HistoryFailed | UnsupportedContextScope> => {
	const page: Effect.Effect<MessagePage, HistoryFailed | UnsupportedContextScope> = provider.capabilities.history
		.channelMessages
		? provider.containerMessages(input)
		: Effect.fail(
				UnsupportedContextScope.make({
					provider: 'slack',
					scope: 'channel_messages',
					retryability: 'non_retryable',
				}),
			)
	return page.pipe(
		Effect.withSpan('slack.history.channel_page', {
			attributes: { provider: 'slack', tenant: input.channel.tenant, operation: 'channel_page' },
		}),
	)
}

export const channelMessageStream = (
	provider: SlackService,
	input: ContainerMessagesInput,
): Stream.Stream<Message, HistoryFailed | UnsupportedContextScope> =>
	messagePageStream(input.options, (options) =>
		channelMessagePage(provider, containerInputWithOptions(input, options)),
	)

export const channelThreadPage = (
	provider: SlackService,
	input: ChannelThreadsInput,
): Effect.Effect<ThreadPage, HistoryFailed | UnsupportedContextScope> => {
	const page: Effect.Effect<ThreadPage, HistoryFailed | UnsupportedContextScope> = provider.capabilities.history
		.channelThreads
		? provider.channelThreads(input)
		: Effect.fail(
				UnsupportedContextScope.make({
					provider: 'slack',
					scope: 'channel_threads',
					retryability: 'non_retryable',
				}),
			)
	return page.pipe(
		Effect.withSpan('slack.history.channel_threads', {
			attributes: { provider: 'slack', tenant: input.channel.tenant, operation: 'channel_threads' },
		}),
	)
}

export const channelThreadStream = (
	provider: SlackService,
	input: ChannelThreadsInput,
): Stream.Stream<ThreadSummary, HistoryFailed | UnsupportedContextScope> =>
	threadSummaryPageStream(input.options, (options) =>
		channelThreadPage(
			provider,
			options === undefined ? { channel: input.channel } : { channel: input.channel, options },
		),
	)
