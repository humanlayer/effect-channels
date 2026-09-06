import { Effect, Schema, Stream } from 'effect'

import type { Content } from './Content.ts'
import type {
	ChannelGone,
	HistoryFailed,
	MetadataFailed,
	PostFailed,
	UnknownTenant,
	UnsupportedContextScope,
} from './DomainErrors.ts'
import type { Message } from './Message.ts'
import type { ChannelInfo, ChannelRef } from './Model.ts'
import { ChannelRef as ChannelRefSchema } from './Model.ts'
import type { MessageHistoryOptions, MessagePage, ThreadPage, ThreadSummary } from './Operations.ts'
import type { SentMessage } from './SentMessage.ts'
import { Slack } from './Slack.ts'

export class Channel extends Schema.TaggedClass<Channel>()('Channel', {
	ref: ChannelRefSchema,
}) {
	static fromRef(ref: ChannelRef) {
		return Channel.make({ ref })
	}

	post(content: Content): Effect.Effect<SentMessage, UnknownTenant | PostFailed, Slack> {
		return Effect.flatMap(Slack, (slack) => slack.postToChannel({ channel: this.ref, content }))
	}

	startTyping(): Effect.Effect<void, never, Slack> {
		return Effect.flatMap(Slack, (slack) => slack.startChannelTyping({ channel: this.ref }))
	}

	/**
	 * Returns one provider-backed page of messages from this channel.
	 */
	listMessages(
		options?: MessageHistoryOptions,
	): Effect.Effect<MessagePage, HistoryFailed | UnsupportedContextScope, Slack> {
		return Effect.flatMap(Slack, (slack) => {
			if (options === undefined) {
				return slack.containerMessages({ channel: this.ref })
			}
			return slack.containerMessages({ channel: this.ref, options })
		})
	}

	/**
	 * Lazily reads channel messages newest-first.
	 */
	get messages(): Stream.Stream<Message, HistoryFailed | UnsupportedContextScope, Slack> {
		return Stream.unwrap(Effect.map(Slack, (slack) => slack.containerMessageStream({ channel: this.ref })))
	}

	/**
	 * Returns one provider-backed page of threads from this channel.
	 */
	listThreads(
		options?: MessageHistoryOptions,
	): Effect.Effect<ThreadPage, HistoryFailed | UnsupportedContextScope, Slack> {
		return Effect.flatMap(Slack, (slack) => {
			if (options === undefined) {
				return slack.channelThreads({ channel: this.ref })
			}
			return slack.channelThreads({ channel: this.ref, options })
		})
	}

	/**
	 * Lazily reads every page of threads from the channel.
	 */
	get threads(): Stream.Stream<ThreadSummary, HistoryFailed | UnsupportedContextScope, Slack> {
		return Stream.unwrap(Effect.map(Slack, (slack) => slack.channelThreadStream({ channel: this.ref })))
	}

	fetchMetadata(): Effect.Effect<ChannelInfo, ChannelGone | MetadataFailed, Slack> {
		return Effect.flatMap(Slack, (slack) => slack.channelInfo({ channel: this.ref }))
	}
}
