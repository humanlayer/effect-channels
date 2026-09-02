import { Effect, Schema, Stream } from 'effect'

import { Channels } from './Channels.ts'
import type { Content } from './Content.ts'
import type {
	ChannelGone,
	HistoryFailed,
	MetadataFailed,
	PostFailed,
	TenantDisabled,
	UnknownProvider,
	UnknownTenant,
	UnsupportedContextScope,
} from './Errors.ts'
import type { Message } from './Message.ts'
import type { MessageHistoryOptions, MessagePage, ThreadPage, ThreadSummary } from './Operations.ts'
import type { ChannelInfo, ChannelRef } from './Schema.ts'
import { ChannelRef as ChannelRefSchema } from './Schema.ts'
import type { SentMessage } from './SentMessage.ts'

export class Channel extends Schema.TaggedClass<Channel>()('Channel', {
	ref: ChannelRefSchema,
}) {
	static fromRef(ref: ChannelRef) {
		return Channel.make({ ref })
	}

	post(
		content: Content,
	): Effect.Effect<SentMessage, UnknownProvider | UnknownTenant | TenantDisabled | PostFailed, Channels> {
		return Effect.flatMap(Channels, (channels) => channels.postToChannel({ channel: this.ref, content }))
	}

	startTyping(): Effect.Effect<void, never, Channels> {
		return Effect.flatMap(Channels, (channels) => channels.startChannelTyping({ channel: this.ref }))
	}

	listMessages(
		options?: MessageHistoryOptions,
	): Effect.Effect<MessagePage, UnknownProvider | HistoryFailed | UnsupportedContextScope, Channels> {
		return Effect.flatMap(Channels, (channels) => {
			if (options === undefined) {
				return channels.containerMessages({ channel: this.ref })
			}
			return channels.containerMessages({ channel: this.ref, options })
		})
	}

	get messages(): Stream.Stream<Message, UnknownProvider | HistoryFailed | UnsupportedContextScope, Channels> {
		return Stream.unwrap(Effect.map(Channels, (channels) => channels.containerMessageStream({ channel: this.ref })))
	}

	listThreads(
		options?: MessageHistoryOptions,
	): Effect.Effect<ThreadPage, UnknownProvider | HistoryFailed | UnsupportedContextScope, Channels> {
		return Effect.flatMap(Channels, (channels) => {
			if (options === undefined) {
				return channels.channelThreads({ channel: this.ref })
			}
			return channels.channelThreads({ channel: this.ref, options })
		})
	}

	get threads(): Stream.Stream<ThreadSummary, UnknownProvider | HistoryFailed | UnsupportedContextScope, Channels> {
		return Stream.unwrap(Effect.map(Channels, (channels) => channels.channelThreadStream({ channel: this.ref })))
	}

	fetchMetadata(): Effect.Effect<ChannelInfo, UnknownProvider | ChannelGone | MetadataFailed, Channels> {
		return Effect.flatMap(Channels, (channels) => channels.channelInfo({ channel: this.ref }))
	}
}
