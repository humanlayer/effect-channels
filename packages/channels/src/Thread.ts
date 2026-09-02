import { Effect, Schema, Stream } from 'effect'

import { Channel } from './Channel.ts'
import { Channels } from './Channels.ts'
import type { Content } from './Content.ts'
import type {
	HistoryFailed,
	MetadataFailed,
	PostFailed,
	SubscriptionStoreError,
	ThreadGone,
	UnknownProvider,
	UnknownTenant,
	TenantDisabled,
} from './Errors.ts'
import type { SubscriptionTransition } from './Events.ts'
import { Message } from './Message.ts'
import type { MessageHistoryOptions, MessagePage } from './Operations.ts'
import { MessageHistoryOptions as MessageHistoryOptionsSchema, MessagesInput } from './Operations.ts'
import type { Author, ThreadInfo } from './Schema.ts'
import { ThreadRef } from './Schema.ts'
import type { SentMessage } from './SentMessage.ts'

export class Thread extends Schema.TaggedClass<Thread>()('Thread', {
	ref: ThreadRef,
	currentMessage: Schema.optionalKey(Schema.suspend(() => Message)),
	recentMessages: Schema.Array(Schema.suspend(() => Message)),
}) {
	static fromRef(ref: ThreadRef) {
		return Thread.make({ ref, recentMessages: [] })
	}

	get channel() {
		return Channel.fromRef(this.ref.channel)
	}

	post(
		content: Content,
	): Effect.Effect<SentMessage, UnknownProvider | UnknownTenant | TenantDisabled | PostFailed, Channels> {
		return Effect.flatMap(Channels, (channels) => channels.post({ threadId: this.ref.id, content }))
	}

	startTyping(): Effect.Effect<void, never, Channels> {
		return Effect.flatMap(Channels, (channels) => channels.startThreadTyping({ threadId: this.ref.id }))
	}

	subscribe(): Effect.Effect<SubscriptionTransition, SubscriptionStoreError, Channels> {
		return Effect.flatMap(Channels, (channels) => channels.subscribe({ threadId: this.ref.id }))
	}

	isSubscribed(): Effect.Effect<boolean, SubscriptionStoreError, Channels> {
		return Effect.flatMap(Channels, (channels) => channels.isSubscribed({ threadId: this.ref.id }))
	}

	unsubscribe(): Effect.Effect<void, SubscriptionStoreError, Channels> {
		return Effect.flatMap(Channels, (channels) => channels.unsubscribe({ threadId: this.ref.id }))
	}

	listMessages(
		options?: MessageHistoryOptions,
	): Effect.Effect<MessagePage, UnknownProvider | HistoryFailed, Channels> {
		return Effect.flatMap(Channels, (channels) => {
			if (options === undefined) {
				return channels.messages(MessagesInput.make({ threadId: this.ref.id }))
			}
			return channels.messages(MessagesInput.make({ threadId: this.ref.id, options }))
		})
	}

	get messages(): Stream.Stream<Message, UnknownProvider | HistoryFailed, Channels> {
		return Stream.unwrap(Effect.map(Channels, (channels) => channels.messageStream({ threadId: this.ref.id })))
	}

	get allMessages(): Stream.Stream<Message, UnknownProvider | HistoryFailed, Channels> {
		const options = MessageHistoryOptionsSchema.make({ direction: 'forward' })
		return Stream.unwrap(
			Effect.map(Channels, (channels) => channels.messageStream({ threadId: this.ref.id, options })),
		)
	}

	getParticipants(): Effect.Effect<ReadonlyArray<Author>, UnknownProvider | HistoryFailed, Channels> {
		const currentAuthor = this.currentMessage?.author
		const allMessages = this.allMessages
		return Effect.gen(function* () {
			const seen = new Map<string, Author>()
			const consider = (author: Author) => {
				if (author.isMe || author.isBot === true || seen.has(author.userId)) {
					return
				}
				seen.set(author.userId, author)
			}
			if (currentAuthor !== undefined) {
				consider(currentAuthor)
			}
			yield* Stream.runForEach(allMessages, (message) => Effect.sync(() => consider(message.author)))
			return [...seen.values()]
		}).pipe(
			Effect.withSpan('channels.participants', {
				attributes: { provider: this.ref.channel.provider, thread_id: this.ref.id, operation: 'participants' },
			}),
		)
	}

	fetchMetadata(): Effect.Effect<ThreadInfo, UnknownProvider | ThreadGone | MetadataFailed, Channels> {
		return Effect.flatMap(Channels, (channels) => channels.info({ threadId: this.ref.id }))
	}
}
