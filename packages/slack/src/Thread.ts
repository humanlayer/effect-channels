import { Effect, Schema, Stream } from 'effect'

import { Channel } from './Channel.ts'
import type { Content } from './Content.ts'
import type {
	HistoryFailed,
	MetadataFailed,
	PostFailed,
	SubscriptionStoreError,
	ThreadGone,
	UnknownTenant,
} from './DomainErrors.ts'
import { Message } from './Message.ts'
import type { Author, ThreadInfo } from './Model.ts'
import { ThreadRef } from './Model.ts'
import type { EphemeralFallback, EphemeralResult, MessageHistoryOptions, MessagePage } from './Operations.ts'
import { MessageHistoryOptions as MessageHistoryOptionsSchema, MessagesInput } from './Operations.ts'
import type { SentMessage } from './SentMessage.ts'
import { Slack } from './Slack.ts'
import type { SubscriptionTransition } from './SlackEvents.ts'
import { SlackSubscriptions } from './SlackSubscriptions.ts'
import { hydrateHistoryMessage, hydrateHistoryPage } from './SlackUserDirectory.ts'
import type { StreamChunk } from './StreamChunk.ts'

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

	get isDM() {
		return this.ref.channel.isDm
	}

	postEphemeral(
		user: Author,
		content: Content,
		fallback: EphemeralFallback,
	): Effect.Effect<EphemeralResult, UnknownTenant | PostFailed, Slack> {
		return Effect.flatMap(Slack, (slack) => slack.postEphemeral({ threadId: this.ref.id, user, content, fallback }))
	}

	post(content: Content): Effect.Effect<SentMessage, UnknownTenant | PostFailed, Slack> {
		return Effect.flatMap(Slack, (slack) => slack.post({ threadId: this.ref.id, content }))
	}

	stream<E, R>(
		chunks: Stream.Stream<StreamChunk, E, R>,
	): Effect.Effect<SentMessage, UnknownTenant | PostFailed, Slack | R> {
		return Effect.flatMap(Slack, (slack) => {
			const currentAuthor = this.currentMessage?.author
			const recipientUserId =
				currentAuthor !== undefined &&
				!currentAuthor.isMe &&
				currentAuthor.isBot !== true &&
				currentAuthor.userId !== 'unknown'
					? currentAuthor.userId
					: undefined
			return slack.stream(
				recipientUserId === undefined ? { threadId: this.ref.id } : { threadId: this.ref.id, recipientUserId },
				chunks,
			)
		})
	}

	startTyping(): Effect.Effect<void, never, Slack> {
		return Effect.flatMap(Slack, (slack) => slack.startThreadTyping({ threadId: this.ref.id }))
	}

	subscribe(): Effect.Effect<SubscriptionTransition, SubscriptionStoreError, SlackSubscriptions> {
		return Effect.flatMap(SlackSubscriptions, (subscriptions) => subscriptions.subscribe({ threadId: this.ref.id }))
	}

	isSubscribed(): Effect.Effect<boolean, SubscriptionStoreError, SlackSubscriptions> {
		return Effect.flatMap(SlackSubscriptions, (subscriptions) =>
			subscriptions.isSubscribed({ threadId: this.ref.id }),
		)
	}

	unsubscribe(): Effect.Effect<void, SubscriptionStoreError, SlackSubscriptions> {
		return Effect.flatMap(SlackSubscriptions, (subscriptions) =>
			subscriptions.unsubscribe({ threadId: this.ref.id }),
		)
	}

	/**
	 * Returns one provider-backed page of messages from this thread.
	 */
	listMessages(options?: MessageHistoryOptions): Effect.Effect<MessagePage, HistoryFailed, Slack> {
		return Effect.flatMap(Slack, (slack) => {
			if (options === undefined) {
				return slack
					.messages(MessagesInput.make({ threadId: this.ref.id }))
					.pipe(Effect.flatMap(hydrateHistoryPage))
			}
			return slack
				.messages(MessagesInput.make({ threadId: this.ref.id, options }))
				.pipe(Effect.flatMap(hydrateHistoryPage))
		})
	}

	/**
	 * Lazily reads thread messages newest-first.
	 */
	get messages(): Stream.Stream<Message, HistoryFailed, Slack> {
		return Stream.unwrap(Effect.map(Slack, (slack) => slack.messageStream({ threadId: this.ref.id }))).pipe(
			Stream.mapEffect(hydrateHistoryMessage),
		)
	}

	/**
	 * Lazily reads the complete thread oldest-first.
	 */
	get allMessages(): Stream.Stream<Message, HistoryFailed, Slack> {
		const options = MessageHistoryOptionsSchema.make({ direction: 'forward' })
		return Stream.unwrap(
			Effect.map(Slack, (slack) => slack.messageStream({ threadId: this.ref.id, options })),
		).pipe(Stream.mapEffect(hydrateHistoryMessage))
	}

	getParticipants(): Effect.Effect<ReadonlyArray<Author>, HistoryFailed, Slack> {
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

	fetchMetadata(): Effect.Effect<ThreadInfo, ThreadGone | MetadataFailed, Slack> {
		return Effect.flatMap(Slack, (slack) => slack.info({ threadId: this.ref.id }))
	}
}
