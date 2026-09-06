import {
	Slack,
	Emoji,
	type MessageEvent,
	type MessageUpdatedEvent,
	type MessageDeletedEvent,
	type ReactionEvent,
	type ConversationStoppedEvent,
} from '@humanlayer/channels-slack'
import { Effect, Stream } from 'effect'

import { respond, demonstrateLifecycle } from './responses.ts'

export const handlers = {
	onNewMention: Effect.fn('example.echo.onNewMention')(function* ({ thread, message }: MessageEvent) {
		yield* Effect.logInfo(`Received a mention from ${message.author.fullName}`)
		const slack = yield* Slack
		const previousChannelMessages = yield* slack.containerMessages({
			channel: thread.ref.channel,
			before: message.ref,
			options: { limit: 20, direction: 'backward' },
		})
		yield* Effect.logInfo(`Loaded ${previousChannelMessages.messages.length} previous channel messages`)
		yield* thread.subscribe()
		yield* thread.startTyping()
		const sent = yield* respond({ thread, prefix: 'Echo', text: message.text })
		yield* demonstrateLifecycle({ thread, message, sent })
	}),
	onSubscribedMessage: Effect.fn('example.echo.onSubscribedMessage')(function* ({ thread, message }: MessageEvent) {
		yield* Effect.logInfo(`Received a subscribed message from ${message.author.fullName}`)
		const threadMessages = yield* thread.messages.pipe(Stream.take(100), Stream.runCollect)
		yield* Effect.logInfo(`Loaded ${threadMessages.length} recent messages from the thread`)
		yield* thread.startTyping()
		const sent = yield* respond({ thread, prefix: 'Echo 2', text: message.text })
		yield* demonstrateLifecycle({ thread, message, sent })
	}),
	onDirectMessage: Effect.fn('example.echo.onDirectMessage')(function* ({ thread, message }: MessageEvent) {
		yield* Effect.logInfo(`Received a direct message from ${message.author.fullName}`)
		const sent = yield* respond({ thread, prefix: 'Direct echo', text: message.text })
		yield* demonstrateLifecycle({ thread, message, sent })
	}),
	onMessageUpdated: (event: MessageUpdatedEvent) =>
		Effect.logInfo(`Message ${event.message.ref} was edited in ${event.thread.ref.id}`),
	onMessageDeleted: (event: MessageDeletedEvent) =>
		Effect.logInfo(`Message ${event.messageRef} was deleted from ${event.threadRef.id}`),
	onReaction: [
		{
			id: 'approval-reaction',
			emojis: [Emoji.ThumbsUp],
			handler: (event: ReactionEvent) =>
				Effect.logInfo(`${event.actor.fullName} approved with ${event.rawEmoji}`),
		},
		{
			id: 'heart-or-check-reaction',
			emojis: [Emoji.Heart, Emoji.Check],
			handler: (event: ReactionEvent) => Effect.logInfo(`${event.actor.fullName} reacted with ${event.rawEmoji}`),
		},
	],
	onConversationStopped: (event: ConversationStoppedEvent) =>
		Effect.logInfo(`Slack stopped the active response in ${event.threadRef.id}`),
}
