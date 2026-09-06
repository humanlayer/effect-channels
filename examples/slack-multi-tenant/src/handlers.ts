import {
	Slack,
	Emoji,
	type MessageEvent,
	type MessageUpdatedEvent,
	type MessageDeletedEvent,
	type ReactionEvent,
	type ConversationStoppedEvent,
} from '@humanlayer/channels-slack'
import { Effect } from 'effect'

import { respond } from './responses.ts'

const reactionRequest = /\breact(?:ion)?\b/i
const reply = ({ thread, message }: MessageEvent) => respond({ thread, text: message.text }).pipe(Effect.asVoid)

export const handlers = {
	onNewMention: Effect.fn('example.workspace_echo.onNewMention')(function* ({ thread, message }: MessageEvent) {
		yield* Effect.logInfo(`Received a mention from ${message.author.fullName}`).pipe(
			Effect.annotateLogs({ tenant: thread.ref.channel.tenant }),
		)
		yield* thread.subscribe()
		yield* respond({ thread, text: message.text, mention: true })
		if (reactionRequest.test(message.text)) {
			const slack = yield* Slack
			yield* slack.addReaction({
				threadId: thread.ref.id,
				messageRef: message.ref,
				emoji: Emoji.Check,
			})
		}
	}),
	onSubscribedMessage: reply,
	onDirectMessage: reply,
	onMessageUpdated: (event: MessageUpdatedEvent) =>
		Effect.logInfo(`Workspace message ${event.message.ref} was edited`).pipe(
			Effect.annotateLogs({ tenant: event.tenant }),
		),
	onMessageDeleted: (event: MessageDeletedEvent) =>
		Effect.logInfo(`Workspace message ${event.messageRef} was deleted`).pipe(
			Effect.annotateLogs({ tenant: event.tenant }),
		),
	onReaction: (event: ReactionEvent) =>
		Effect.logInfo(`Workspace reaction ${event.rawEmoji}`).pipe(Effect.annotateLogs({ tenant: event.tenant })),
	onConversationStopped: (event: ConversationStoppedEvent) =>
		Effect.logInfo(`Workspace stream stopped in ${event.threadRef.id}`).pipe(
			Effect.annotateLogs({ tenant: event.tenant }),
		),
}
