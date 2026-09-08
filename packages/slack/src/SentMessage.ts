import { Effect, Schema } from 'effect'

import type { Content } from './Content.js'
import type { DeleteFailed, EditFailed, ReactionFailed, UnknownTenant } from './DomainErrors.js'
import { Emoji } from './Emoji.js'
import { Message } from './Message.js'
import { MessageRef, ProviderName, ThreadId } from './Model.js'
import { Slack } from './Slack.js'

export const SentRef = Schema.Struct({
	threadId: ThreadId,
	messageRef: MessageRef,
	provider: ProviderName,
	degraded: Schema.Array(Schema.String),
})
export type SentRef = typeof SentRef.Type

export class SentMessage extends Schema.TaggedClass<SentMessage>()('SentMessage', {
	ref: SentRef,
	message: Schema.suspend(() => Message),
}) {
	edit(content: Content): Effect.Effect<SentMessage, UnknownTenant | EditFailed, Slack> {
		return Effect.flatMap(Slack, (slack) =>
			slack.edit({ threadId: this.ref.threadId, messageRef: this.ref.messageRef, content }),
		)
	}

	delete(): Effect.Effect<void, UnknownTenant | DeleteFailed, Slack> {
		return Effect.flatMap(Slack, (slack) =>
			slack.delete({ threadId: this.ref.threadId, messageRef: this.ref.messageRef }),
		)
	}

	addReaction(emoji: Emoji): Effect.Effect<void, UnknownTenant | ReactionFailed, Slack> {
		return Effect.flatMap(Slack, (slack) =>
			slack.addReaction({ threadId: this.ref.threadId, messageRef: this.ref.messageRef, emoji }),
		)
	}

	removeReaction(emoji: Emoji): Effect.Effect<void, UnknownTenant | ReactionFailed, Slack> {
		return Effect.flatMap(Slack, (slack) =>
			slack.removeReaction({ threadId: this.ref.threadId, messageRef: this.ref.messageRef, emoji }),
		)
	}
}
