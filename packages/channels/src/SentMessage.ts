import { Effect, Schema } from 'effect'

import { Channels } from './Channels.ts'
import type { Content } from './Content.ts'
import { Emoji } from './Emoji.ts'
import type {
	DeleteFailed,
	EditFailed,
	ReactionFailed,
	TenantDisabled,
	UnknownProvider,
	UnknownTenant,
} from './Errors.ts'
import { Message } from './Message.ts'
import { MessageRef, ProviderName, ThreadId } from './Schema.ts'

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
	edit(
		content: Content,
	): Effect.Effect<SentMessage, UnknownProvider | UnknownTenant | TenantDisabled | EditFailed, Channels> {
		return Effect.flatMap(Channels, (channels) =>
			channels.edit({ threadId: this.ref.threadId, messageRef: this.ref.messageRef, content }),
		)
	}

	delete(): Effect.Effect<void, UnknownProvider | UnknownTenant | TenantDisabled | DeleteFailed, Channels> {
		return Effect.flatMap(Channels, (channels) =>
			channels.delete({ threadId: this.ref.threadId, messageRef: this.ref.messageRef }),
		)
	}

	addReaction(emoji: Emoji): Effect.Effect<void, UnknownProvider | UnknownTenant | ReactionFailed, Channels> {
		return Effect.flatMap(Channels, (channels) =>
			channels.addReaction({ threadId: this.ref.threadId, messageRef: this.ref.messageRef, emoji }),
		)
	}

	removeReaction(emoji: Emoji): Effect.Effect<void, UnknownProvider | UnknownTenant | ReactionFailed, Channels> {
		return Effect.flatMap(Channels, (channels) =>
			channels.removeReaction({ threadId: this.ref.threadId, messageRef: this.ref.messageRef, emoji }),
		)
	}
}
