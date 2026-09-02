import { Effect, Option, Schema } from 'effect'

import { Attachment } from './Attachment.ts'
import { Channels } from './Channels.ts'
import type { SubjectFailed, UnknownProvider } from './Errors.ts'
import { Author, MessageMetadata, MessageRef, type MessageSubject, ThreadRef } from './Schema.ts'

export class Message extends Schema.TaggedClass<Message>()('Message', {
	ref: MessageRef,
	threadRef: ThreadRef,
	text: Schema.String,
	markdown: Schema.String,
	author: Author,
	metadata: MessageMetadata,
	attachments: Schema.Array(Schema.suspend(() => Attachment)),
	replyTo: Schema.optionalKey(MessageRef),
	raw: Schema.Json,
}) {
	fetchSubject(): Effect.Effect<Option.Option<MessageSubject>, UnknownProvider | SubjectFailed, Channels> {
		return Effect.flatMap(Channels, (channels) => channels.subject({ message: this }))
	}
}
