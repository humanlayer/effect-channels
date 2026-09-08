import { Schema } from 'effect'

import { Attachment } from './Attachment.js'
import { Author, MessageMetadata, MessageRef, ThreadRef } from './Model.js'

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
}) {}
