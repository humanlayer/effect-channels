import { Effect, Schema } from 'effect'

import { Channels } from './Channels.ts'
import type { FileReadFailed, UnknownProvider, UnknownTenant } from './Errors.ts'
import type { FileData } from './Schema.ts'
import { AttachmentRef } from './Schema.ts'

export class Attachment extends Schema.TaggedClass<Attachment>()('Attachment', {
	ref: AttachmentRef,
}) {
	download(): Effect.Effect<FileData, UnknownProvider | UnknownTenant | FileReadFailed, Channels> {
		return Effect.flatMap(Channels, (channels) => channels.downloadAttachment({ attachment: this.ref }))
	}
}
