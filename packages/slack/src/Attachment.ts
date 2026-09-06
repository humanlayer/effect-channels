import { Effect, Schema } from 'effect'

import type { FileReadFailed, UnknownTenant } from './DomainErrors.ts'
import type { FileData } from './Model.ts'
import { AttachmentRef } from './Model.ts'
import { Slack } from './Slack.ts'

export class Attachment extends Schema.TaggedClass<Attachment>()('Attachment', {
	ref: AttachmentRef,
}) {
	download(): Effect.Effect<FileData, UnknownTenant | FileReadFailed, Slack> {
		return Effect.flatMap(Slack, (slack) => slack.downloadAttachment({ attachment: this.ref }))
	}
}
