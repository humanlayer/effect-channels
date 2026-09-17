import { Effect, Schema } from 'effect'

import type { FileReadFailed, UnknownTenant } from './DomainErrors'
import type { FileData } from './Model'
import { AttachmentRef } from './Model'
import { Slack } from './Slack'

export class Attachment extends Schema.TaggedClass<Attachment>()('Attachment', {
	ref: AttachmentRef,
}) {
	download(): Effect.Effect<FileData, UnknownTenant | FileReadFailed, Slack> {
		return Effect.flatMap(Slack, (slack) => slack.downloadAttachment({ attachment: this.ref }))
	}
}
