import { Effect, Schema } from 'effect'

import type { FileReadFailed, UnknownTenant } from './DomainErrors.js'
import type { FileData } from './Model.js'
import { AttachmentRef } from './Model.js'
import { Slack } from './Slack.js'

export class Attachment extends Schema.TaggedClass<Attachment>()('Attachment', {
	ref: AttachmentRef,
}) {
	download(): Effect.Effect<FileData, UnknownTenant | FileReadFailed, Slack> {
		return Effect.flatMap(Slack, (slack) => slack.downloadAttachment({ attachment: this.ref }))
	}
}
