import { Effect, Schema } from 'effect'

import type { LinearUpdateAttachmentRequest } from '../LinearApi'
import { Attachment, attachment, runGraphql } from './ProcedureSupport'

const document = `mutation LinearAttachmentUpdate($id: String!, $input: AttachmentUpdateInput!) { attachmentUpdate(id: $id, input: $input) { success attachment { id title subtitle url metadata } } }`
const Data = Schema.Struct({ attachmentUpdate: Schema.Struct({ success: Schema.Boolean, attachment: Attachment }) })
export const updateAttachment = Effect.fn('linear.api.update_attachment')((input: LinearUpdateAttachmentRequest) =>
	runGraphql('update_attachment', document, { id: input.attachment.attachmentId, input: input.input }, Data).pipe(
		Effect.map(({ attachmentUpdate }) => attachment(input.attachment.issue, attachmentUpdate.attachment)),
	),
)
