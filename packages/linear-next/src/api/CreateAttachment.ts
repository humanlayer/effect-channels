import { Effect, Schema } from 'effect'

import type { LinearCreateAttachmentRequest } from '../LinearApi'
import { Attachment, attachment, runGraphql } from './ProcedureSupport'

const document = `mutation LinearAttachmentCreate($input: AttachmentCreateInput!) { attachmentCreate(input: $input) { success attachment { id title subtitle url metadata } } }`
const Data = Schema.Struct({ attachmentCreate: Schema.Struct({ success: Schema.Boolean, attachment: Attachment }) })
export const createAttachment = Effect.fn('linear.api.create_attachment')((input: LinearCreateAttachmentRequest) =>
	runGraphql('create_attachment', document, { input: { issueId: input.issue.issueId, ...input.input } }, Data).pipe(
		Effect.map(({ attachmentCreate }) => attachment(input.issue, attachmentCreate.attachment)),
	),
)
