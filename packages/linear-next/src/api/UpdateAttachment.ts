import { Effect, Schema } from 'effect'

import { type LinearUpdateAttachmentRequest, LinearUpdateAttachmentInput } from '../LinearApi'
import { LinearAttachmentId } from '../LinearIdentity'
import { failLinearMutation } from './LinearApiErrors'
import { projectLinearAttachment } from './LinearApiProjections'
import { LinearApiAttachment } from './LinearApiSchemas'
import { linearGraphql } from './LinearGraphql'

const document = `mutation LinearAttachmentUpdate($id: String!, $input: AttachmentUpdateInput!) { attachmentUpdate(id: $id, input: $input) { success attachment { id title subtitle url metadata } } }`
export const UpdateAttachmentVariables = Schema.Struct({ id: LinearAttachmentId, input: LinearUpdateAttachmentInput })
const UpdateAttachmentResponse = Schema.Struct({
	attachmentUpdate: Schema.Struct({ success: Schema.Boolean, attachment: LinearApiAttachment }),
})

export const updateAttachment = Effect.fn('linear.api.update_attachment')((input: LinearUpdateAttachmentRequest) =>
	linearGraphql({
		operation: 'update_attachment',
		query: document,
		variables: UpdateAttachmentVariables,
		input: { id: input.attachment.attachmentId, input: input.input },
		response: UpdateAttachmentResponse,
	}).pipe(
		Effect.flatMap(({ attachmentUpdate }) => {
			if (!attachmentUpdate.success) return failLinearMutation('update_attachment')
			return Effect.succeed(projectLinearAttachment(input.attachment.issue, attachmentUpdate.attachment))
		}),
	),
)
