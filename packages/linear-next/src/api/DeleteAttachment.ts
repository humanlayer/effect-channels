import { Effect, Schema } from 'effect'

import type { LinearDeleteAttachmentRequest } from '../LinearApi'
import { LinearAttachmentId } from '../LinearIdentity'
import { failLinearMutation } from './LinearApiErrors'
import { linearGraphql } from './LinearGraphql'

const document = `mutation LinearAttachmentDelete($id: String!) { attachmentDelete(id: $id) { success } }`
export const DeleteAttachmentVariables = Schema.Struct({ id: LinearAttachmentId })
const DeleteAttachmentResponse = Schema.Struct({ attachmentDelete: Schema.Struct({ success: Schema.Boolean }) })

export const deleteAttachment = Effect.fn('linear.api.delete_attachment')((input: LinearDeleteAttachmentRequest) =>
	linearGraphql({
		operation: 'delete_attachment',
		query: document,
		variables: DeleteAttachmentVariables,
		input: { id: input.attachment.attachmentId },
		response: DeleteAttachmentResponse,
	}).pipe(
		Effect.flatMap(({ attachmentDelete }) => {
			if (!attachmentDelete.success) return failLinearMutation('delete_attachment')
			return Effect.void
		}),
	),
)
