import { Effect, Schema } from 'effect'

import type { LinearDeleteAttachmentRequest } from '../LinearApi'
import { LinearProviderError } from './LinearApiErrors'
import { runGraphql } from './ProcedureSupport'

const document = `mutation LinearAttachmentDelete($id: String!) { attachmentDelete(id: $id) { success } }`
const Data = Schema.Struct({ attachmentDelete: Schema.Struct({ success: Schema.Boolean }) })
export const deleteAttachment = Effect.fn('linear.api.delete_attachment')((input: LinearDeleteAttachmentRequest) =>
	runGraphql('delete_attachment', document, { id: input.attachment.attachmentId }, Data).pipe(
		Effect.flatMap(({ attachmentDelete }) =>
			attachmentDelete.success
				? Effect.void
				: Effect.fail(
						LinearProviderError.make({
							operation: 'delete_attachment',
							reason: 'graphql',
							retryable: false,
						}),
					),
		),
	),
)
