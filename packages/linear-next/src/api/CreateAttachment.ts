import { Effect, Schema } from 'effect'

import type { LinearCreateAttachmentRequest } from '../LinearApi'
import { failLinearMutation } from './LinearApiErrors'
import { projectLinearAttachment } from './LinearApiProjections'
import { LinearApiAttachment } from './LinearApiSchemas'
import { linearGraphql } from './LinearGraphql'

const document = `mutation LinearAttachmentCreate($input: AttachmentCreateInput!) { attachmentCreate(input: $input) { success attachment { id title subtitle url metadata } } }`
const CreateAttachmentResponse = Schema.Struct({
	attachmentCreate: Schema.Struct({ success: Schema.Boolean, attachment: LinearApiAttachment }),
})

export const createAttachment = Effect.fn('linear.api.create_attachment')((input: LinearCreateAttachmentRequest) =>
	linearGraphql({
		operation: 'create_attachment',
		query: document,
		variables: { input: { issueId: input.issue.issueId, ...input.input } },
		response: CreateAttachmentResponse,
	}).pipe(
		Effect.flatMap(({ attachmentCreate }) => {
			if (!attachmentCreate.success) return failLinearMutation('create_attachment')
			return Effect.succeed(projectLinearAttachment(input.issue, attachmentCreate.attachment))
		}),
	),
)
