import { Effect, Schema } from 'effect'

import { LinearCreateAttachmentInput, type LinearCreateAttachmentRequest } from '../LinearApi'
import { LinearIssueId } from '../LinearIdentity'
import { failLinearMutation } from './LinearApiErrors'
import { projectLinearAttachment } from './LinearApiProjections'
import { LinearApiAttachment } from './LinearApiSchemas'
import { linearGraphql } from './LinearGraphql'

const document = `mutation LinearAttachmentCreate($input: AttachmentCreateInput!) { attachmentCreate(input: $input) { success attachment { id title subtitle url metadata } } }`
export const CreateAttachmentVariables = Schema.Struct({
	input: Schema.Struct({ issueId: LinearIssueId, ...LinearCreateAttachmentInput.fields }),
})
const CreateAttachmentResponse = Schema.Struct({
	attachmentCreate: Schema.Struct({ success: Schema.Boolean, attachment: LinearApiAttachment }),
})

export const createAttachment = Effect.fn('linear.api.create_attachment')((input: LinearCreateAttachmentRequest) =>
	linearGraphql({
		operation: 'create_attachment',
		query: document,
		variables: CreateAttachmentVariables,
		input: { input: { issueId: input.issue.issueId, ...input.input } },
		response: CreateAttachmentResponse,
	}).pipe(
		Effect.flatMap(({ attachmentCreate }) => {
			if (!attachmentCreate.success) return failLinearMutation('create_attachment')
			return Effect.succeed(projectLinearAttachment(input.issue, attachmentCreate.attachment))
		}),
	),
)
