import { Effect, Schema } from 'effect'

import type { LinearCreateCommentRequest } from '../LinearApi'
import { failLinearMutation } from './LinearApiErrors'
import { projectLinearComment } from './LinearApiProjections'
import { LinearApiComment } from './LinearApiSchemas'
import { linearGraphql } from './LinearGraphql'

const document = `mutation LinearCommentCreate($input: CommentCreateInput!) { commentCreate(input: $input) { success comment { id body parent { id } user { id name email } } } }`
const CreateCommentResponse = Schema.Struct({
	commentCreate: Schema.Struct({ success: Schema.Boolean, comment: LinearApiComment }),
})

type CreateCommentVariables = {
	readonly input: {
		readonly issueId: string
		readonly body: string
		parentId?: string
	}
}

export const createComment = Effect.fn('linear.api.create_comment')((input: LinearCreateCommentRequest) => {
	const variables: CreateCommentVariables = {
		input: { issueId: input.issue.issueId, body: input.content.markdown },
	}
	if (input.parentId !== undefined) variables.input.parentId = input.parentId
	return linearGraphql({
		operation: 'create_comment',
		query: document,
		variables,
		response: CreateCommentResponse,
	}).pipe(
		Effect.flatMap(({ commentCreate }) => {
			if (!commentCreate.success) return failLinearMutation('create_comment')
			return Effect.succeed(projectLinearComment(input.issue, commentCreate.comment))
		}),
	)
})
