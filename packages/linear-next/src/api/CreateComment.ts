import { Effect, Schema, Struct } from 'effect'

import type { LinearCreateCommentRequest } from '../LinearApi'
import { LinearCommentId, LinearIssueId } from '../LinearIdentity'
import { failLinearMutation } from './LinearApiErrors'
import { projectLinearComment } from './LinearApiProjections'
import { LinearApiComment } from './LinearApiSchemas'
import { linearGraphql } from './LinearGraphql'

const document = `mutation LinearCommentCreate($input: CommentCreateInput!) { commentCreate(input: $input) { success comment { id body parent { id } user { id name email } } } }`
const CreateCommentResponse = Schema.Struct({
	commentCreate: Schema.Struct({ success: Schema.Boolean, comment: LinearApiComment }),
})

export const CreateCommentVariables = Schema.Struct({
	input: Schema.Struct({
		issueId: LinearIssueId,
		body: Schema.String,
		parentId: Schema.optionalKey(LinearCommentId),
	}),
})

export const createComment = Effect.fn('linear.api.create_comment')((input: LinearCreateCommentRequest) =>
	linearGraphql({
		operation: 'create_comment',
		query: document,
		variables: CreateCommentVariables,
		input: {
			input: { issueId: input.issue.issueId, body: input.content.markdown, ...Struct.pick(input, ['parentId']) },
		},
		response: CreateCommentResponse,
	}).pipe(
		Effect.flatMap(({ commentCreate }) => {
			if (!commentCreate.success) return failLinearMutation('create_comment')
			return Effect.succeed(projectLinearComment(input.issue, commentCreate.comment))
		}),
	),
)
