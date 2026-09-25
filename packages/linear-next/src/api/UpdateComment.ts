import { Effect, Schema } from 'effect'

import type { LinearUpdateCommentRequest } from '../LinearApi'
import { failLinearMutation } from './LinearApiErrors'
import { projectLinearComment } from './LinearApiProjections'
import { LinearApiComment } from './LinearApiSchemas'
import { linearGraphql } from './LinearGraphql'

const document = `mutation LinearCommentUpdate($id: String!, $input: CommentUpdateInput!) { commentUpdate(id: $id, input: $input) { success comment { id body parent { id } user { id name email } } } }`
const UpdateCommentResponse = Schema.Struct({
	commentUpdate: Schema.Struct({ success: Schema.Boolean, comment: LinearApiComment }),
})

export const updateComment = Effect.fn('linear.api.update_comment')((input: LinearUpdateCommentRequest) =>
	linearGraphql({
		operation: 'update_comment',
		query: document,
		variables: { id: input.comment.commentId, input: { body: input.content.markdown } },
		response: UpdateCommentResponse,
	}).pipe(
		Effect.flatMap(({ commentUpdate }) => {
			if (!commentUpdate.success) return failLinearMutation('update_comment')
			return Effect.succeed(projectLinearComment(input.comment, commentUpdate.comment))
		}),
	),
)
