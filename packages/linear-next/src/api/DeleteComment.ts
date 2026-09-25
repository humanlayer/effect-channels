import { Effect, Schema } from 'effect'

import type { LinearDeleteCommentRequest } from '../LinearApi'
import { failLinearMutation } from './LinearApiErrors'
import { linearGraphql } from './LinearGraphql'

const document = `mutation LinearCommentDelete($id: String!) { commentDelete(id: $id) { success } }`
const DeleteCommentResponse = Schema.Struct({ commentDelete: Schema.Struct({ success: Schema.Boolean }) })

export const deleteComment = Effect.fn('linear.api.delete_comment')((input: LinearDeleteCommentRequest) =>
	linearGraphql({
		operation: 'delete_comment',
		query: document,
		variables: { id: input.comment.commentId },
		response: DeleteCommentResponse,
	}).pipe(
		Effect.flatMap(({ commentDelete }) => {
			if (!commentDelete.success) return failLinearMutation('delete_comment')
			return Effect.void
		}),
	),
)
