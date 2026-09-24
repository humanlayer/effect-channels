import { Effect, Schema } from 'effect'

import type { LinearDeleteCommentRequest } from '../LinearApi'
import { LinearProviderError } from './LinearApiErrors'
import { runGraphql } from './ProcedureSupport'

const document = `mutation LinearCommentDelete($id: String!) { commentDelete(id: $id) { success } }`
const Data = Schema.Struct({ commentDelete: Schema.Struct({ success: Schema.Boolean }) })
export const deleteComment = Effect.fn('linear.api.delete_comment')((input: LinearDeleteCommentRequest) =>
	runGraphql('delete_comment', document, { id: input.comment.commentId }, Data).pipe(
		Effect.flatMap(({ commentDelete }) =>
			commentDelete.success
				? Effect.void
				: Effect.fail(
						LinearProviderError.make({
							operation: 'delete_comment',
							reason: 'graphql',
							retryable: false,
						}),
					),
		),
	),
)
