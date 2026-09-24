import { Effect, Schema } from 'effect'

import type { LinearUpdateCommentRequest } from '../LinearApi'
import { Comment, comment, runGraphql } from './ProcedureSupport'

const document = `mutation LinearCommentUpdate($id: String!, $input: CommentUpdateInput!) { commentUpdate(id: $id, input: $input) { success comment { id body parent { id } user { id name email } } } }`
const Data = Schema.Struct({ commentUpdate: Schema.Struct({ success: Schema.Boolean, comment: Comment }) })
export const updateComment = Effect.fn('linear.api.update_comment')((input: LinearUpdateCommentRequest) =>
	runGraphql(
		'update_comment',
		document,
		{ id: input.comment.commentId, input: { body: input.content.markdown } },
		Data,
	).pipe(Effect.map(({ commentUpdate }) => comment(input.comment, commentUpdate.comment))),
)
