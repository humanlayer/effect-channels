import { Effect, Predicate, Schema } from 'effect'

import type { LinearCreateCommentRequest } from '../LinearApi'
import { Comment, comment, runGraphql } from './ProcedureSupport'

const document = `mutation LinearCommentCreate($input: CommentCreateInput!) { commentCreate(input: $input) { success comment { id body parent { id } user { id name email } } } }`
const Data = Schema.Struct({ commentCreate: Schema.Struct({ success: Schema.Boolean, comment: Comment }) })
export const createComment = Effect.fn('linear.api.create_comment')((input: LinearCreateCommentRequest) =>
	runGraphql(
		'create_comment',
		document,
		{
			input: {
				issueId: input.issue.issueId,
				body: input.content.markdown,
				...(Predicate.isUndefined(input.parentId) ? {} : { parentId: input.parentId }),
			},
		},
		Data,
	).pipe(Effect.map(({ commentCreate }) => comment(input.issue, commentCreate.comment))),
)
