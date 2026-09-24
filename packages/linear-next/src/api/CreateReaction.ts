import { Effect, Schema } from 'effect'

import type { LinearCreateReactionRequest } from '../LinearApi'
import { LinearReaction } from '../LinearResources'
import { Reaction, participant, runGraphql } from './ProcedureSupport'

const document = `mutation LinearReactionCreate($input: ReactionCreateInput!) { reactionCreate(input: $input) { success reaction { id emoji user { id name email } } } }`
const Data = Schema.Struct({ reactionCreate: Schema.Struct({ success: Schema.Boolean, reaction: Reaction }) })
export const createReaction = Effect.fn('linear.api.create_reaction')((input: LinearCreateReactionRequest) => {
	const issue = input.target._tag === 'Issue' ? input.target.issue : input.target.comment
	return runGraphql(
		'create_reaction',
		document,
		{
			input: {
				emoji: input.emoji,
				...(input.target._tag === 'Issue'
					? { issueId: input.target.issue.issueId }
					: { commentId: input.target.comment.commentId }),
			},
		},
		Data,
	).pipe(
		Effect.map(({ reactionCreate }) =>
			LinearReaction.make({
				id: reactionCreate.reaction.id,
				issueId: issue.issueId,
				commentId: input.target._tag === 'Comment' ? input.target.comment.commentId : null,
				emoji: reactionCreate.reaction.emoji,
				author: participant(reactionCreate.reaction.user),
				ref: { issue, reactionId: reactionCreate.reaction.id },
			}),
		),
	)
})
