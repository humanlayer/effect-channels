import { Effect, Match, Schema } from 'effect'

import { LinearCreateReactionRequest } from '../LinearApi'
import { LinearCommentId, LinearIssueId } from '../LinearIdentity'
import { LinearReaction } from '../LinearResources'
import { failLinearMutation } from './LinearApiErrors'
import { projectLinearParticipant } from './LinearApiProjections'
import { LinearApiReaction } from './LinearApiSchemas'
import { linearGraphql } from './LinearGraphql'

const document = `mutation LinearReactionCreate($input: ReactionCreateInput!) { reactionCreate(input: $input) { success reaction { id emoji user { id name email } } } }`
const emoji = LinearCreateReactionRequest.fields.emoji
export const CreateReactionVariables = Schema.Struct({
	input: Schema.Union([
		Schema.Struct({ emoji, issueId: LinearIssueId }),
		Schema.Struct({ emoji, commentId: LinearCommentId }),
	]),
})
const CreateReactionResponse = Schema.Struct({
	reactionCreate: Schema.Struct({ success: Schema.Boolean, reaction: LinearApiReaction }),
})

export const createReaction = Effect.fn('linear.api.create_reaction')((input: LinearCreateReactionRequest) => {
	const target = Match.value(input.target).pipe(
		Match.tagsExhaustive({
			Issue: ({ issue }) => ({
				issue,
				commentId: null,
				variables: { emoji: input.emoji, issueId: issue.issueId },
			}),
			Comment: ({ comment }) => ({
				issue: comment,
				commentId: comment.commentId,
				variables: { emoji: input.emoji, commentId: comment.commentId },
			}),
		}),
	)
	return linearGraphql({
		operation: 'create_reaction',
		query: document,
		variables: CreateReactionVariables,
		input: { input: target.variables },
		response: CreateReactionResponse,
	}).pipe(
		Effect.flatMap(({ reactionCreate }) => {
			if (!reactionCreate.success) return failLinearMutation('create_reaction')
			return Effect.succeed(
				LinearReaction.make({
					id: reactionCreate.reaction.id,
					issueId: target.issue.issueId,
					commentId: target.commentId,
					emoji: reactionCreate.reaction.emoji,
					author: projectLinearParticipant(reactionCreate.reaction.user),
					ref: { issue: target.issue, reactionId: reactionCreate.reaction.id },
				}),
			)
		}),
	)
})
