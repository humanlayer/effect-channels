import { Effect, Schema } from 'effect'

import type { LinearDeleteReactionRequest } from '../LinearApi'
import { LinearReactionId } from '../LinearIdentity'
import { failLinearMutation } from './LinearApiErrors'
import { linearGraphql } from './LinearGraphql'

const document = `mutation LinearReactionDelete($id: String!) { reactionDelete(id: $id) { success } }`
export const DeleteReactionVariables = Schema.Struct({ id: LinearReactionId })
const DeleteReactionResponse = Schema.Struct({ reactionDelete: Schema.Struct({ success: Schema.Boolean }) })

export const deleteReaction = Effect.fn('linear.api.delete_reaction')((input: LinearDeleteReactionRequest) =>
	linearGraphql({
		operation: 'delete_reaction',
		query: document,
		variables: DeleteReactionVariables,
		input: { id: input.reactionId },
		response: DeleteReactionResponse,
	}).pipe(
		Effect.flatMap(({ reactionDelete }) => {
			if (!reactionDelete.success) return failLinearMutation('delete_reaction')
			return Effect.void
		}),
	),
)
