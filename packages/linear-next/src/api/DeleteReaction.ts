import { Effect, Schema } from 'effect'

import type { LinearDeleteReactionRequest } from '../LinearApi'
import { LinearProviderError } from './LinearApiErrors'
import { runGraphql } from './ProcedureSupport'

const document = `mutation LinearReactionDelete($id: String!) { reactionDelete(id: $id) { success } }`
const Data = Schema.Struct({ reactionDelete: Schema.Struct({ success: Schema.Boolean }) })
export const deleteReaction = Effect.fn('linear.api.delete_reaction')((input: LinearDeleteReactionRequest) =>
	runGraphql('delete_reaction', document, { id: input.reactionId }, Data).pipe(
		Effect.flatMap(({ reactionDelete }) =>
			reactionDelete.success
				? Effect.void
				: Effect.fail(
						LinearProviderError.make({
							operation: 'delete_reaction',
							reason: 'graphql',
							retryable: false,
						}),
					),
		),
	),
)
