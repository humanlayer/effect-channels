import { Array as Arr, Effect, Option } from 'effect'

import type { GitHubReactionRequest } from '../GitHubApi'
import { GitHubApiClient } from './GitHubApiClient'
import { reactionsLocation } from './GitHubApiProjections'
import { Reaction } from './GitHubApiSchemas'

/** Remove the bot's own reaction from a comment, issue, or pull request. Nothing to remove counts as done. */
export const removeReaction = Effect.fn('github.api.remove_reaction')(function* (input: GitHubReactionRequest) {
	const api = yield* GitHubApiClient
	const { ref, path } = reactionsLocation(input.target)
	const ownUserId = yield* api.botUserId
	const reactions = yield* api.list({ operation: 'remove_reaction', ref, path, schema: Reaction })
	const own = Arr.findFirst(reactions, (reaction) => reaction.content === input.reaction && reaction.user?.id === ownUserId)
	yield* Option.match(own, {
		onNone: () => Effect.void,
		onSome: ({ id }) => api.callVoid({ operation: 'remove_reaction', ref, method: 'DELETE', path: `${path}/${id}` }),
	})
})
