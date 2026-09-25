import { Effect, Option } from 'effect'

import type { GitHubReactionRequest } from '../GitHubApi'
import { GitHubApiClient } from './GitHubApiClient'
import { commentPath, commentRepository } from './GitHubApiProjections'
import { Reaction } from './GitHubApiSchemas'
export const removeReaction = Effect.fn('github.api.remove_reaction')(function* (input: GitHubReactionRequest) {
	const api = yield* GitHubApiClient
	const ref = commentRepository(input.comment)
	const path = `${commentPath(input.comment)}/reactions`
	const ownUserId = yield* api.botUserId
	const reactions = yield* api.list({ operation: 'remove_reaction', ref, path, schema: Reaction })
	const reactionId = Option.fromUndefinedOr(
		reactions.find((reaction) => reaction.content === input.reaction && reaction.user?.id === ownUserId)?.id,
	)
	if (Option.isNone(reactionId)) return
	yield* api.callVoid({ operation: 'remove_reaction', ref, method: 'DELETE', path: `${path}/${reactionId.value}` })
})
