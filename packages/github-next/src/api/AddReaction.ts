import { Effect } from 'effect'

import type { GitHubReactionRequest } from '../GitHubApi'
import { GitHubApiClient } from './GitHubApiClient'
import { commentPath, commentRepository } from './GitHubApiProjections'
import { Reaction } from './GitHubApiSchemas'
export const addReaction = Effect.fn('github.api.add_reaction')(function* (input: GitHubReactionRequest) {
	const api = yield* GitHubApiClient
	const ref = commentRepository(input.comment)
	yield* api.call({
		operation: 'add_reaction',
		ref,
		method: 'POST',
		path: `${commentPath(input.comment)}/reactions`,
		schema: Reaction,
		body: { content: input.reaction },
	})
})
