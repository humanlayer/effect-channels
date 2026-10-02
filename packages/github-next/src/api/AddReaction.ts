import { Effect, Schema } from 'effect'

import type { GitHubReactionRequest } from '../GitHubApi'
import { GitHubReaction } from '../GitHubModels'
import { GitHubApiClient } from './GitHubApiClient'
import { reactionsLocation } from './GitHubApiProjections'
import { Reaction } from './GitHubApiSchemas'

const AddReactionBody = Schema.Struct({ content: GitHubReaction })

/** React to a comment, issue, or pull request. GitHub answers 200 when the bot already has this reaction. */
export const addReaction = Effect.fn('github.api.add_reaction')(function* (input: GitHubReactionRequest) {
	const api = yield* GitHubApiClient
	const { ref, path } = reactionsLocation(input.target)
	yield* api.call({
		operation: 'add_reaction',
		ref,
		method: 'POST',
		path,
		schema: Reaction,
		body: { schema: AddReactionBody, value: { content: input.reaction } },
	})
})
