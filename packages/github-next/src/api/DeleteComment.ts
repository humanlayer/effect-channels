import { Effect } from 'effect'

import type { GitHubDeleteComment } from '../GitHubApi'
import { GitHubApiClient } from './GitHubApiClient'
import { commentPath, commentRepository } from './GitHubApiProjections'
export const deleteComment = Effect.fn('github.api.delete_comment')(function* (input: GitHubDeleteComment) {
	const api = yield* GitHubApiClient
	yield* api.callVoid({
		operation: 'delete_comment',
		ref: commentRepository(input.comment),
		method: 'DELETE',
		path: commentPath(input.comment),
	})
})
