import { Effect } from 'effect'

import type { GitHubUserAccessRequest } from '../GitHubApi'
import { GitHubApiClient } from './GitHubApiClient'
import { accessLevel, repositoryPath } from './GitHubApiProjections'
import { CollaboratorPermission } from './GitHubApiSchemas'

/** A user's access to a repository, from GitHub's collaborator permission API. */
export const fetchUserAccess = Effect.fn('github.api.fetch_user_access')(function* (input: GitHubUserAccessRequest) {
	const api = yield* GitHubApiClient
	const value = yield* api.call({
		operation: 'fetch_user_access',
		ref: input.repository,
		method: 'GET',
		path: `${repositoryPath(input.repository)}/collaborators/${encodeURIComponent(input.login)}/permission`,
		schema: CollaboratorPermission,
	})
	return accessLevel(value)
})
