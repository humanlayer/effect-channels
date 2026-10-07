import { Effect } from 'effect'

import type { GitHubRepositoryRequest } from '../GitHubApi'
import { GitHubApiClient } from './GitHubApiClient'
import { label, repositoryPath } from './GitHubApiProjections'
import { Label } from './GitHubApiSchemas'

export const listRepositoryLabels = Effect.fn('github.api.list_repository_labels')(function* (
	input: GitHubRepositoryRequest,
) {
	const api = yield* GitHubApiClient
	const values = yield* api.list({
		operation: 'list_repository_labels',
		ref: input.repository,
		path: `${repositoryPath(input.repository)}/labels`,
		schema: Label,
	})
	return values.map(label)
})
