import { Effect } from 'effect'

import type { GitHubPullRequestRequest } from '../GitHubApi'
import { GitHubApiClient } from './GitHubApiClient'
import { label, repositoryPath } from './GitHubApiProjections'
import { Label } from './GitHubApiSchemas'
export const listPullRequestLabels = Effect.fn('github.api.list_pull_request_labels')(function* (
	input: GitHubPullRequestRequest,
) {
	const api = yield* GitHubApiClient
	const values = yield* api.list({
		operation: 'list_pull_request_labels',
		ref: input.pullRequest,
		path: `${repositoryPath(input.pullRequest)}/issues/${input.pullRequest.number}/labels`,
		schema: Label,
	})
	return values.map(label)
})
