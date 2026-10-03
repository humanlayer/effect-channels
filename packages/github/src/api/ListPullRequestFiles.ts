import { Effect } from 'effect'

import type { GitHubPullRequestRequest } from '../GitHubApi'
import { GitHubApiClient } from './GitHubApiClient'
import { pullRequestFile, repositoryPath } from './GitHubApiProjections'
import { PullRequestFile } from './GitHubApiSchemas'
export const listPullRequestFiles = Effect.fn('github.api.list_pull_request_files')(function* (
	input: GitHubPullRequestRequest,
) {
	const api = yield* GitHubApiClient
	const values = yield* api.list({
		operation: 'list_pull_request_files',
		ref: input.pullRequest,
		path: `${repositoryPath(input.pullRequest)}/pulls/${input.pullRequest.number}/files`,
		schema: PullRequestFile,
	})
	return values.map(pullRequestFile)
})
