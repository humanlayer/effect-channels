import { Effect } from 'effect'

import type { GitHubPullRequestRequest } from '../GitHubApi'
import { GitHubApiClient } from './GitHubApiClient'
import { commit, repositoryPath } from './GitHubApiProjections'
import { Commit } from './GitHubApiSchemas'
export const listPullRequestCommits = Effect.fn('github.api.list_pull_request_commits')(function* (
	input: GitHubPullRequestRequest,
) {
	const api = yield* GitHubApiClient
	const values = yield* api.list({
		operation: 'list_pull_request_commits',
		ref: input.pullRequest,
		path: `${repositoryPath(input.pullRequest)}/pulls/${input.pullRequest.number}/commits`,
		schema: Commit,
	})
	return values.map(commit)
})
