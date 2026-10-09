import { Effect } from 'effect'

import type { GitHubListPullRequestsForBranch } from '../GitHubApi'
import { GitHubApiClient } from './GitHubApiClient'
import { pullRequestInfo, repositoryPath } from './GitHubApiProjections'
import { PullRequest } from './GitHubApiSchemas'

export const listPullRequestsForBranch = Effect.fn('github.api.list_pull_requests_for_branch')(function* (
	input: GitHubListPullRequestsForBranch,
) {
	const api = yield* GitHubApiClient
	const values = yield* api.list({
		operation: 'list_pull_requests_for_branch',
		ref: input.repository,
		path: `${repositoryPath(input.repository)}/pulls`,
		query: [
			['state', 'open'],
			['head', `${input.repository.owner}:${input.head}`],
		],
		schema: PullRequest,
	})
	return values.map((value) => pullRequestInfo({ ...input.repository, number: value.number }, value))
})
