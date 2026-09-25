import { Effect } from 'effect'

import type { GitHubPullRequestRequest } from '../GitHubApi'
import { GitHubApiClient } from './GitHubApiClient'
import { issueComment, repositoryPath } from './GitHubApiProjections'
import { IssueComment } from './GitHubApiSchemas'
export const listPullRequestComments = Effect.fn('github.api.list_pull_request_comments')(function* (
	input: GitHubPullRequestRequest,
) {
	const api = yield* GitHubApiClient
	const values = yield* api.list({
		operation: 'list_pull_request_comments',
		ref: input.pullRequest,
		path: `${repositoryPath(input.pullRequest)}/issues/${input.pullRequest.number}/comments`,
		schema: IssueComment,
	})
	const discussion = { _tag: 'PullRequest', ref: input.pullRequest } as const
	return values.map((value) => issueComment(discussion, value))
})
