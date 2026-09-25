import { Effect } from 'effect'

import type { GitHubIssueRequest } from '../GitHubApi'
import { GitHubApiError } from '../GitHubApi'
import { GitHubApiClient } from './GitHubApiClient'
import { issueInfo, repositoryPath } from './GitHubApiProjections'
import { Issue } from './GitHubApiSchemas'
export const fetchIssue = Effect.fn('github.api.fetch_issue')(function* (input: GitHubIssueRequest) {
	const api = yield* GitHubApiClient
	const value = yield* api.call({
		operation: 'fetch_issue',
		ref: input.issue,
		method: 'GET',
		path: `${repositoryPath(input.issue)}/issues/${input.issue.number}`,
		schema: Issue,
	})
	if (value.number !== input.issue.number)
		return yield* GitHubApiError.make({ operation: 'fetch_issue', reason: 'invalid_response', retryable: false })
	return issueInfo(input.issue, value)
})
