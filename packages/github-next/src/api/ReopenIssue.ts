import { Effect } from 'effect'

import type { GitHubIssueRequest } from '../GitHubApi'
import { GitHubApiClient } from './GitHubApiClient'
import { issueInfo, repositoryPath } from './GitHubApiProjections'
import { Issue } from './GitHubApiSchemas'
export const reopenIssue = Effect.fn('github.api.reopen_issue')(function* (input: GitHubIssueRequest) {
	const api = yield* GitHubApiClient
	const value = yield* api.call({
		operation: 'reopen_issue',
		ref: input.issue,
		method: 'PATCH',
		path: `${repositoryPath(input.issue)}/issues/${input.issue.number}`,
		schema: Issue,
		body: { state: 'open', state_reason: 'reopened' },
	})
	return issueInfo(input.issue, value)
})
