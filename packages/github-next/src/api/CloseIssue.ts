import { Effect } from 'effect'

import type { GitHubCloseIssue } from '../GitHubApi'
import { GitHubApiClient } from './GitHubApiClient'
import { issueInfo, repositoryPath } from './GitHubApiProjections'
import { Issue } from './GitHubApiSchemas'
export const closeIssue = Effect.fn('github.api.close_issue')(function* (input: GitHubCloseIssue) {
	const api = yield* GitHubApiClient
	const value = yield* api.call({
		operation: 'close_issue',
		ref: input.issue,
		method: 'PATCH',
		path: `${repositoryPath(input.issue)}/issues/${input.issue.number}`,
		schema: Issue,
		body: { state: 'closed', state_reason: input.reason },
	})
	return issueInfo(input.issue, value)
})
