import { Effect, Schema } from 'effect'

import { type GitHubCloseIssue, GitHubIssueCloseReason } from '../GitHubApi'
import { GitHubApiClient } from './GitHubApiClient'
import { issueInfo, repositoryPath } from './GitHubApiProjections'
import { Issue } from './GitHubApiSchemas'

const CloseIssueBody = Schema.Struct({ state: Schema.Literal('closed'), state_reason: GitHubIssueCloseReason })

export const closeIssue = Effect.fn('github.api.close_issue')(function* (input: GitHubCloseIssue) {
	const api = yield* GitHubApiClient
	const value = yield* api.call({
		operation: 'close_issue',
		ref: input.issue,
		method: 'PATCH',
		path: `${repositoryPath(input.issue)}/issues/${input.issue.number}`,
		schema: Issue,
		body: { schema: CloseIssueBody, value: { state: 'closed', state_reason: input.reason } },
	})
	return issueInfo(input.issue, value)
})
