import { Effect, Schema } from 'effect'

import type { GitHubIssueRequest } from '../GitHubApi'
import { GitHubApiClient } from './GitHubApiClient'
import { issueInfo, repositoryPath } from './GitHubApiProjections'
import { Issue } from './GitHubApiSchemas'

const ReopenIssueBody = Schema.Struct({ state: Schema.Literal('open'), state_reason: Schema.Literal('reopened') })

export const reopenIssue = Effect.fn('github.api.reopen_issue')(function* (input: GitHubIssueRequest) {
	const api = yield* GitHubApiClient
	const value = yield* api.call({
		operation: 'reopen_issue',
		ref: input.issue,
		method: 'PATCH',
		path: `${repositoryPath(input.issue)}/issues/${input.issue.number}`,
		schema: Issue,
		body: { schema: ReopenIssueBody, value: { state: 'open', state_reason: 'reopened' } },
	})
	return issueInfo(input.issue, value)
})
