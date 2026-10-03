import { Effect } from 'effect'

import type { GitHubIssueRequest } from '../GitHubApi'
import { GitHubApiClient } from './GitHubApiClient'
import { label, repositoryPath } from './GitHubApiProjections'
import { Label } from './GitHubApiSchemas'
export const listIssueLabels = Effect.fn('github.api.list_issue_labels')(function* (input: GitHubIssueRequest) {
	const api = yield* GitHubApiClient
	const values = yield* api.list({
		operation: 'list_issue_labels',
		ref: input.issue,
		path: `${repositoryPath(input.issue)}/issues/${input.issue.number}/labels`,
		schema: Label,
	})
	return values.map(label)
})
