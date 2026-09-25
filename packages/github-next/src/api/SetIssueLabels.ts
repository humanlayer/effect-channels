import { Effect, Schema } from 'effect'

import type { GitHubIssueLabelsRequest } from '../GitHubApi'
import { GitHubApiClient } from './GitHubApiClient'
import { label, repositoryPath } from './GitHubApiProjections'
import { Label } from './GitHubApiSchemas'
export const setIssueLabels = Effect.fn('github.api.set_issue_labels')(function* (input: GitHubIssueLabelsRequest) {
	const api = yield* GitHubApiClient
	const values = yield* api.call({
		operation: 'set_issue_labels',
		ref: input.issue,
		method: 'PUT',
		path: `${repositoryPath(input.issue)}/issues/${input.issue.number}/labels`,
		schema: Schema.Array(Label),
		body: { labels: input.labels },
	})
	return values.map(label)
})
