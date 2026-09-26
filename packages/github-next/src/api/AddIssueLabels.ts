import { Effect, Schema } from 'effect'

import { GitHubLabels, type GitHubIssueLabelsRequest } from '../GitHubApi'
import { GitHubApiClient } from './GitHubApiClient'
import { label, repositoryPath } from './GitHubApiProjections'
import { Label } from './GitHubApiSchemas'

const AddIssueLabelsBody = Schema.Struct({ labels: GitHubLabels })

export const addIssueLabels = Effect.fn('github.api.add_issue_labels')(function* (input: GitHubIssueLabelsRequest) {
	const api = yield* GitHubApiClient
	const values = yield* api.call({
		operation: 'add_issue_labels',
		ref: input.issue,
		method: 'POST',
		path: `${repositoryPath(input.issue)}/issues/${input.issue.number}/labels`,
		schema: Schema.Array(Label),
		body: { schema: AddIssueLabelsBody, value: { labels: input.labels } },
	})
	return values.map(label)
})
