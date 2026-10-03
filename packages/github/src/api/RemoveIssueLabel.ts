import { Effect, Schema } from 'effect'

import type { GitHubRemoveIssueLabel } from '../GitHubApi'
import { GitHubApiClient } from './GitHubApiClient'
import { label, repositoryPath } from './GitHubApiProjections'
import { Label } from './GitHubApiSchemas'
export const removeIssueLabel = Effect.fn('github.api.remove_issue_label')(function* (input: GitHubRemoveIssueLabel) {
	const api = yield* GitHubApiClient
	const values = yield* api.call({
		operation: 'remove_issue_label',
		ref: input.issue,
		method: 'DELETE',
		path:
			`${repositoryPath(input.issue)}/issues/${input.issue.number}/labels` +
			`/${encodeURIComponent(input.label)}`,
		schema: Schema.Array(Label),
	})
	return values.map(label)
})
