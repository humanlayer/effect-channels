import { Effect } from 'effect'

import type { GitHubIssueRequest } from '../GitHubApi'
import { GitHubApiClient } from './GitHubApiClient'
import { repositoryPath } from './GitHubApiProjections'
export const removeAllIssueLabels = Effect.fn('github.api.remove_all_issue_labels')(function* (
	input: GitHubIssueRequest,
) {
	const api = yield* GitHubApiClient
	yield* api.callVoid({
		operation: 'remove_all_issue_labels',
		ref: input.issue,
		method: 'DELETE',
		path: `${repositoryPath(input.issue)}/issues/${input.issue.number}/labels`,
	})
})
