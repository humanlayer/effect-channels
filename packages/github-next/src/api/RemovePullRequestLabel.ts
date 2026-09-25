import { Effect, Schema } from 'effect'

import type { GitHubRemovePullRequestLabel } from '../GitHubApi'
import { GitHubApiClient } from './GitHubApiClient'
import { label, repositoryPath } from './GitHubApiProjections'
import { Label } from './GitHubApiSchemas'
export const removePullRequestLabel = Effect.fn('github.api.remove_pull_request_label')(function* (
	input: GitHubRemovePullRequestLabel,
) {
	const api = yield* GitHubApiClient
	const values = yield* api.call({
		operation: 'remove_pull_request_label',
		ref: input.pullRequest,
		method: 'DELETE',
		path:
			`${repositoryPath(input.pullRequest)}/issues/${input.pullRequest.number}/labels` +
			`/${encodeURIComponent(input.label)}`,
		schema: Schema.Array(Label),
	})
	return values.map(label)
})
