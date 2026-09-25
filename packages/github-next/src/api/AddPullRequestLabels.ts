import { Effect, Schema } from 'effect'

import type { GitHubPullRequestLabelsRequest } from '../GitHubApi'
import { GitHubApiClient } from './GitHubApiClient'
import { label, repositoryPath } from './GitHubApiProjections'
import { Label } from './GitHubApiSchemas'
export const addPullRequestLabels = Effect.fn('github.api.add_pull_request_labels')(function* (
	input: GitHubPullRequestLabelsRequest,
) {
	const api = yield* GitHubApiClient
	const values = yield* api.call({
		operation: 'add_pull_request_labels',
		ref: input.pullRequest,
		method: 'POST',
		path: `${repositoryPath(input.pullRequest)}/issues/${input.pullRequest.number}/labels`,
		schema: Schema.Array(Label),
		body: { labels: input.labels },
	})
	return values.map(label)
})
