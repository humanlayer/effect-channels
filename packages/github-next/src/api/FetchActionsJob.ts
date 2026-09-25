import { Effect } from 'effect'

import { GitHubApiError, type GitHubActionsJobRequest } from '../GitHubApi'
import { GitHubApiClient } from './GitHubApiClient'
import { actionsJobInfo, repositoryPath } from './GitHubApiProjections'
import { ActionsJob } from './GitHubApiSchemas'
export const fetchActionsJob = Effect.fn('github.api.fetch_actions_job')(function* (input: GitHubActionsJobRequest) {
	const api = yield* GitHubApiClient
	const value = yield* api.call({
		operation: 'fetch_actions_job',
		ref: input.job,
		method: 'GET',
		path: `${repositoryPath(input.job)}/actions/jobs/${input.job.id}`,
		schema: ActionsJob,
	})
	if (value.id !== input.job.id)
		return yield* GitHubApiError.make({
			operation: 'fetch_actions_job',
			reason: 'invalid_response',
			retryable: false,
		})
	return actionsJobInfo(input.job, value)
})
