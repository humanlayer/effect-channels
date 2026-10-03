import { Effect } from 'effect'

import { GitHubApiError, type GitHubCheckRunRequest } from '../GitHubApi'
import { GitHubApiClient } from './GitHubApiClient'
import { checkRunInfo, repositoryPath } from './GitHubApiProjections'
import { CheckRun } from './GitHubApiSchemas'
export const fetchCheckRun = Effect.fn('github.api.fetch_check_run')(function* (input: GitHubCheckRunRequest) {
	const api = yield* GitHubApiClient
	const value = yield* api.call({
		operation: 'fetch_check_run',
		ref: input.checkRun,
		method: 'GET',
		path: `${repositoryPath(input.checkRun)}/check-runs/${input.checkRun.id}`,
		schema: CheckRun,
	})
	if (value.id !== input.checkRun.id)
		return yield* GitHubApiError.make({
			operation: 'fetch_check_run',
			reason: 'invalid_response',
			retryable: false,
		})
	return checkRunInfo(input.checkRun, value)
})
