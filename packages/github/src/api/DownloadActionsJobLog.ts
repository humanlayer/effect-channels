import { Effect } from 'effect'
import * as FetchHttpClient from 'effect/http/FetchHttpClient'

import type { GitHubActionsJobRequest } from '../GitHubApi'
import { GitHubApiClient } from './GitHubApiClient'
import { repositoryPath } from './GitHubApiProjections'
export const downloadActionsJobLog = Effect.fn('github.api.download_actions_job_log')(function* (
	input: GitHubActionsJobRequest,
) {
	const api = yield* GitHubApiClient
	return yield* api
		.text({
			operation: 'download_actions_job_log',
			ref: input.job,
			method: 'GET',
			path: `${repositoryPath(input.job)}/actions/jobs/${input.job.id}/logs`,
			followRedirects: true,
		})
		.pipe(Effect.provideService(FetchHttpClient.RequestInit, { redirect: 'manual' }))
})
