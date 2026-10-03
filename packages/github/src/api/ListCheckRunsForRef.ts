import { Effect } from 'effect'

import type { GitHubListCheckRunsForRef } from '../GitHubApi'
import { GitHubApiClient } from './GitHubApiClient'
import { checkRun, repositoryPath } from './GitHubApiProjections'
import { CheckRunsPage } from './GitHubApiSchemas'
export const listCheckRunsForRef = Effect.fn('github.api.list_check_runs_for_ref')(function* (
	input: GitHubListCheckRunsForRef,
) {
	const api = yield* GitHubApiClient
	const values = yield* api.paginate({
		operation: 'list_check_runs_for_ref',
		ref: input.pullRequest,
		path: `${repositoryPath(input.pullRequest)}/commits/${encodeURIComponent(input.sha)}/check-runs`,
		query: [['filter', 'all']],
		schema: CheckRunsPage,
		items: (page) => page.check_runs,
	})
	return values.map((value) => checkRun(input.pullRequest, value))
})
