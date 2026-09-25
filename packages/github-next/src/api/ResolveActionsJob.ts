import { Effect, Predicate } from 'effect'

import type { GitHubCheckRunRequest } from '../GitHubApi'
import { fetchCheckRun } from './FetchCheckRun'
import { GitHubApiClient } from './GitHubApiClient'
import { actionsJob, repositoryPath, sameUrl } from './GitHubApiProjections'
import { ActionsJobsPage, WorkflowRunsPage } from './GitHubApiSchemas'
export const resolveActionsJob = Effect.fn('github.api.resolve_actions_job')(function* (input: GitHubCheckRunRequest) {
	const api = yield* GitHubApiClient
	const check = yield* fetchCheckRun(input)
	if (check.checkSuiteId === null) return null
	const runs = yield* api.paginate({
		operation: 'resolve_check_run_actions_job',
		ref: input.checkRun,
		path: `${repositoryPath(input.checkRun)}/actions/runs`,
		query: [['check_suite_id', String(check.checkSuiteId)]],
		schema: WorkflowRunsPage,
		items: (page) => page.workflow_runs,
	})
	for (const run of runs) {
		const jobs = yield* api.paginate({
			operation: 'resolve_check_run_actions_job',
			ref: input.checkRun,
			path: `${repositoryPath(input.checkRun)}/actions/runs/${run.id}/jobs`,
			query: [['filter', 'all']],
			schema: ActionsJobsPage,
			items: (page) => page.jobs,
		})
		const job = jobs.find((candidate) => sameUrl(candidate.check_run_url, check.apiUrl))
		if (Predicate.isNotUndefined(job)) return actionsJob(input.checkRun, job)
	}
	return null
})
