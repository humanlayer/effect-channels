import { Effect } from 'effect'

import type { GitHubCheckRunRequest } from '../GitHubApi'
import { GitHubApiClient } from './GitHubApiClient'
import { checkAnnotation, repositoryPath } from './GitHubApiProjections'
import { CheckAnnotation } from './GitHubApiSchemas'
export const listCheckRunAnnotations = Effect.fn('github.api.list_check_run_annotations')(function* (
	input: GitHubCheckRunRequest,
) {
	const api = yield* GitHubApiClient
	const values = yield* api.list({
		operation: 'list_check_run_annotations',
		ref: input.checkRun,
		path: `${repositoryPath(input.checkRun)}/check-runs/${input.checkRun.id}/annotations`,
		schema: CheckAnnotation,
	})
	return values.map(checkAnnotation)
})
