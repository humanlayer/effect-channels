import { Effect, Predicate, type Schema } from 'effect'

import type { GitHubMergePullRequest } from '../GitHubApi'
import { GitHubMergeResult } from '../GitHubModels'
import { GitHubApiClient } from './GitHubApiClient'
import { repositoryPath } from './GitHubApiProjections'
import { MergeResult } from './GitHubApiSchemas'
export const mergePullRequest = Effect.fn('github.api.merge_pull_request')(function* (input: GitHubMergePullRequest) {
	const api = yield* GitHubApiClient
	const body: Record<string, Schema.Json> = { merge_method: input.method, sha: input.expectedHeadSha }
	if (Predicate.isNotUndefined(input.commitTitle)) body.commit_title = input.commitTitle
	if (Predicate.isNotUndefined(input.commitMessage)) body.commit_message = input.commitMessage
	const value = yield* api.call({
		operation: 'merge_pull_request',
		ref: input.pullRequest,
		method: 'PUT',
		path: `${repositoryPath(input.pullRequest)}/pulls/${input.pullRequest.number}/merge`,
		schema: MergeResult,
		body,
	})
	return GitHubMergeResult.make(value)
})
