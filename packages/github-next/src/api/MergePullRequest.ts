import { Effect, Schema, Struct } from 'effect'

import type { GitHubMergePullRequest } from '../GitHubApi'
import { GitHubMergeMethod, GitHubMergeResult } from '../GitHubModels'
import { GitHubApiClient } from './GitHubApiClient'
import { repositoryPath } from './GitHubApiProjections'
import { MergeResult } from './GitHubApiSchemas'

/** GitHub builds its default commit title and message when `commit_title` or `commit_message` is omitted. */
const MergePullRequestBody = Schema.Struct({
	merge_method: GitHubMergeMethod,
	sha: Schema.NonEmptyString,
	commit_title: Schema.optionalKey(Schema.String),
	commit_message: Schema.optionalKey(Schema.String),
})

export const mergePullRequest = Effect.fn('github.api.merge_pull_request')(function* (input: GitHubMergePullRequest) {
	const api = yield* GitHubApiClient
	const value = yield* api.call({
		operation: 'merge_pull_request',
		ref: input.pullRequest,
		method: 'PUT',
		path: `${repositoryPath(input.pullRequest)}/pulls/${input.pullRequest.number}/merge`,
		schema: MergeResult,
		body: {
			schema: MergePullRequestBody,
			value: {
				merge_method: input.method,
				sha: input.expectedHeadSha,
				...Struct.renameKeys(Struct.pick(input, ['commitTitle', 'commitMessage']), {
					commitTitle: 'commit_title',
					commitMessage: 'commit_message',
				}),
			},
		},
	})
	return GitHubMergeResult.make(value)
})
