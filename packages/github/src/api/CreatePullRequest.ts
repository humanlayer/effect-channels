import { Effect, Schema } from 'effect'

import type { GitHubCreatePullRequest } from '../GitHubApi'
import { GitHubApiClient } from './GitHubApiClient'
import { pullRequestInfo, repositoryPath } from './GitHubApiProjections'
import { PullRequest } from './GitHubApiSchemas'

const CreatePullRequestBody = Schema.Struct({
	title: Schema.String,
	body: Schema.String,
	head: Schema.String,
	base: Schema.String,
})

export const createPullRequest = Effect.fn('github.api.create_pull_request')(function* (
	input: GitHubCreatePullRequest,
) {
	const api = yield* GitHubApiClient
	const value = yield* api.call({
		operation: 'create_pull_request',
		ref: input.repository,
		method: 'POST',
		path: `${repositoryPath(input.repository)}/pulls`,
		schema: PullRequest,
		body: {
			schema: CreatePullRequestBody,
			value: { title: input.title, body: input.body, head: input.head, base: input.base },
		},
	})
	return pullRequestInfo({ ...input.repository, number: value.number }, value)
})
