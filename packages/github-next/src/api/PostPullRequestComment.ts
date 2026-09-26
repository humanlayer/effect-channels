import { Effect, Schema } from 'effect'

import type { GitHubPostPullRequestComment } from '../GitHubApi'
import { GitHubDiscussionRef } from '../GitHubModels'
import { GitHubApiClient } from './GitHubApiClient'
import { issueComment, repositoryPath } from './GitHubApiProjections'
import { IssueComment } from './GitHubApiSchemas'

const PostPullRequestCommentBody = Schema.Struct({ body: Schema.String })

export const postPullRequestComment = Effect.fn('github.api.post_pull_request_comment')(function* (
	input: GitHubPostPullRequestComment,
) {
	const api = yield* GitHubApiClient
	const value = yield* api.call({
		operation: 'post_pull_request_comment',
		ref: input.pullRequest,
		method: 'POST',
		path: `${repositoryPath(input.pullRequest)}/issues/${input.pullRequest.number}/comments`,
		schema: IssueComment,
		body: { schema: PostPullRequestCommentBody, value: { body: input.content.markdown } },
	})
	return issueComment(GitHubDiscussionRef.cases.PullRequest.make({ ref: input.pullRequest }), value)
})
