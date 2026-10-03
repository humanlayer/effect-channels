import { Effect, Schema } from 'effect'

import type { GitHubPostIssueComment } from '../GitHubApi'
import { GitHubDiscussionRef } from '../GitHubModels'
import { GitHubApiClient } from './GitHubApiClient'
import { issueComment, repositoryPath } from './GitHubApiProjections'
import { IssueComment } from './GitHubApiSchemas'

export const PostIssueCommentBody = Schema.Struct({ body: Schema.String })

export const postIssueComment = Effect.fn('github.api.post_issue_comment')(function* (input: GitHubPostIssueComment) {
	const api = yield* GitHubApiClient
	const value = yield* api.call({
		operation: 'post_issue_comment',
		ref: input.issue,
		method: 'POST',
		path: `${repositoryPath(input.issue)}/issues/${input.issue.number}/comments`,
		schema: IssueComment,
		body: { schema: PostIssueCommentBody, value: { body: input.content.markdown } },
	})
	return issueComment(GitHubDiscussionRef.cases.Issue.make({ ref: input.issue }), value)
})
