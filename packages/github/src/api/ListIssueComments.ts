import { Effect } from 'effect'

import type { GitHubIssueRequest } from '../GitHubApi'
import { GitHubDiscussionRef } from '../GitHubModels'
import { GitHubApiClient } from './GitHubApiClient'
import { issueComment, repositoryPath } from './GitHubApiProjections'
import { IssueComment } from './GitHubApiSchemas'
export const listIssueComments = Effect.fn('github.api.list_issue_comments')(function* (input: GitHubIssueRequest) {
	const api = yield* GitHubApiClient
	const values = yield* api.list({
		operation: 'list_issue_comments',
		ref: input.issue,
		path: `${repositoryPath(input.issue)}/issues/${input.issue.number}/comments`,
		schema: IssueComment,
	})
	const discussion = GitHubDiscussionRef.cases.Issue.make({ ref: input.issue })
	return values.map((value) => issueComment(discussion, value))
})
