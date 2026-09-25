import { Effect, Schema } from 'effect'

import type { LinearApiError, LinearIssueRequest } from '../LinearApi'
import type { LinearComment } from '../LinearResources'
import { type LinearProviderError, LinearResponseDecodeError, linearProviderErrorDetails } from './LinearApiErrors'
import { projectLinearComment } from './LinearApiProjections'
import { LinearApiComment, LinearApiPageInfo } from './LinearApiSchemas'
import { linearGraphql } from './LinearGraphql'
import type { LinearHttpClient } from './LinearHttpClient'

const document = `query LinearIssueComments($id: String!, $after: String) { issue(id: $id) { comments(first: 100, after: $after) { nodes { id body parent { id } user { id name email } } pageInfo { endCursor hasNextPage } } } }`
const ListIssueCommentsResponse = Schema.Struct({
	issue: Schema.Struct({
		comments: Schema.Struct({ nodes: Schema.Array(LinearApiComment), pageInfo: LinearApiPageInfo }),
	}),
})

type ListIssueCommentsVariables = {
	readonly id: string
	after?: string
}

export const listIssueComments = Effect.fn('linear.api.list_issue_comments')((input: LinearIssueRequest) => {
	const page = (
		after: string | null,
		accumulated: ReadonlyArray<LinearComment>,
	): Effect.Effect<ReadonlyArray<LinearComment>, LinearProviderError | LinearApiError, LinearHttpClient> => {
		const variables: ListIssueCommentsVariables = { id: input.issue.issueId }
		if (after !== null) variables.after = after
		return linearGraphql({
			operation: 'list_issue_comments',
			query: document,
			variables,
			response: ListIssueCommentsResponse,
		}).pipe(
			Effect.flatMap(({ issue }) => {
				const values = accumulated.concat(
					issue.comments.nodes.map((value) => projectLinearComment(input.issue, value)),
				)
				if (!issue.comments.pageInfo.hasNextPage) return Effect.succeed(values)
				if (issue.comments.pageInfo.endCursor === null) {
					return Effect.fail(new LinearResponseDecodeError(linearProviderErrorDetails('list_issue_comments')))
				}
				return page(issue.comments.pageInfo.endCursor, values)
			}),
		)
	}
	return page(null, [])
})
