import { Effect, Schema } from 'effect'

import type { LinearApiError, LinearIssueRequest } from '../LinearApi'
import { LinearIssueId } from '../LinearIdentity'
import type { LinearComment } from '../LinearResources'
import { type LinearProviderError, LinearResponseDecodeError, linearProviderErrorDetails } from './LinearApiErrors'
import { projectLinearComment } from './LinearApiProjections'
import { LinearApiComment, LinearApiPageInfo } from './LinearApiSchemas'
import { linearGraphql } from './LinearGraphql'
import type { LinearHttpClient } from './LinearHttpClient'

const document = `query LinearIssueComments($id: String!, $after: String) { issue(id: $id) { comments(first: 100, after: $after) { nodes { id body parent { id } user { id name email } } pageInfo { endCursor hasNextPage } } } }`
export const ListIssueCommentsVariables = Schema.Struct({ id: LinearIssueId, after: Schema.optionalKey(Schema.String) })
const ListIssueCommentsResponse = Schema.Struct({
	issue: Schema.Struct({
		comments: Schema.Struct({ nodes: Schema.Array(LinearApiComment), pageInfo: LinearApiPageInfo }),
	}),
})

export const listIssueComments = Effect.fn('linear.api.list_issue_comments')((input: LinearIssueRequest) => {
	const page = (
		variables: typeof ListIssueCommentsVariables.Type,
		accumulated: ReadonlyArray<LinearComment>,
	): Effect.Effect<ReadonlyArray<LinearComment>, LinearProviderError | LinearApiError, LinearHttpClient> =>
		linearGraphql({
			operation: 'list_issue_comments',
			query: document,
			variables: ListIssueCommentsVariables,
			input: variables,
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
				return page({ id: input.issue.issueId, after: issue.comments.pageInfo.endCursor }, values)
			}),
		)
	return page({ id: input.issue.issueId }, [])
})
