import { Effect, Schema } from 'effect'
import type * as HttpClient from 'effect/unstable/http/HttpClient'

import type { LinearIssueRequest } from '../LinearApi'
import type { LinearComment } from '../LinearResources'
import { LinearProviderError } from './LinearApiErrors'
import { Comment, PageInfo, comment, runGraphql } from './ProcedureSupport'

const document = `query LinearIssueComments($id: String!, $after: String) { issue(id: $id) { comments(first: 100, after: $after) { nodes { id body parent { id } user { id name email } } pageInfo { endCursor hasNextPage } } } }`
const Data = Schema.Struct({
	issue: Schema.Struct({ comments: Schema.Struct({ nodes: Schema.Array(Comment), pageInfo: PageInfo }) }),
})
export const listIssueComments = Effect.fn('linear.api.list_issue_comments')((input: LinearIssueRequest) => {
	const page = (
		after: string | null,
		accumulated: ReadonlyArray<LinearComment>,
	): Effect.Effect<ReadonlyArray<LinearComment>, LinearProviderError, HttpClient.HttpClient> =>
		runGraphql(
			'list_issue_comments',
			document,
			{ id: input.issue.issueId, ...(after === null ? {} : { after }) },
			Data,
		).pipe(
			Effect.flatMap(({ issue }) => {
				const values = [...accumulated, ...issue.comments.nodes.map((value) => comment(input.issue, value))]
				if (!issue.comments.pageInfo.hasNextPage) return Effect.succeed(values)
				if (issue.comments.pageInfo.endCursor === null)
					return Effect.fail(
						LinearProviderError.make({
							operation: 'list_issue_comments',
							reason: 'decode',
							retryable: false,
						}),
					)
				return page(issue.comments.pageInfo.endCursor, values)
			}),
		)
	return page(null, [])
})
