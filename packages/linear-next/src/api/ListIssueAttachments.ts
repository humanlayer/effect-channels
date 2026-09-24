import { Effect, Schema } from 'effect'
import type * as HttpClient from 'effect/unstable/http/HttpClient'

import type { LinearIssueRequest } from '../LinearApi'
import type { LinearIssueAttachment } from '../LinearResources'
import { LinearProviderError } from './LinearApiErrors'
import { Attachment, PageInfo, attachment, runGraphql } from './ProcedureSupport'

const document = `query LinearIssueAttachments($id: String!, $after: String) { issue(id: $id) { attachments(first: 100, after: $after) { nodes { id title subtitle url metadata } pageInfo { endCursor hasNextPage } } } }`
const Data = Schema.Struct({
	issue: Schema.Struct({ attachments: Schema.Struct({ nodes: Schema.Array(Attachment), pageInfo: PageInfo }) }),
})
export const listIssueAttachments = Effect.fn('linear.api.list_issue_attachments')((input: LinearIssueRequest) => {
	const page = (
		after: string | null,
		accumulated: ReadonlyArray<LinearIssueAttachment>,
	): Effect.Effect<ReadonlyArray<LinearIssueAttachment>, LinearProviderError, HttpClient.HttpClient> =>
		runGraphql(
			'list_issue_attachments',
			document,
			{ id: input.issue.issueId, ...(after === null ? {} : { after }) },
			Data,
		).pipe(
			Effect.flatMap(({ issue }) => {
				const values = [
					...accumulated,
					...issue.attachments.nodes.map((value) => attachment(input.issue, value)),
				]
				if (!issue.attachments.pageInfo.hasNextPage) return Effect.succeed(values)
				if (issue.attachments.pageInfo.endCursor === null)
					return Effect.fail(
						LinearProviderError.make({
							operation: 'list_issue_attachments',
							reason: 'decode',
							retryable: false,
						}),
					)
				return page(issue.attachments.pageInfo.endCursor, values)
			}),
		)
	return page(null, [])
})
