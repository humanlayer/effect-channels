import { Effect, Schema } from 'effect'

import type { LinearApiError, LinearIssueRequest } from '../LinearApi'
import type { LinearIssueAttachment } from '../LinearResources'
import { type LinearProviderError, LinearResponseDecodeError, linearProviderErrorDetails } from './LinearApiErrors'
import { projectLinearAttachment } from './LinearApiProjections'
import { LinearApiAttachment, LinearApiPageInfo } from './LinearApiSchemas'
import { linearGraphql } from './LinearGraphql'
import type { LinearHttpClient } from './LinearHttpClient'

const document = `query LinearIssueAttachments($id: String!, $after: String) { issue(id: $id) { attachments(first: 100, after: $after) { nodes { id title subtitle url metadata } pageInfo { endCursor hasNextPage } } } }`
const ListIssueAttachmentsResponse = Schema.Struct({
	issue: Schema.Struct({
		attachments: Schema.Struct({ nodes: Schema.Array(LinearApiAttachment), pageInfo: LinearApiPageInfo }),
	}),
})

type ListIssueAttachmentsVariables = {
	readonly id: string
	after?: string
}

export const listIssueAttachments = Effect.fn('linear.api.list_issue_attachments')((input: LinearIssueRequest) => {
	const page = (
		after: string | null,
		accumulated: ReadonlyArray<LinearIssueAttachment>,
	): Effect.Effect<ReadonlyArray<LinearIssueAttachment>, LinearProviderError | LinearApiError, LinearHttpClient> => {
		const variables: ListIssueAttachmentsVariables = { id: input.issue.issueId }
		if (after !== null) variables.after = after
		return linearGraphql({
			operation: 'list_issue_attachments',
			query: document,
			variables,
			response: ListIssueAttachmentsResponse,
		}).pipe(
			Effect.flatMap(({ issue }) => {
				const values = accumulated.concat(
					issue.attachments.nodes.map((value) => projectLinearAttachment(input.issue, value)),
				)
				if (!issue.attachments.pageInfo.hasNextPage) return Effect.succeed(values)
				if (issue.attachments.pageInfo.endCursor === null) {
					return Effect.fail(
						new LinearResponseDecodeError(linearProviderErrorDetails('list_issue_attachments')),
					)
				}
				return page(issue.attachments.pageInfo.endCursor, values)
			}),
		)
	}
	return page(null, [])
})
