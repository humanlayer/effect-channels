import { Effect, Schema } from 'effect'

import type { LinearUpdateIssueRequest } from '../LinearApi'
import { failLinearMutation } from './LinearApiErrors'
import { projectLinearIssue } from './LinearApiProjections'
import { LinearApiIssue, linearApiIssueFields } from './LinearApiSchemas'
import { linearGraphql } from './LinearGraphql'

const document = `mutation LinearIssueUpdate($id: String!, $input: IssueUpdateInput!) { issueUpdate(id: $id, input: $input) { success issue { ${linearApiIssueFields} } } }`
const UpdateIssueResponse = Schema.Struct({
	issueUpdate: Schema.Struct({ success: Schema.Boolean, issue: Schema.NullOr(LinearApiIssue) }),
})

export const updateIssue = Effect.fn('linear.api.update_issue')((input: LinearUpdateIssueRequest) => {
	const { addLabelIds, removeLabelIds, ...fields } = input.update
	const update: Record<string, Schema.Json> = { ...fields }
	if (addLabelIds !== undefined) update.addedLabelIds = addLabelIds
	if (removeLabelIds !== undefined) update.removedLabelIds = removeLabelIds
	return linearGraphql({
		operation: 'update_issue',
		query: document,
		variables: { id: input.issue.issueId, input: update },
		response: UpdateIssueResponse,
	}).pipe(
		Effect.flatMap(({ issueUpdate }) => {
			if (!issueUpdate.success) return failLinearMutation('update_issue')
			if (issueUpdate.issue === null) return failLinearMutation('update_issue')
			return Effect.succeed(projectLinearIssue(input.issue, issueUpdate.issue))
		}),
	)
})
