import { Effect, Schema } from 'effect'

import { LinearIssueUpdate, type LinearUpdateIssueRequest } from '../LinearApi'
import { LinearIssueId } from '../LinearIdentity'
import { failLinearMutation } from './LinearApiErrors'
import { projectLinearIssue } from './LinearApiProjections'
import { LinearApiIssue, linearApiIssueFields } from './LinearApiSchemas'
import { linearGraphql } from './LinearGraphql'

const document = `mutation LinearIssueUpdate($id: String!, $input: IssueUpdateInput!) { issueUpdate(id: $id, input: $input) { success issue { ${linearApiIssueFields} } } }`

/** Linear's `IssueUpdateInput`: an omitted field is left unchanged, while `null` clears it. */
export const LinearIssueUpdateInput = LinearIssueUpdate.pipe(
	Schema.encodeKeys({ addLabelIds: 'addedLabelIds', removeLabelIds: 'removedLabelIds' }),
)
export const UpdateIssueVariables = Schema.Struct({ id: LinearIssueId, input: LinearIssueUpdateInput })
const UpdateIssueResponse = Schema.Struct({
	issueUpdate: Schema.Struct({ success: Schema.Boolean, issue: Schema.NullOr(LinearApiIssue) }),
})

export const updateIssue = Effect.fn('linear.api.update_issue')((input: LinearUpdateIssueRequest) =>
	linearGraphql({
		operation: 'update_issue',
		query: document,
		variables: UpdateIssueVariables,
		input: { id: input.issue.issueId, input: input.update },
		response: UpdateIssueResponse,
	}).pipe(
		Effect.flatMap(({ issueUpdate }) => {
			if (!issueUpdate.success) return failLinearMutation('update_issue')
			if (issueUpdate.issue === null) return failLinearMutation('update_issue')
			return Effect.succeed(projectLinearIssue(input.issue, issueUpdate.issue))
		}),
	),
)
