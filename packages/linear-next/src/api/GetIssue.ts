import { Effect, Schema } from 'effect'

import type { LinearIssueRequest } from '../LinearApi'
import { LinearIssueId } from '../LinearIdentity'
import { projectLinearIssue } from './LinearApiProjections'
import { LinearApiIssue, linearApiIssueFields } from './LinearApiSchemas'
import { linearGraphql } from './LinearGraphql'

const document = `query LinearIssue($id: String!) { issue(id: $id) { ${linearApiIssueFields} } }`
export const GetIssueVariables = Schema.Struct({ id: LinearIssueId })
const GetIssueResponse = Schema.Struct({ issue: LinearApiIssue })

export const getIssue = Effect.fn('linear.api.get_issue')((input: LinearIssueRequest) =>
	linearGraphql({
		operation: 'get_issue',
		query: document,
		variables: GetIssueVariables,
		input: { id: input.issue.issueId },
		response: GetIssueResponse,
	}).pipe(Effect.map(({ issue }) => projectLinearIssue(input.issue, issue))),
)
