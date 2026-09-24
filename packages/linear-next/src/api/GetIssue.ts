import { Effect, Schema } from 'effect'

import type { LinearIssueRequest } from '../LinearApi'
import { Issue, issueFields, issueInfo, runGraphql } from './ProcedureSupport'

const document = `query LinearIssue($id: String!) { issue(id: $id) { ${issueFields} } }`
const Data = Schema.Struct({ issue: Issue })
export const getIssue = Effect.fn('linear.api.get_issue')((input: LinearIssueRequest) =>
	runGraphql('get_issue', document, { id: input.issue.issueId }, Data).pipe(
		Effect.map(({ issue }) => issueInfo(input.issue, issue)),
	),
)
