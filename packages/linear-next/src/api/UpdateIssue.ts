import { Effect, Predicate, Schema } from 'effect'

import type { LinearUpdateIssueRequest } from '../LinearApi'
import { Issue, issueFields, issueInfo, runGraphql } from './ProcedureSupport'

const document = `mutation LinearIssueUpdate($id: String!, $input: IssueUpdateInput!) { issueUpdate(id: $id, input: $input) { success issue { ${issueFields} } } }`
const Data = Schema.Struct({ issueUpdate: Schema.Struct({ success: Schema.Boolean, issue: Issue }) })
export const updateIssue = Effect.fn('linear.api.update_issue')((input: LinearUpdateIssueRequest) => {
	const { addLabelIds, removeLabelIds, ...fields } = input.update
	return runGraphql(
		'update_issue',
		document,
		{
			id: input.issue.issueId,
			input: {
				...fields,
				...(Predicate.isUndefined(addLabelIds) ? {} : { addedLabelIds: addLabelIds }),
				...(Predicate.isUndefined(removeLabelIds) ? {} : { removedLabelIds: removeLabelIds }),
			},
		},
		Data,
	).pipe(Effect.map(({ issueUpdate }) => issueInfo(input.issue, issueUpdate.issue)))
})
