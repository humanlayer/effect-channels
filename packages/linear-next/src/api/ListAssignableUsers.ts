import { Effect, Schema } from 'effect'

import type { LinearListAssignableUsersRequest } from '../LinearApi'
import { LinearPageInfo, LinearUserPage } from '../LinearModels'
import { PageInfo, Participant, pageOptions, runGraphql, user } from './ProcedureSupport'

const document = `query LinearUsers($issueId: String!, $first: Int!, $after: String, $query: String) { issue(id: $issueId) { team { id members(first: $first, after: $after, filter: { active: { eq: true }, app: { eq: false }, isAssignable: { eq: true }, name: { containsIgnoreCase: $query } }) { nodes { id name email active app isAssignable canAccessAnyPublicTeam } pageInfo { endCursor hasNextPage } } } } }`
const Data = Schema.Struct({
	issue: Schema.Struct({
		team: Schema.Struct({ members: Schema.Struct({ nodes: Schema.Array(Participant), pageInfo: PageInfo }) }),
	}),
})
export const listAssignableUsers = Effect.fn('linear.api.list_assignable_users')(
	(input: LinearListAssignableUsersRequest) =>
		runGraphql(
			'list_assignable_users',
			document,
			{ issueId: input.issue.issueId, ...pageOptions(input) },
			Data,
		).pipe(
			Effect.map(({ issue }) =>
				LinearUserPage.make({
					users: issue.team.members.nodes
						.map(user)
						.filter((value) => value.active && !value.app && value.isAssignable),
					pageInfo: LinearPageInfo.make(issue.team.members.pageInfo),
				}),
			),
		),
)
