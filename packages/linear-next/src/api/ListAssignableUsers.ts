import { Effect, Schema } from 'effect'

import type { LinearListAssignableUsersRequest } from '../LinearApi'
import { LinearPageInfo, LinearUserPage } from '../LinearModels'
import { projectLinearUser } from './LinearApiProjections'
import { LinearApiPageInfo, LinearApiParticipant } from './LinearApiSchemas'
import { linearUserPageVariables } from './LinearApiVariables'
import { linearGraphql } from './LinearGraphql'

const document = `query LinearUsers($issueId: String!, $first: Int!, $after: String, $query: String) { issue(id: $issueId) { team { id members(first: $first, after: $after, filter: { active: { eq: true }, app: { eq: false }, isAssignable: { eq: true }, name: { containsIgnoreCase: $query } }) { nodes { id name email active app isAssignable canAccessAnyPublicTeam } pageInfo { endCursor hasNextPage } } } } }`
const ListAssignableUsersResponse = Schema.Struct({
	issue: Schema.Struct({
		team: Schema.Struct({
			members: Schema.Struct({ nodes: Schema.Array(LinearApiParticipant), pageInfo: LinearApiPageInfo }),
		}),
	}),
})

export const listAssignableUsers = Effect.fn('linear.api.list_assignable_users')(
	(input: LinearListAssignableUsersRequest) =>
		linearGraphql({
			operation: 'list_assignable_users',
			query: document,
			variables: { issueId: input.issue.issueId, ...linearUserPageVariables(input) },
			response: ListAssignableUsersResponse,
		}).pipe(
			Effect.map(({ issue }) =>
				LinearUserPage.make({
					users: issue.team.members.nodes
						.map(projectLinearUser)
						.filter((value) => value.active && !value.app && value.isAssignable),
					pageInfo: LinearPageInfo.make(issue.team.members.pageInfo),
				}),
			),
		),
)
