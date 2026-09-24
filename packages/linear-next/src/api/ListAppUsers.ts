import { Effect, Schema } from 'effect'

import type { LinearListAppUsersRequest } from '../LinearApi'
import { LinearTeamId } from '../LinearIdentity'
import { LinearAppUser, LinearAppUserPage, LinearPageInfo } from '../LinearModels'
import { LinearProviderError } from './LinearApiErrors'
import { PageInfo, Participant, pageOptions, runGraphql, user } from './ProcedureSupport'

const document = `query LinearAppUsers($issueId: String!, $teamId: ID!, $first: Int!, $after: String, $query: String) { issue(id: $issueId) { team { id visibility } } users(first: $first, after: $after, filter: { active: { eq: true }, app: { eq: true }, isAssignable: { eq: true }, name: { containsIgnoreCase: $query } }) { nodes { id name email active app isAssignable canAccessAnyPublicTeam teams(first: 1, filter: { id: { eq: $teamId } }) { nodes { id } } } pageInfo { endCursor hasNextPage } } }`
const Data = Schema.Struct({
	issue: Schema.Struct({ team: Schema.Struct({ id: LinearTeamId, visibility: Schema.String }) }),
	users: Schema.Struct({ nodes: Schema.Array(Participant), pageInfo: PageInfo }),
})
export const listAppUsers = Effect.fn('linear.api.list_app_users')((input: LinearListAppUsersRequest) => {
	if (input.issue.teamId === null)
		return Effect.fail(
			LinearProviderError.make({ operation: 'list_app_users', reason: 'validation', retryable: false }),
		)
	return runGraphql(
		'list_app_users',
		document,
		{ issueId: input.issue.issueId, teamId: input.issue.teamId, ...pageOptions(input) },
		Data,
	).pipe(
		Effect.map(({ issue, users }) =>
			LinearAppUserPage.make({
				users: users.nodes
					.map(user)
					.filter(
						(value) =>
							value.active &&
							value.app &&
							value.isAssignable &&
							(value.teamIds.includes(issue.team.id) ||
								(issue.team.visibility === 'public' && value.canAccessAnyPublicTeam)),
					)
					.map((value) => LinearAppUser.make({ ...value, app: true })),
				pageInfo: LinearPageInfo.make(users.pageInfo),
			}),
		),
	)
})
