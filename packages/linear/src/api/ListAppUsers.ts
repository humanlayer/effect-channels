import { Effect, Schema, Struct } from 'effect'

import { type LinearListAppUsersRequest, LinearUserPageOptions } from '../LinearApi'
import { LinearIssueId, LinearTeamId } from '../LinearIdentity'
import { LinearAppUser, LinearAppUserPage, LinearPageInfo } from '../LinearModels'
import { LinearValidationError, linearProviderErrorDetails } from './LinearApiErrors'
import { projectLinearUser } from './LinearApiProjections'
import { LinearApiPageInfo, LinearApiParticipant } from './LinearApiSchemas'
import { linearGraphql } from './LinearGraphql'

const document = `query LinearAppUsers($issueId: String!, $teamId: ID!, $first: Int!, $after: String, $query: String) { issue(id: $issueId) { team { id visibility } } users(first: $first, after: $after, filter: { active: { eq: true }, app: { eq: true }, isAssignable: { eq: true }, name: { containsIgnoreCase: $query } }) { nodes { id name email active app isAssignable canAccessAnyPublicTeam teams(first: 1, filter: { id: { eq: $teamId } }) { nodes { id } } } pageInfo { endCursor hasNextPage } } }`
export const ListAppUsersVariables = Schema.Struct({
	issueId: LinearIssueId,
	teamId: LinearTeamId,
	first: Schema.Int,
	after: LinearUserPageOptions.fields.after,
	query: LinearUserPageOptions.fields.query,
})
const ListAppUsersResponse = Schema.Struct({
	issue: Schema.Struct({ team: Schema.Struct({ id: LinearTeamId, visibility: Schema.String }) }),
	users: Schema.Struct({ nodes: Schema.Array(LinearApiParticipant), pageInfo: LinearApiPageInfo }),
})

export const listAppUsers = Effect.fn('linear.api.list_app_users')((input: LinearListAppUsersRequest) => {
	if (input.issue.teamId === null) {
		return Effect.fail(new LinearValidationError(linearProviderErrorDetails('list_app_users')))
	}
	return linearGraphql({
		operation: 'list_app_users',
		query: document,
		variables: ListAppUsersVariables,
		input: {
			issueId: input.issue.issueId,
			teamId: input.issue.teamId,
			first: input.first ?? 50,
			...Struct.pick(input, ['after', 'query']),
		},
		response: ListAppUsersResponse,
	}).pipe(
		Effect.map(({ issue, users }) =>
			LinearAppUserPage.make({
				users: users.nodes
					.map(projectLinearUser)
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
