import { Effect, Schema } from 'effect'

import type { LinearGetUserRequest } from '../LinearApi'
import { LinearValidationError, linearProviderErrorDetails } from './LinearApiErrors'
import { projectLinearUser } from './LinearApiProjections'
import { LinearApiParticipant } from './LinearApiSchemas'
import { linearGraphql } from './LinearGraphql'

const document = `query LinearUser($id: String!, $teamId: ID!) { user(id: $id) { id name email active app isAssignable canAccessAnyPublicTeam teams(first: 1, filter: { id: { eq: $teamId } }) { nodes { id } } } }`
const GetUserResponse = Schema.Struct({ user: LinearApiParticipant })

export const getUser = Effect.fn('linear.api.get_user')((input: LinearGetUserRequest) => {
	if (input.issue.teamId === null) {
		return Effect.fail(new LinearValidationError(linearProviderErrorDetails('get_user')))
	}
	return linearGraphql({
		operation: 'get_user',
		query: document,
		variables: { id: input.userId, teamId: input.issue.teamId },
		response: GetUserResponse,
	}).pipe(Effect.map(({ user }) => projectLinearUser(user)))
})
