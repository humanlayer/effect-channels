import { Effect, Schema } from 'effect'

import type { LinearGetUserRequest } from '../LinearApi'
import { LinearProviderError } from './LinearApiErrors'
import { Participant, runGraphql, user } from './ProcedureSupport'

const document = `query LinearUser($id: String!, $teamId: ID!) { user(id: $id) { id name email active app isAssignable canAccessAnyPublicTeam teams(first: 1, filter: { id: { eq: $teamId } }) { nodes { id } } } }`
const Data = Schema.Struct({ user: Participant })
export const getUser = Effect.fn('linear.api.get_user')((input: LinearGetUserRequest) => {
	if (input.issue.teamId === null)
		return Effect.fail(LinearProviderError.make({ operation: 'get_user', reason: 'validation', retryable: false }))
	return runGraphql('get_user', document, { id: input.userId, teamId: input.issue.teamId }, Data).pipe(
		Effect.map(({ user: value }) => user(value)),
	)
})
