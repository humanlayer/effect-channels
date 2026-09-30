/**
 * `agentSessionUpdate`: add labeled links to an Agent Session. Linear keeps the links it already has,
 * and adding one does not change the session's state. The Agent Plan arrives in a later phase.
 */
import { Effect, Schema } from 'effect'

import { LinearAgentSessionId } from '../LinearIdentity'
import { LinearAgentSessionExternalUrl, type LinearUpdateAgentSessionRequest } from '../LinearModels'
import { failLinearMutation } from './LinearApiErrors'
import { linearGraphql } from './LinearGraphql'

const document = `mutation LinearAgentSessionUpdate($id: String!, $input: AgentSessionUpdateInput!) { agentSessionUpdate(id: $id, input: $input) { success agentSession { id } } }`

export const UpdateAgentSessionVariables = Schema.Struct({
	id: LinearAgentSessionId,
	input: Schema.Struct({ addedExternalUrls: Schema.Array(LinearAgentSessionExternalUrl) }),
})

const UpdateAgentSessionResponse = Schema.Struct({
	agentSessionUpdate: Schema.Struct({
		success: Schema.Boolean,
		agentSession: Schema.Struct({ id: LinearAgentSessionId }),
	}),
})

export const updateAgentSession = Effect.fn('linear.api.update_agent_session')(
	(request: LinearUpdateAgentSessionRequest) =>
		linearGraphql({
			operation: 'update_agent_session',
			query: document,
			variables: UpdateAgentSessionVariables,
			input: { id: request.sessionId, input: { addedExternalUrls: request.addedExternalUrls } },
			response: UpdateAgentSessionResponse,
		}).pipe(
			Effect.flatMap(({ agentSessionUpdate }) =>
				agentSessionUpdate.success && agentSessionUpdate.agentSession.id === request.sessionId
					? Effect.void
					: failLinearMutation('update_agent_session'),
			),
		),
)
