/**
 * `agentSessionUpdate`: add labeled links to an Agent Session, or replace its whole Agent Plan. Linear
 * keeps the links it already has, and adding one does not change the session's state.
 */
import { Effect, Schema } from 'effect'

import { LinearAgentSessionId } from '../LinearIdentity'
import {
	LinearAgentPlanStep,
	LinearAgentSessionExternalUrl,
	type LinearUpdateAgentSessionRequest,
} from '../LinearModels'
import { failLinearMutation } from './LinearApiErrors'
import { linearGraphql } from './LinearGraphql'

const document = `mutation LinearAgentSessionUpdate($id: String!, $input: AgentSessionUpdateInput!) { agentSessionUpdate(id: $id, input: $input) { success agentSession { id } } }`

export const UpdateAgentSessionVariables = Schema.Struct({
	id: LinearAgentSessionId,
	input: Schema.Struct({
		addedExternalUrls: Schema.optionalKey(Schema.Array(LinearAgentSessionExternalUrl)),
		plan: Schema.optionalKey(Schema.Array(LinearAgentPlanStep)),
	}),
})

const UpdateAgentSessionResponse = Schema.Struct({
	agentSessionUpdate: Schema.Struct({
		success: Schema.Boolean,
		agentSession: Schema.Struct({ id: LinearAgentSessionId }),
	}),
})

/** The changes a request asks for: links to add, the whole plan, or both. */
const sessionChanges = ({ organizationId: _organizationId, sessionId: _sessionId, ...changes }: LinearUpdateAgentSessionRequest) =>
	changes

export const updateAgentSession = Effect.fn('linear.api.update_agent_session')(
	(request: LinearUpdateAgentSessionRequest) =>
		linearGraphql({
			operation: 'update_agent_session',
			query: document,
			variables: UpdateAgentSessionVariables,
			input: { id: request.sessionId, input: sessionChanges(request) },
			response: UpdateAgentSessionResponse,
		}).pipe(
			Effect.flatMap(({ agentSessionUpdate }) =>
				agentSessionUpdate.success && agentSessionUpdate.agentSession.id === request.sessionId
					? Effect.void
					: failLinearMutation('update_agent_session'),
			),
		),
)
