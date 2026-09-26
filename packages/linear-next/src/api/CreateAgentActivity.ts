import { Effect, Schema } from 'effect'

import { LinearAgentActivityId, LinearAgentSessionId } from '../LinearIdentity'
import {
	LinearActivityContent,
	LinearAgentActivityReceipt,
	type LinearCreateAgentActivityRequest,
} from '../LinearModels'
import { failLinearMutation } from './LinearApiErrors'
import { linearGraphql } from './LinearGraphql'

const AgentActivityContentInput = Schema.Struct({
	type: Schema.Literals(['thought', 'response']),
	body: Schema.String,
})

export const CreateAgentActivityVariables = Schema.Struct({
	input: Schema.Struct({
		agentSessionId: LinearAgentSessionId,
		content: AgentActivityContentInput,
		ephemeral: Schema.Boolean,
	}),
})

const CreateAgentActivityResponse = Schema.Struct({
	agentActivityCreate: Schema.Struct({
		success: Schema.Boolean,
		agentActivity: Schema.Struct({
			id: LinearAgentActivityId,
			agentSession: Schema.Struct({ id: LinearAgentSessionId }),
		}),
	}),
})

const mutation = `mutation LinearAgentActivityCreate($input: AgentActivityCreateInput!) {
  agentActivityCreate(input: $input) {
    success
    agentActivity {
      id
      agentSession { id }
    }
  }
}`

export const createAgentActivity = (request: LinearCreateAgentActivityRequest) => {
	const content = LinearActivityContent.match(request.content, {
		Thought: ({ body }) => AgentActivityContentInput.make({ type: 'thought', body }),
		Response: ({ body }) => AgentActivityContentInput.make({ type: 'response', body }),
	})
	return linearGraphql({
		operation: 'create_agent_activity',
		query: mutation,
		variables: CreateAgentActivityVariables,
		input: {
			input: {
				agentSessionId: request.sessionId,
				content,
				ephemeral: request.ephemeral,
			},
		},
		response: CreateAgentActivityResponse,
	}).pipe(
		Effect.flatMap((data) => {
			const activity = data.agentActivityCreate.agentActivity
			if (!data.agentActivityCreate.success || activity.agentSession.id !== request.sessionId) {
				return failLinearMutation('create_agent_activity')
			}
			return Effect.succeed(
				LinearAgentActivityReceipt.make({
					activityId: activity.id,
					sessionId: activity.agentSession.id,
				}),
			)
		}),
	)
}
