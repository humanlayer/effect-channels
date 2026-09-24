import { Effect, Redacted, Schema } from 'effect'

import { LinearApiError } from '../LinearApi'
import { LinearAgentActivityId, LinearAgentSessionId } from '../LinearIdentity'
import {
	LinearActivityContent,
	LinearAgentActivityReceipt,
	type LinearCreateAgentActivityRequest,
} from '../LinearModels'
import { linearGraphql } from './LinearGraphql'

const CreateAgentActivityData = Schema.Struct({
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

export const createAgentActivity = (
	request: LinearCreateAgentActivityRequest,
	credential: Redacted.Redacted<string>,
) => {
	const content = LinearActivityContent.match(request.content, {
		Thought: ({ body }) => ({ type: 'thought', body }),
		Response: ({ body }) => ({ type: 'response', body }),
	})
	return linearGraphql({
		operation: 'create_agent_activity',
		query: mutation,
		variables: {
			input: {
				agentSessionId: request.sessionId,
				content,
				ephemeral: request.ephemeral,
			},
		},
		credential,
		data: CreateAgentActivityData,
	}).pipe(
		Effect.flatMap((data) => {
			const activity = data.agentActivityCreate.agentActivity
			if (!data.agentActivityCreate.success || activity.agentSession.id !== request.sessionId)
				return Effect.fail(
					LinearApiError.make({
						operation: 'create_agent_activity',
						reason: 'rejected',
						retryable: false,
					}),
				)
			return Effect.succeed(
				LinearAgentActivityReceipt.make({
					activityId: activity.id,
					sessionId: activity.agentSession.id,
				}),
			)
		}),
	)
}
