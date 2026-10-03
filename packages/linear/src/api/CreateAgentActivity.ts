/**
 * `agentActivityCreate`: post one activity to an Agent Session.
 *
 * With a caller-chosen `id`, Linear refuses a second activity with the same ID (`conflict on insert of
 * AgentActivity`, which `LinearGraphql` reports as `LinearAlreadyExistsError`), so a retry cannot post twice.
 */
import { Array as Arr, Effect, Match, Predicate, Schema } from 'effect'

import { LinearAgentActivityId, LinearAgentSessionId } from '../LinearIdentity'
import {
	LinearAgentActivityReceipt,
	type LinearCreateAgentActivityRequest,
} from '../LinearModels'
import { failLinearMutation } from './LinearApiErrors'
import { linearGraphql } from './LinearGraphql'

const AgentActivityContentInput = Schema.Struct({
	type: Schema.Literals(['thought', 'response', 'error', 'elicitation']),
	body: Schema.String,
})

/** One choice of a `select` elicitation. */
const SelectOption = Schema.Struct({ label: Schema.String, value: Schema.String })

export const CreateAgentActivityVariables = Schema.Struct({
	input: Schema.Struct({
		id: Schema.optionalKey(LinearAgentActivityId),
		agentSessionId: LinearAgentSessionId,
		content: AgentActivityContentInput,
		ephemeral: Schema.Boolean,
		signal: Schema.optionalKey(Schema.Literal('select')),
		signalMetadata: Schema.optionalKey(Schema.Struct({ options: Schema.Array(SelectOption) })),
	}),
})
type CreateAgentActivityInput = (typeof CreateAgentActivityVariables.Type)['input']

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

/** The GraphQL input for a request: content type and body, and the `select` signal for an elicitation with options. */
const activityInput = (request: LinearCreateAgentActivityRequest): CreateAgentActivityInput => {
	const session = { agentSessionId: request.sessionId, ephemeral: request.ephemeral }
	const base = Predicate.isUndefined(request.activityId) ? session : { ...session, id: request.activityId }
	return Match.value(request.content).pipe(
		Match.withReturnType<CreateAgentActivityInput>(),
		Match.tagsExhaustive({
			Thought: ({ body }) => ({ ...base, content: AgentActivityContentInput.make({ type: 'thought', body }) }),
			Response: ({ body }) => ({ ...base, content: AgentActivityContentInput.make({ type: 'response', body }) }),
			Error: ({ body }) => ({ ...base, content: AgentActivityContentInput.make({ type: 'error', body }) }),
			Elicitation: ({ body, options }) => {
				const content = AgentActivityContentInput.make({ type: 'elicitation', body })
				const choices = options ?? []
				if (!Arr.isReadonlyArrayNonEmpty(choices)) return { ...base, content }
				return {
					...base,
					content,
					signal: 'select',
					signalMetadata: { options: choices.map((option) => SelectOption.make({ label: option, value: option })) },
				}
			},
		}),
	)
}

export const createAgentActivity = (request: LinearCreateAgentActivityRequest) =>
	linearGraphql({
		operation: 'create_agent_activity',
		query: mutation,
		variables: CreateAgentActivityVariables,
		input: { input: activityInput(request) },
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
