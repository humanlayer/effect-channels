import { Context, Effect, Schema } from 'effect'

import { LinearAgentActivityReceipt, LinearCreateAgentActivityRequest } from './LinearModels'

export const LinearApiOperation = Schema.Literals([
	'acquire_client_credentials_token',
	'viewer_identity',
	'create_agent_activity',
])
export type LinearApiOperation = typeof LinearApiOperation.Type

export class LinearApiError extends Schema.TaggedError<LinearApiError>()('LinearApiError', {
	operation: LinearApiOperation,
	reason: Schema.Literals([
		'unavailable',
		'unauthorized',
		'forbidden',
		'invalid_response',
		'identity_mismatch',
		'rejected',
	]),
	retryable: Schema.Boolean,
	status: Schema.optionalKey(Schema.Int),
}) {}

/** Provider-native Linear operations used by behavior-bearing Linear resources. */
export class LinearApi extends Context.Service<
	LinearApi,
	{
		readonly createAgentActivity: (
			request: LinearCreateAgentActivityRequest,
		) => Effect.Effect<LinearAgentActivityReceipt, LinearApiError>
	}
>()('@humanlayer/channels-linear-next/LinearApi') {}
