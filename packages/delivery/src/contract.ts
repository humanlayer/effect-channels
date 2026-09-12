import { Schema } from 'effect'
import { HttpApi, HttpApiEndpoint, HttpApiGroup, HttpApiSchema } from 'effect/unstable/httpapi'

import {
	DeliveryControlUnavailable,
	DeliveryNotFound,
	DeliveryOutcomeConflict,
	DeliveryTerminalReceipt,
} from './protocol.js'
import { DeliveryId } from './protocol.js'

export class Unauthorized extends Schema.TaggedError<Unauthorized>()('Unauthorized', {
	message: Schema.String,
}) {}

export class Forbidden extends Schema.TaggedError<Forbidden>()('Forbidden', {
	message: Schema.String,
}) {}

export class Unavailable extends Schema.TaggedError<Unavailable>()('Unavailable', {
	message: Schema.String,
}) {}

const Params = Schema.Struct({ deliveryId: DeliveryId })
const EmptyPayload = Schema.Struct({})
const Success = DeliveryTerminalReceipt.pipe(HttpApiSchema.status(202))
const Errors = [
	DeliveryNotFound.pipe(HttpApiSchema.status(404)),
	DeliveryOutcomeConflict.pipe(HttpApiSchema.status(409)),
	DeliveryControlUnavailable.pipe(HttpApiSchema.status(503)),
	Unauthorized.pipe(HttpApiSchema.status(401)),
	Forbidden.pipe(HttpApiSchema.status(403)),
	Unavailable.pipe(HttpApiSchema.status(503)),
] as const

export const DeliveryContract = HttpApi.make('DeliveryControlApi').add(
	HttpApiGroup.make('deliveries').add(
		HttpApiEndpoint.post('complete', '/deliveries/:deliveryId/complete', {
			params: Params,
			payload: EmptyPayload,
			success: Success,
			error: Errors,
		}),
		HttpApiEndpoint.post('fail', '/deliveries/:deliveryId/fail', {
			params: Params,
			payload: EmptyPayload,
			success: Success,
			error: Errors,
		}),
	),
)
