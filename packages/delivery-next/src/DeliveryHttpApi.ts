/**
 * This file defines the delivery API a remote worker calls, as one typed Effect `HttpApi`.
 *
 * Every route takes the delivery's token as `Authorization: Bearer <token>`. Mutations answer 202:
 * the change is saved, not yet shown in Slack, GitHub, or Linear.
 *
 * Paths are relative. `Channels.make` mounts them under the same `basePath` as the provider webhooks.
 */
import { Schema } from 'effect'
import { HttpApi, HttpApiEndpoint, HttpApiGroup, HttpApiSchema } from 'effect/unstable/httpapi'

import {
	AwaitingInputRequest,
	DeliveryClosed,
	DeliveryControlUnavailable,
	DeliveryMarkdown,
	DeliveryMutationReceipt,
	DeliveryNotFound,
	DeliveryStatus,
	DeliveryTerminalConflict,
} from './DeliveryControl'

/** The request had no `Authorization: Bearer` token. */
export class DeliveryCredentialMissing extends Schema.TaggedError<DeliveryCredentialMissing>()(
	'DeliveryCredentialMissing',
	{},
) {}

const Params = Schema.Struct({ deliveryId: Schema.String })

export const CompleteDeliveryPayload = Schema.Struct({
	markdown: Schema.optionalKey(DeliveryMarkdown),
	awaitingInput: Schema.optionalKey(AwaitingInputRequest),
})
export type CompleteDeliveryPayload = typeof CompleteDeliveryPayload.Type

export const FailDeliveryPayload = Schema.Struct({
	markdown: Schema.optionalKey(DeliveryMarkdown),
})
export type FailDeliveryPayload = typeof FailDeliveryPayload.Type

const Accepted = DeliveryMutationReceipt.pipe(HttpApiSchema.status(202))

const statusErrors = [
	DeliveryCredentialMissing.pipe(HttpApiSchema.status(401)),
	DeliveryNotFound.pipe(HttpApiSchema.status(404)),
	DeliveryControlUnavailable.pipe(HttpApiSchema.status(503)),
] as const

const mutationErrors = [
	...statusErrors,
	DeliveryTerminalConflict.pipe(HttpApiSchema.status(409)),
	DeliveryClosed.pipe(HttpApiSchema.status(409)),
] as const

export const DeliveryHttpApi = HttpApi.make('ChannelsDeliveryApi').add(
	HttpApiGroup.make('deliveries').add(
		HttpApiEndpoint.get('status', '/deliveries/:deliveryId', {
			params: Params,
			success: DeliveryStatus,
			error: statusErrors,
		}),
		HttpApiEndpoint.post('complete', '/deliveries/:deliveryId/complete', {
			params: Params,
			payload: CompleteDeliveryPayload,
			success: Accepted,
			error: mutationErrors,
		}),
		HttpApiEndpoint.post('fail', '/deliveries/:deliveryId/fail', {
			params: Params,
			payload: FailDeliveryPayload,
			success: Accepted,
			error: mutationErrors,
		}),
	),
)

/** The mount prefix for a `basePath`, in the form `HttpApi.prefix` takes. `/` adds nothing. */
export const deliveryApiPrefix = (basePath: string | undefined): `/${string}` =>
	`/${(basePath ?? '').replace(/^\/+|\/+$/g, '')}`

/** The delivery API under a `basePath`. */
export const prefixedDeliveryHttpApi = (basePath: string | undefined) =>
	DeliveryHttpApi.prefix(deliveryApiPrefix(basePath))
