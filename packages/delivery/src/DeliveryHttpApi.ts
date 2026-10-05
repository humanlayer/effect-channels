/**
 * This file defines the delivery API a remote worker calls, as one typed Effect `HttpApi`.
 *
 * Every route takes the delivery's token as `Authorization: Bearer <token>`. Mutations answer 202:
 * the change is saved, not yet shown in Slack, GitHub, or Linear. Mailbox processing sends the output
 * afterwards, and retries it on its own.
 *
 * Paths are relative. `Channels.make` mounts them under the same `basePath` as the provider webhooks.
 */
import { Schema } from 'effect'
import { HttpApi, HttpApiEndpoint, HttpApiGroup, HttpApiSchema } from 'effect/http-api'

import { DeliveryActivity } from './DeliveryActivity'
import {
	AwaitingInputRequest,
	DeliveryClosed,
	DeliveryControlUnavailable,
	DeliveryMarkdown,
	DeliveryMessageConflict,
	DeliveryMessageDeleted,
	DeliveryMessageMarkdown,
	DeliveryMessageNotFound,
	DeliveryMutationReceipt,
	DeliveryNotFound,
	DeliveryOperationUnsupported,
	DeliveryReactionTargetUnavailable,
	DeliveryStatus,
	DeliveryTerminalConflict,
} from './DeliveryControl'
import { ExternalLink } from './DeliveryLink'
import { MessageId } from './DeliveryMessage'
import { DeliveryPlan } from './DeliveryPlan'
import { DeliveryReactionTarget, PortableReaction } from './DeliveryReaction'

/** The request had no `Authorization: Bearer` token. */
export class DeliveryCredentialMissing extends Schema.TaggedError<DeliveryCredentialMissing>()(
	'DeliveryCredentialMissing',
	{},
) {}

const Params = Schema.Struct({ deliveryId: Schema.String })
const MessageParams = Schema.Struct({ deliveryId: Schema.String, messageId: MessageId })
const ReactionParams = Schema.Struct({ deliveryId: Schema.String, reaction: PortableReaction })

export const CompleteDeliveryPayload = Schema.Struct({
	markdown: Schema.optionalKey(DeliveryMarkdown),
	awaitingInput: Schema.optionalKey(AwaitingInputRequest),
})
export type CompleteDeliveryPayload = typeof CompleteDeliveryPayload.Type

export const FailDeliveryPayload = Schema.Struct({
	markdown: Schema.optionalKey(DeliveryMarkdown),
})
export type FailDeliveryPayload = typeof FailDeliveryPayload.Type

/** A link to add: `label` and an `https` `url`. */
export const AddLinkPayload = Schema.Struct({
	label: ExternalLink.fields.label,
	url: ExternalLink.fields.url,
})
export type AddLinkPayload = typeof AddLinkPayload.Type

/** A message to post: the remote worker's name for it, and its text. */
export const CreateMessagePayload = Schema.Struct({
	messageId: MessageId,
	markdown: DeliveryMessageMarkdown,
})
export type CreateMessagePayload = typeof CreateMessagePayload.Type

/** A message's new text. */
export const UpdateMessagePayload = Schema.Struct({
	markdown: DeliveryMessageMarkdown,
})
export type UpdateMessagePayload = typeof UpdateMessagePayload.Type

/** The activity to show: `{ "_tag": "Working", "message": "…" }` or `{ "_tag": "Idle" }`. */
export const SetActivityPayload = Schema.Struct({
	activity: DeliveryActivity,
})
export type SetActivityPayload = typeof SetActivityPayload.Type

/**
 * Where the reaction goes, and whether the bot's reaction should be there:
 * `{ "target": { "_tag": "ActivationTarget" }, "active": true }`, or a `MessageTarget` with a `messageId`.
 */
export const SetReactionPayload = Schema.Struct({
	target: DeliveryReactionTarget,
	active: Schema.Boolean,
})
export type SetReactionPayload = typeof SetReactionPayload.Type

/**
 * The whole plan, which replaces the one before: `{ "plan": { "title": "…", "items": [{ "id": "…",
 * "title": "…", "state": { "_tag": "InProgress" } }] } }`.
 */
export const PutPlanPayload = Schema.Struct({
	plan: DeliveryPlan,
})
export type PutPlanPayload = typeof PutPlanPayload.Type

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
	DeliveryOperationUnsupported.pipe(HttpApiSchema.status(409)),
	DeliveryMessageNotFound.pipe(HttpApiSchema.status(409)),
	DeliveryMessageDeleted.pipe(HttpApiSchema.status(409)),
	DeliveryMessageConflict.pipe(HttpApiSchema.status(409)),
	DeliveryReactionTargetUnavailable.pipe(HttpApiSchema.status(409)),
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
		HttpApiEndpoint.post('addLink', '/deliveries/:deliveryId/links', {
			params: Params,
			payload: AddLinkPayload,
			success: Accepted,
			error: mutationErrors,
		}),
		HttpApiEndpoint.put('setActivity', '/deliveries/:deliveryId/activity', {
			params: Params,
			payload: SetActivityPayload,
			success: Accepted,
			error: mutationErrors,
		}),
		HttpApiEndpoint.put('putPlan', '/deliveries/:deliveryId/plan', {
			params: Params,
			payload: PutPlanPayload,
			success: Accepted,
			error: mutationErrors,
		}),
		HttpApiEndpoint.put('setReaction', '/deliveries/:deliveryId/reactions/:reaction', {
			params: ReactionParams,
			payload: SetReactionPayload,
			success: Accepted,
			error: mutationErrors,
		}),
		HttpApiEndpoint.post('createMessage', '/deliveries/:deliveryId/messages', {
			params: Params,
			payload: CreateMessagePayload,
			success: Accepted,
			error: mutationErrors,
		}),
		HttpApiEndpoint.patch('updateMessage', '/deliveries/:deliveryId/messages/:messageId', {
			params: MessageParams,
			payload: UpdateMessagePayload,
			success: Accepted,
			error: mutationErrors,
		}),
		HttpApiEndpoint.delete('deleteMessage', '/deliveries/:deliveryId/messages/:messageId', {
			params: MessageParams,
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
