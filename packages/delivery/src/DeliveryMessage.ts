/**
 * This file defines delivery messages: progress messages a remote worker posts, edits, and removes
 * before it ends the delivery.
 *
 * The remote worker names each message with its own `MessageId`. The provider's reference to the
 * message it made, such as a Slack channel and timestamp, stays in the store as the receipt of the
 * `CreateMessage` operation, and is never returned by the delivery API.
 *
 * Operations run one at a time, in the order they were saved, so an update or deletion always runs
 * after its message's `CreateMessage` has settled. When it runs, the store hands the provider the saved
 * reference with it.
 */
import { Schema } from 'effect'

/** A message's name, chosen by the remote worker and unique within its delivery. */
export const MessageId = Schema.NonEmptyString.check(
	Schema.isMaxLength(128),
	Schema.isPattern(/^[A-Za-z0-9._-]+$/),
).pipe(Schema.brand('MessageId'))
export type MessageId = typeof MessageId.Type

/** Post a new message. */
export const CreateMessage = Schema.TaggedStruct('CreateMessage', {
	messageId: MessageId,
	markdown: Schema.String,
})
export type CreateMessage = typeof CreateMessage.Type

/** Replace a posted message's text. */
export const UpdateMessage = Schema.TaggedStruct('UpdateMessage', {
	messageId: MessageId,
	markdown: Schema.String,
})
export type UpdateMessage = typeof UpdateMessage.Type

/** Remove a posted message. */
export const DeleteMessage = Schema.TaggedStruct('DeleteMessage', {
	messageId: MessageId,
})
export type DeleteMessage = typeof DeleteMessage.Type

/** A saved operation on one message. */
export const DeliveryMessageOperation = Schema.Union([CreateMessage, UpdateMessage, DeleteMessage])
export type DeliveryMessageOperation = typeof DeliveryMessageOperation.Type

/**
 * The provider's own reference to a message it posted: the receipt it gave for `CreateMessage`.
 * Only that provider reads it.
 */
export const ProviderMessageReference = Schema.Json
export type ProviderMessageReference = typeof ProviderMessageReference.Type

/** `UpdateMessage` as a provider receives it, with the reference to the message it changes. */
export const ProviderUpdateMessage = Schema.TaggedStruct('UpdateMessage', {
	messageId: MessageId,
	markdown: Schema.String,
	reference: ProviderMessageReference,
})
export type ProviderUpdateMessage = typeof ProviderUpdateMessage.Type

/** `DeleteMessage` as a provider receives it, with the reference to the message it removes. */
export const ProviderDeleteMessage = Schema.TaggedStruct('DeleteMessage', {
	messageId: MessageId,
	reference: ProviderMessageReference,
})
export type ProviderDeleteMessage = typeof ProviderDeleteMessage.Type
