/**
 * This file defines portable reactions: a small set of reactions a remote worker can add to, or remove
 * from, something in its conversation, such as the message that started the delivery.
 *
 * A remote worker names intent, never provider IDs: a portable reaction such as `eyes`, a logical
 * target, and whether the bot's reaction should be there. Each provider maps the reaction to its own
 * emoji and finds the target from what the delivery saved: the activation target saved before the
 * callback ran, or the provider's reference to a message the delivery posted.
 *
 * A reaction is desired state per target and reaction. The same request again is a replay. A change
 * replaces a `SetMessageReaction` for the same target and reaction still waiting to be sent, so only
 * the latest is sent; one already being sent is followed by a new operation. Provider callbacks keep
 * their own, richer reaction APIs, such as Slack's custom emoji.
 */
import { Schema } from 'effect'

import { MessageId, ProviderMessageReference } from './DeliveryMessage'

/** The reactions every provider can show. */
export const PortableReaction = Schema.Literals([
	'thumbs_up',
	'thumbs_down',
	'laugh',
	'confused',
	'heart',
	'hooray',
	'rocket',
	'eyes',
])
export type PortableReaction = typeof PortableReaction.Type

/**
 * What a reaction goes on.
 *
 * - `ActivationTarget`: the message, comment, or issue that started the delivery
 * - `MessageTarget`: a message this delivery created, by the remote worker's `MessageId`
 * - `PlanTarget`: where the delivery's plan is shown; no provider shows plans yet, so it is never available
 */
export const DeliveryReactionTarget = Schema.TaggedUnion({
	ActivationTarget: {},
	MessageTarget: { messageId: MessageId },
	PlanTarget: {},
})
export type DeliveryReactionTarget = typeof DeliveryReactionTarget.Type

/** The kinds of reaction target a destination can react on. A provider lists them when it prepares a delivery. */
export const DeliveryReactionTargetKind = Schema.Literals(['ActivationTarget', 'MessageTarget', 'PlanTarget'])
export type DeliveryReactionTargetKind = typeof DeliveryReactionTargetKind.Type

/** The kind of a target, to check it against the kinds a destination lists. */
export const deliveryReactionTargetKind = DeliveryReactionTarget.match({
	ActivationTarget: (): DeliveryReactionTargetKind => 'ActivationTarget',
	MessageTarget: (): DeliveryReactionTargetKind => 'MessageTarget',
	PlanTarget: (): DeliveryReactionTargetKind => 'PlanTarget',
})

/** Whether two targets name the same thing. */
export const sameDeliveryReactionTarget = Schema.toEquivalence(DeliveryReactionTarget)

/** Make the bot's reaction present (`active: true`) or absent on a target. */
export const SetMessageReaction = Schema.TaggedStruct('SetMessageReaction', {
	target: DeliveryReactionTarget,
	reaction: PortableReaction,
	active: Schema.Boolean,
})
export type SetMessageReaction = typeof SetMessageReaction.Type

/**
 * A reaction target as a provider receives it. A message target carries the provider's own reference
 * to the message, from the receipt of its `CreateMessage`. The activation target is in the delivery's
 * saved preparation, which the provider already has.
 */
export const ProviderReactionTarget = Schema.TaggedUnion({
	ActivationTarget: {},
	MessageTarget: { messageId: MessageId, reference: ProviderMessageReference },
})
export type ProviderReactionTarget = typeof ProviderReactionTarget.Type

/**
 * `SetMessageReaction` as a provider receives it.
 *
 * @property addedReference - for a removal, the provider's receipt for this delivery's last applied add
 * of the same reaction on the same target. A provider that removes a reaction by its own ID, such as
 * Linear, needs it; Slack and GitHub remove by name.
 */
export const ProviderSetMessageReaction = Schema.TaggedStruct('SetMessageReaction', {
	target: ProviderReactionTarget,
	reaction: PortableReaction,
	active: Schema.Boolean,
	addedReference: Schema.optionalKey(Schema.Json),
})
export type ProviderSetMessageReaction = typeof ProviderSetMessageReaction.Type
