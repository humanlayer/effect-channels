/**
 * Where a Slack delivery's output goes, saved before the callback runs so a later attempt or a remote
 * worker can reach the same thread. The delivery core stores these as opaque JSON; only Slack reads them.
 */
import type { DeliveryOperationKind, DeliveryReactionTargetKind } from '@humanlayer/channels-delivery'
import { Option, Schema } from 'effect'

import { SlackMessageRef, SlackThreadRef } from './SlackModels'

/** The version of {@link SlackDeliveryDestination} and {@link SlackActivationTarget}. Bump when either changes shape. */
export const slackPresentationVersion = 1

/** The thread a delivery replies in. */
export const SlackDeliveryDestination = Schema.TaggedStruct('SlackThread', {
	thread: SlackThreadRef,
})
export type SlackDeliveryDestination = typeof SlackDeliveryDestination.Type

/** The message that started a delivery, for reactions. Subscribed-thread batches have none. */
export const SlackActivationTarget = Schema.TaggedStruct('SlackMessage', {
	message: SlackMessageRef,
})
export type SlackActivationTarget = typeof SlackActivationTarget.Type

/** JSON codecs for storing the destination and activation target. */
export const SlackDeliveryDestinationJson = Schema.toCodecJson(SlackDeliveryDestination)
export const SlackActivationTargetJson = Schema.toCodecJson(SlackActivationTarget)

/** The output operations a Slack thread supports. */
export const slackThreadSupportedOperations: ReadonlyArray<DeliveryOperationKind> = [
	'PresentOutcome',
	'CreateMessage',
	'UpdateMessage',
	'DeleteMessage',
	'SetMessageReaction',
	'SetActivity',
	'RenderPlan',
	'AddExternalLink',
]

/**
 * What a Slack delivery's reactions can go on: the message that started it, when there is one, and the
 * messages the delivery posted.
 */
export const slackReactionTargets = (
	activationTarget: Option.Option<SlackActivationTarget>,
): ReadonlyArray<DeliveryReactionTargetKind> =>
	Option.isSome(activationTarget) ? ['ActivationTarget', 'MessageTarget'] : ['MessageTarget']
