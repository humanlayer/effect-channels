/**
 * Where a Linear delivery's output goes, saved before the callback runs.
 *
 * A session callback writes to its Agent Session; an issue callback writes to the issue. The delivery
 * core stores these values as opaque JSON beside `presentationVersion`; only the Linear provider reads them.
 */
import {
	DeliveryOperationKind,
	DeliveryReactionTargetKind,
	PreparedDeliveryInvocation,
} from '@humanlayer/channels-delivery'
import { Effect, Match, Predicate, Schema } from 'effect'

import { LinearCallbackName } from './LinearCallbacks'
import {
	LinearAgentSessionId,
	LinearCommentId,
	LinearIssueId,
	LinearOrganizationId,
	LinearUserId,
} from './LinearIdentity'

/** The version of every shape in this file. Bump it when one changes in a way old records cannot decode. */
export const LinearDeliveryPresentationVersion = 1

/** Output for `onAgentSessionCreated` and `onAgentSessionPrompted` goes to the Agent Session. */
export const LinearAgentSessionDestination = Schema.TaggedStruct('LinearAgentSessionDestination', {
	organizationId: LinearOrganizationId,
	appUserId: LinearUserId,
	sessionId: LinearAgentSessionId,
	issueId: LinearIssueId,
})
export type LinearAgentSessionDestination = typeof LinearAgentSessionDestination.Type

/** Output for `onIssueCreated`, `onMentioned`, `onAssigned`, and `onSubscribedEvent` goes to the issue. */
export const LinearIssueDestination = Schema.TaggedStruct('LinearIssueDestination', {
	organizationId: LinearOrganizationId,
	issueId: LinearIssueId,
})
export type LinearIssueDestination = typeof LinearIssueDestination.Type

export const LinearDeliveryDestination = Schema.Union([LinearAgentSessionDestination, LinearIssueDestination])
export type LinearDeliveryDestination = typeof LinearDeliveryDestination.Type

/** The delivery started from the issue itself. */
export const LinearIssueActivationTarget = Schema.TaggedStruct('LinearIssueActivationTarget', {
	organizationId: LinearOrganizationId,
	issueId: LinearIssueId,
})
export type LinearIssueActivationTarget = typeof LinearIssueActivationTarget.Type

/** The delivery started from a comment on the issue. */
export const LinearCommentActivationTarget = Schema.TaggedStruct('LinearCommentActivationTarget', {
	organizationId: LinearOrganizationId,
	issueId: LinearIssueId,
	commentId: LinearCommentId,
})
export type LinearCommentActivationTarget = typeof LinearCommentActivationTarget.Type

export const LinearActivationTarget = Schema.Union([LinearIssueActivationTarget, LinearCommentActivationTarget])
export type LinearActivationTarget = typeof LinearActivationTarget.Type

/**
 * What a Linear processor saves before running a callback.
 *
 * @property activationTarget - absent when the batch has no one thing that started it, as for subscribed events
 */
export const LinearDeliveryPreparation = Schema.Struct({
	callback: LinearCallbackName,
	destination: LinearDeliveryDestination,
	activationTarget: Schema.optionalKey(LinearActivationTarget),
})
export type LinearDeliveryPreparation = typeof LinearDeliveryPreparation.Type

/**
 * A session shows `SetActivity` as an ephemeral thought. It has no message update or delete: Linear
 * activities cannot be edited or removed.
 */
const agentSessionOperations: ReadonlyArray<DeliveryOperationKind> = [
	'PresentOutcome',
	'CreateMessage',
	'SetMessageReaction',
	'SetActivity',
	'RenderPlan',
	'AddExternalLink',
]

/** An issue has no session to show activity in, so it has no `SetActivity`. */
const issueOperations: ReadonlyArray<DeliveryOperationKind> = [
	'PresentOutcome',
	'CreateMessage',
	'UpdateMessage',
	'DeleteMessage',
	'SetMessageReaction',
	'RenderPlan',
	'AddExternalLink',
]

/** Session activities cannot be edited or deleted, so a session destination has no message update or delete. */
export const linearSupportedOperations = (destination: LinearDeliveryDestination) =>
	Match.value(destination).pipe(
		Match.tagsExhaustive({
			LinearAgentSessionDestination: () => agentSessionOperations,
			LinearIssueDestination: () => issueOperations,
		}),
	)

/**
 * What a delivery's reactions can go on. A session reacts on the comment that started it, else the
 * issue; its messages are activities, which take no reactions. An issue delivery also reacts on the
 * comments it posted.
 */
export const linearReactionTargets = (preparation: LinearDeliveryPreparation) => {
	const messages = Match.value(preparation.destination).pipe(
		Match.withReturnType<ReadonlyArray<DeliveryReactionTargetKind>>(),
		Match.tagsExhaustive({
			LinearAgentSessionDestination: () => [],
			LinearIssueDestination: () => ['MessageTarget'],
		}),
	)
	return Predicate.isUndefined(preparation.activationTarget) ? messages : ['ActivationTarget' as const, ...messages]
}

const encodeDestination = Schema.encodeEffect(Schema.toCodecJson(LinearDeliveryDestination))
const encodeActivationTarget = Schema.encodeEffect(Schema.toCodecJson(LinearActivationTarget))

/** Encodes a preparation into the provider-neutral record the delivery core stores. */
export const encodeLinearDeliveryPreparation = Effect.fn('linear.delivery.encode_preparation')(function* (
	preparation: LinearDeliveryPreparation,
) {
	const destination = yield* encodeDestination(preparation.destination)
	const fields = {
		callback: preparation.callback,
		presentationVersion: LinearDeliveryPresentationVersion,
		destination,
		supportedOperations: linearSupportedOperations(preparation.destination),
		reactionTargets: linearReactionTargets(preparation),
	}
	if (Predicate.isUndefined(preparation.activationTarget)) return PreparedDeliveryInvocation.make(fields)
	const activationTarget = yield* encodeActivationTarget(preparation.activationTarget)
	return PreparedDeliveryInvocation.make({ ...fields, activationTarget })
})
