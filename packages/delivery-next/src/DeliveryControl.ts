/**
 * This file defines `DeliveryControl`: what a remote worker can do to its delivery.
 *
 * A remote worker names a delivery by ID and proves it may act on it with the delivery's token.
 * `DeliveryControl` parses the ID, then asks the owning store, which checks the token and changes the
 * delivery in one step. It never calls Slack, GitHub, or Linear.
 *
 * `DeliveryControlBackend` is the store's half. Memory and the Durable Object implement it; SQL and
 * Redis do not yet, so a bot on those stores cannot mount the delivery API.
 *
 * A change saves the provider output it needs in the same write, such as the `PresentOutcome` of a
 * result. Mailbox processing applies that output later.
 */
import { Context, Effect, Layer, Match, Option, Predicate, Redacted, Schema } from 'effect'

import { DeliveryActivity } from './DeliveryActivity'
import { DeliveryOperationKind, DeliveryStage } from './DeliveryContext'
import { ExternalLink } from './DeliveryLink'
import { MessageId } from './DeliveryMessage'
import { DeliveryOutputStatus } from './DeliveryOperation'
import { DeliveryOutcome, DeliveryTerminal } from './DeliveryOutcome'
import { DeliveryId, DeliveryReference, parseDeliveryId } from './DeliveryReference'

/** The longest final Markdown a remote worker may send. */
export const DELIVERY_MARKDOWN_MAX_LENGTH = 65_536

export const DeliveryMarkdown = Schema.String.check(Schema.isMaxLength(DELIVERY_MARKDOWN_MAX_LENGTH))

/** End the turn with a question. The user's reply starts a new delivery. */
export const AwaitingInputRequest = Schema.Struct({
	options: Schema.optionalKey(Schema.Array(Schema.NonEmptyString.check(Schema.isMaxLength(200))).check(Schema.isMaxLength(25))),
})

export const CompleteDelivery = Schema.TaggedStruct('CompleteDelivery', {
	markdown: Schema.optionalKey(DeliveryMarkdown),
	awaitingInput: Schema.optionalKey(AwaitingInputRequest),
})
export type CompleteDelivery = typeof CompleteDelivery.Type

export const FailDelivery = Schema.TaggedStruct('FailDelivery', {
	markdown: Schema.optionalKey(DeliveryMarkdown),
})
export type FailDelivery = typeof FailDelivery.Type

/** Add a link to the delivery. A repeat of a URL already added is a replay. */
export const AddDeliveryLink = Schema.TaggedStruct('AddDeliveryLink', {
	link: ExternalLink,
})
export type AddDeliveryLink = typeof AddDeliveryLink.Type

/** The text of a message. Slack refuses an empty one. */
export const DeliveryMessageMarkdown = Schema.NonEmptyString.check(Schema.isMaxLength(DELIVERY_MARKDOWN_MAX_LENGTH))

/** Post a message named `messageId`. The same request again is a replay; the same ID with other text is a conflict. */
export const CreateDeliveryMessage = Schema.TaggedStruct('CreateDeliveryMessage', {
	messageId: MessageId,
	markdown: DeliveryMessageMarkdown,
})
export type CreateDeliveryMessage = typeof CreateDeliveryMessage.Type

/** Replace the text of a message this delivery created. Text it already has is a replay. */
export const UpdateDeliveryMessage = Schema.TaggedStruct('UpdateDeliveryMessage', {
	messageId: MessageId,
	markdown: DeliveryMessageMarkdown,
})
export type UpdateDeliveryMessage = typeof UpdateDeliveryMessage.Type

/** Remove a message this delivery created. Removing it again is a replay. */
export const DeleteDeliveryMessage = Schema.TaggedStruct('DeleteDeliveryMessage', {
	messageId: MessageId,
})
export type DeleteDeliveryMessage = typeof DeleteDeliveryMessage.Type

/** A change to one of the delivery's messages. */
export const DeliveryMessageMutation = Schema.Union([CreateDeliveryMessage, UpdateDeliveryMessage, DeleteDeliveryMessage])
export type DeliveryMessageMutation = typeof DeliveryMessageMutation.Type

/** Show what the agent is doing now. The latest request wins; the state already desired is a replay. */
export const SetDeliveryActivity = Schema.TaggedStruct('SetDeliveryActivity', {
	activity: DeliveryActivity,
})
export type SetDeliveryActivity = typeof SetDeliveryActivity.Type

/** A change a remote worker asks for. Later phases add reaction and plan changes. */
export const DeliveryMutation = Schema.Union([
	CompleteDelivery,
	FailDelivery,
	AddDeliveryLink,
	CreateDeliveryMessage,
	UpdateDeliveryMessage,
	DeleteDeliveryMessage,
	SetDeliveryActivity,
])
export type DeliveryMutation = typeof DeliveryMutation.Type

/**
 * The store accepted the change. It does not mean any provider has shown it yet.
 *
 * @property status - `already_recorded` when the same request was accepted before
 */
export const DeliveryMutationReceipt = Schema.TaggedStruct('DeliveryMutationReceipt', {
	deliveryId: DeliveryId,
	status: Schema.Literals(['accepted', 'already_recorded']),
})
export type DeliveryMutationReceipt = typeof DeliveryMutationReceipt.Type

/**
 * What a remote worker may read about its delivery. Never includes the token or provider IDs.
 *
 * @property outcome - how the remote worker ended the delivery
 * @property activity - the activity the remote worker last asked for; `Idle` once the delivery has a result
 * @property output - the provider output the delivery owes or has sent. A failed output does not change `outcome`.
 */
export const DeliveryStatus = Schema.TaggedStruct('DeliveryStatus', {
	deliveryId: DeliveryId,
	stage: DeliveryStage,
	outcome: Schema.optionalKey(DeliveryOutcome),
	activity: Schema.optionalKey(DeliveryActivity),
	interruptRequested: Schema.Boolean,
	supportedOperations: Schema.Array(DeliveryOperationKind),
	output: Schema.Array(DeliveryOutputStatus),
})
export type DeliveryStatus = typeof DeliveryStatus.Type

/**
 * No such delivery, or the token does not match it. The two are not told apart,
 * so a caller cannot learn which deliveries exist.
 */
export class DeliveryNotFound extends Schema.TaggedError<DeliveryNotFound>()('DeliveryNotFound', {}) {}

/** The delivery already ended differently, or with different Markdown. */
export class DeliveryTerminalConflict extends Schema.TaggedError<DeliveryTerminalConflict>()(
	'DeliveryTerminalConflict',
	{},
) {}

/** The delivery has ended, so it takes no new changes. Repeats of changes it already took are still accepted. */
export class DeliveryClosed extends Schema.TaggedError<DeliveryClosed>()('DeliveryClosed', {}) {}

/** The delivery's destination cannot do this, such as edit a message. Nothing was saved. */
export class DeliveryOperationUnsupported extends Schema.TaggedError<DeliveryOperationUnsupported>()(
	'DeliveryOperationUnsupported',
	{ operation: DeliveryOperationKind },
) {}

/** The delivery has no message with this ID, or posting it failed, so there is nothing to change. */
export class DeliveryMessageNotFound extends Schema.TaggedError<DeliveryMessageNotFound>()('DeliveryMessageNotFound', {
	messageId: MessageId,
}) {}

/** The message was removed, so it cannot be changed. */
export class DeliveryMessageDeleted extends Schema.TaggedError<DeliveryMessageDeleted>()('DeliveryMessageDeleted', {
	messageId: MessageId,
}) {}

/** A message with this ID was already created with different text. */
export class DeliveryMessageConflict extends Schema.TaggedError<DeliveryMessageConflict>()('DeliveryMessageConflict', {
	messageId: MessageId,
}) {}

/** The store could not be reached. */
export class DeliveryControlUnavailable extends Schema.TaggedError<DeliveryControlUnavailable>()(
	'DeliveryControlUnavailable',
	{ reason: Schema.String },
) {}

export const DeliveryStatusError = Schema.Union([DeliveryNotFound, DeliveryControlUnavailable])
export type DeliveryStatusError = typeof DeliveryStatusError.Type

export const DeliveryMutationError = Schema.Union([
	DeliveryNotFound,
	DeliveryTerminalConflict,
	DeliveryClosed,
	DeliveryOperationUnsupported,
	DeliveryMessageNotFound,
	DeliveryMessageDeleted,
	DeliveryMessageConflict,
	DeliveryControlUnavailable,
])
export type DeliveryMutationError = typeof DeliveryMutationError.Type

/** The request a store checks and applies: the parsed delivery, the presented token, and the change. */
export const ReadDeliveryStatus = Schema.Struct({
	reference: DeliveryReference,
	accessToken: Schema.String,
})
export type ReadDeliveryStatus = typeof ReadDeliveryStatus.Type

export const ApplyDeliveryMutation = Schema.Struct({
	reference: DeliveryReference,
	accessToken: Schema.String,
	mutation: DeliveryMutation,
})
export type ApplyDeliveryMutation = typeof ApplyDeliveryMutation.Type

/**
 * The store's half of delivery control. Each method checks the token and applies its change in one
 * atomic step, so the delivery cannot change between the check and the write.
 */
export class DeliveryControlBackend extends Context.Service<
	DeliveryControlBackend,
	{
		readonly readDeliveryStatus: (input: ReadDeliveryStatus) => Effect.Effect<DeliveryStatus, DeliveryStatusError>
		readonly applyDeliveryMutation: (
			input: ApplyDeliveryMutation,
		) => Effect.Effect<DeliveryMutationReceipt, DeliveryMutationError>
	}
>()('@humanlayer/channels-delivery-next/DeliveryControlBackend') {}

/**
 * What a remote worker can do to its delivery, whatever store holds it.
 * On Cloudflare the Worker's implementation forwards each call to the owning mailbox object.
 */
export class DeliveryControl extends Context.Service<
	DeliveryControl,
	{
		readonly status: (input: {
			readonly deliveryId: string
			readonly accessToken: Redacted.Redacted<string>
		}) => Effect.Effect<DeliveryStatus, DeliveryStatusError>
		readonly apply: (input: {
			readonly deliveryId: string
			readonly accessToken: Redacted.Redacted<string>
			readonly mutation: DeliveryMutation
		}) => Effect.Effect<DeliveryMutationReceipt, DeliveryMutationError>
	}
>()('@humanlayer/channels-delivery-next/DeliveryControl') {}

/** The result a complete or fail request asks for. */
export const terminalFromMutation = (mutation: CompleteDelivery | FailDelivery): DeliveryTerminal => {
	const markdown = Predicate.isUndefined(mutation.markdown) ? {} : { markdown: mutation.markdown }
	const outcome = Match.value(mutation).pipe(
		Match.tagsExhaustive({
			FailDelivery: () => DeliveryOutcome.cases.Failed.make({}),
			CompleteDelivery: ({ awaitingInput }) =>
				Predicate.isUndefined(awaitingInput)
					? DeliveryOutcome.cases.Completed.make({})
					: DeliveryOutcome.cases.AwaitingInput.make(
							Predicate.isUndefined(awaitingInput.options) ? {} : { options: awaitingInput.options },
						),
		}),
	)
	return DeliveryTerminal.make({ outcome, ...markdown })
}

/** Whether a repeated terminal request is the same request. */
export const sameDeliveryTerminal = Schema.toEquivalence(DeliveryTerminal)

const referenceOrNotFound = (deliveryId: string) =>
	Option.match(parseDeliveryId(deliveryId), {
		onNone: () => Effect.fail(new DeliveryNotFound()),
		onSome: Effect.succeed,
	})

/** `DeliveryControl` over the store's own `DeliveryControlBackend`. */
export const DeliveryControlLive = Layer.effect(
	DeliveryControl,
	Effect.gen(function* () {
		const backend = yield* DeliveryControlBackend
		return DeliveryControl.of({
			status: Effect.fn('delivery.control.status')(function* (input) {
				const reference = yield* referenceOrNotFound(input.deliveryId)
				return yield* backend.readDeliveryStatus({ reference, accessToken: Redacted.value(input.accessToken) })
			}),
			apply: Effect.fn('delivery.control.apply')(function* (input) {
				const reference = yield* referenceOrNotFound(input.deliveryId)
				return yield* backend.applyDeliveryMutation({
					reference,
					accessToken: Redacted.value(input.accessToken),
					mutation: input.mutation,
				})
			}),
		})
	}),
)
