/**
 * This file defines `DeliveryControl`: what a remote worker can do to its delivery.
 *
 * A remote worker names a delivery by ID and proves it may act on it with the delivery's token.
 * `DeliveryControl` parses the ID, then asks the owning store, which checks the token and changes the
 * delivery in one step. It never calls Slack, GitHub, or Linear.
 *
 * `DeliveryControlBackend` is the store's half. Memory and the Durable Object implement it; SQL and
 * Redis do not yet, so a bot on those stores cannot mount the delivery API.
 */
import { Context, Effect, Layer, Match, Option, Predicate, Redacted, Schema } from 'effect'

import {
	DeliveryOperationKind,
	DeliveryOutcome,
	DeliveryStage,
	DeliveryTerminal,
	type DeliveryTerminal as DeliveryTerminalType,
} from './DeliveryContext'
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

/** A change a remote worker asks for. Later phases add message, reaction, activity, and plan changes. */
export const DeliveryMutation = Schema.Union([CompleteDelivery, FailDelivery])
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

/** What a remote worker may read about its delivery. Never includes the token or provider IDs. */
export const DeliveryStatus = Schema.TaggedStruct('DeliveryStatus', {
	deliveryId: DeliveryId,
	stage: DeliveryStage,
	outcome: Schema.optionalKey(DeliveryOutcome),
	interruptRequested: Schema.Boolean,
	supportedOperations: Schema.Array(DeliveryOperationKind),
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

/** The delivery ended without a remote result, so it takes no more changes. */
export class DeliveryClosed extends Schema.TaggedError<DeliveryClosed>()('DeliveryClosed', {}) {}

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
	DeliveryControlUnavailable,
])
export type DeliveryMutationError = typeof DeliveryMutationError.Type

/** The request a store checks and applies: the parsed delivery, the presented token, and the change. */
export const ReadDeliveryStatus = Schema.Struct({
	reference: DeliveryReference,
	accessToken: Schema.String,
})
export type ReadDeliveryStatus = typeof ReadDeliveryStatus.Type

export const RecordDeliveryTerminal = Schema.Struct({
	reference: DeliveryReference,
	accessToken: Schema.String,
	terminal: DeliveryTerminal,
})
export type RecordDeliveryTerminal = typeof RecordDeliveryTerminal.Type

/**
 * The store's half of delivery control. Each method checks the token and applies its change in one
 * atomic step, so the delivery cannot change between the check and the write.
 */
export class DeliveryControlBackend extends Context.Service<
	DeliveryControlBackend,
	{
		readonly readDeliveryStatus: (input: ReadDeliveryStatus) => Effect.Effect<DeliveryStatus, DeliveryStatusError>
		readonly recordDeliveryTerminal: (
			input: RecordDeliveryTerminal,
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

/** The terminal a mutation asks for. */
export const terminalFromMutation = (mutation: DeliveryMutation): DeliveryTerminalType => {
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
				return yield* backend.recordDeliveryTerminal({
					reference,
					accessToken: Redacted.value(input.accessToken),
					terminal: terminalFromMutation(input.mutation),
				})
			}),
		})
	}),
)
