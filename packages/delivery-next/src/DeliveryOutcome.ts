/**
 * This file defines how a delivery ends, and the output operation that shows it.
 *
 * Every result a remote worker sends through the delivery API saves one `PresentOutcome` in the same
 * write. The provider decides what that looks like: Slack posts the Markdown, or does nothing when
 * there is none. A callback that finishes on its own, without handoff, saves no `PresentOutcome`.
 */
import { Schema } from 'effect'

/** How a delivery ended. `AwaitingInput` ends the turn with a question; the reply is a new delivery. */
export const DeliveryOutcome = Schema.TaggedUnion({
	Completed: {},
	Failed: {},
	AwaitingInput: { options: Schema.optionalKey(Schema.Array(Schema.NonEmptyString)) },
})
export type DeliveryOutcome = typeof DeliveryOutcome.Type

/** A remote worker's final word on a delivery. */
export const DeliveryTerminal = Schema.Struct({
	outcome: DeliveryOutcome,
	markdown: Schema.optionalKey(Schema.String),
})
export type DeliveryTerminal = typeof DeliveryTerminal.Type

/** Show how the delivery ended. */
export const PresentOutcome = Schema.TaggedStruct('PresentOutcome', {
	outcome: DeliveryOutcome,
	markdown: Schema.optionalKey(Schema.String),
})
export type PresentOutcome = typeof PresentOutcome.Type
