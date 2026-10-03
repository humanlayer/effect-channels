/**
 * This file defines delivery activity: whether the agent is working right now, shown where the
 * provider shows it, such as Slack's thread status line.
 *
 * Activity is desired state, not history. The store keeps only the latest request: a new one replaces
 * a `SetActivity` still waiting to be sent, and a request for the state already desired is a replay.
 * Use activity for progress that changes, and messages for lasting output. `PresentOutcome` clears the
 * activity when the delivery ends.
 */
import { Schema } from 'effect'

/** What the agent is doing: `Working` with a short line saying on what, or `Idle`. */
export const DeliveryActivity = Schema.TaggedUnion({
	Working: { message: Schema.NonEmptyString.check(Schema.isMaxLength(200)) },
	Idle: {},
})
export type DeliveryActivity = typeof DeliveryActivity.Type

/** Whether two activities are the same desired state. */
export const sameDeliveryActivity = Schema.toEquivalence(DeliveryActivity)

/** Show the latest desired activity. */
export const SetActivity = Schema.TaggedStruct('SetActivity', {
	activity: DeliveryActivity,
})
export type SetActivity = typeof SetActivity.Type
