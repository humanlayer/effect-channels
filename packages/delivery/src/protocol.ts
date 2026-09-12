import { Schema } from 'effect'

/** Maximum encoded delivery locator length accepted at public boundaries. */
export const DELIVERY_ID_MAX_LENGTH = 2_048

export const DeliveryId = Schema.NonEmptyString.check(
	Schema.isMaxLength(DELIVERY_ID_MAX_LENGTH),
	Schema.isPattern(/^delivery:(?:v1:[A-Za-z0-9_-]+:[A-Za-z0-9_-]+|v2:[A-Za-z0-9_-]+)$/),
)
export type DeliveryId = typeof DeliveryId.Type

export const DeliveryTerminalOutcome = Schema.Literals(['completed', 'failed'])
export type DeliveryTerminalOutcome = typeof DeliveryTerminalOutcome.Type

export class DeliveryNotFound extends Schema.TaggedError<DeliveryNotFound>()('DeliveryNotFound', {
	deliveryId: DeliveryId,
	message: Schema.String,
}) {}

export class DeliveryOutcomeConflict extends Schema.TaggedError<DeliveryOutcomeConflict>()('DeliveryOutcomeConflict', {
	deliveryId: DeliveryId,
	recordedOutcome: DeliveryTerminalOutcome,
	requestedOutcome: DeliveryTerminalOutcome,
	message: Schema.String,
}) {}

export class DeliveryControlUnavailable extends Schema.TaggedError<DeliveryControlUnavailable>()(
	'DeliveryControlUnavailable',
	{
		operation: Schema.String,
		message: Schema.String,
	},
) {}

export const DeliveryTerminalReceipt = Schema.Struct({
	deliveryId: DeliveryId,
	outcome: DeliveryTerminalOutcome,
	status: Schema.Literals(['accepted', 'already_recorded']),
})
export interface DeliveryTerminalReceipt extends Schema.Schema.Type<typeof DeliveryTerminalReceipt> {}

export const deliveryNotFound = (deliveryId: DeliveryId) =>
	DeliveryNotFound.make({ deliveryId, message: 'The requested delivery was not found.' })

export const deliveryOutcomeConflict = (input: {
	readonly deliveryId: DeliveryId
	readonly recordedOutcome: DeliveryTerminalOutcome
	readonly requestedOutcome: DeliveryTerminalOutcome
}) =>
	DeliveryOutcomeConflict.make({
		...input,
		message: `Delivery already has outcome "${input.recordedOutcome}" and cannot be changed to "${input.requestedOutcome}".`,
	})

export const deliveryControlUnavailable = (operation: string) =>
	DeliveryControlUnavailable.make({ operation, message: 'Delivery control is temporarily unavailable.' })
