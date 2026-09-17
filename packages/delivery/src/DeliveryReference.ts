import { Effect, Encoding, Schema } from 'effect'

import { eventIdentity, type Envelope } from './Mailbox'
import { DeliveryId } from './protocol'

export { DeliveryId }

export const DeliveryReference = Schema.Struct({
	deliveryId: DeliveryId,
	mailboxKey: Schema.NonEmptyString,
})
export interface DeliveryReference extends Schema.Schema.Type<typeof DeliveryReference> {}

/** Encode and validate a delivery locator without allowing schema construction to defect. */
export const encodeDeliveryId = Effect.fn('delivery.reference.encode')(function* (input: {
	readonly mailboxKey: string
	readonly envelope: Envelope
}) {
	return yield* Schema.decodeEffect(DeliveryId)(
		`delivery:v1:${Encoding.encodeBase64Url(input.mailboxKey)}:${Encoding.encodeBase64Url(
			`${input.envelope.acceptedAt}:${eventIdentity(input.envelope)}`,
		)}`,
	)
})

export const decodeDeliveryReference = Effect.fn('delivery.reference.decode')(function* (input: {
	readonly deliveryId: string
}) {
	const deliveryId = yield* Schema.decodeEffect(DeliveryId)(input.deliveryId)
	const encodedKey = yield* Schema.decodeUnknownEffect(Schema.NonEmptyString)(deliveryId.split(':').at(2))
	const mailboxKey = yield* Effect.fromResult(Encoding.decodeBase64UrlString(encodedKey))
	return DeliveryReference.make({ deliveryId, mailboxKey })
})
