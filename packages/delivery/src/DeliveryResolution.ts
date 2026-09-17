import { Effect, Schema } from 'effect'

import type { ResolvedDelivery } from './DeliveryControl'
import type { EventDefinition } from './EventDefinition'

export class DeliveryDefinitionMismatch extends Schema.TaggedError<DeliveryDefinitionMismatch>()(
	'DeliveryDefinitionMismatch',
	{ message: Schema.String },
) {}

export type NativeResolvedDelivery<Event, Resource, Provider extends string> = Omit<
	ResolvedDelivery,
	'provider' | 'resource' | 'payload'
> & {
	readonly provider: Provider
	readonly resource: Resource
	readonly event: Event
}

/** Decode a persisted delivery through its provider-owned event definition. */
export const resolveDeliveryFor = <
	Event extends Schema.Constraint,
	Resource extends Schema.Constraint,
	Provider extends string,
>(
	delivery: ResolvedDelivery,
	definition: EventDefinition<Event, Resource, Provider>,
): Effect.Effect<
	NativeResolvedDelivery<Event['Type'], Resource['Type'], Provider>,
	DeliveryDefinitionMismatch | Schema.SchemaError,
	Event['DecodingServices'] | Resource['DecodingServices']
> =>
	Effect.gen(function* () {
		if (
			delivery.provider !== definition.provider ||
			delivery.definition !== definition.name ||
			delivery.version !== definition.version
		)
			return yield* DeliveryDefinitionMismatch.make({
				message: 'The delivery does not match the requested provider event definition.',
			})
		const event = yield* Schema.decodeEffect(Schema.fromJsonString(definition.event))(delivery.payload)
		const resource = yield* Schema.decodeEffect(Schema.fromJsonString(definition.resource))(delivery.resource)
		return {
			deliveryId: delivery.deliveryId,
			provider: definition.provider,
			installation: delivery.installation,
			organizationId: delivery.organizationId,
			definition: delivery.definition,
			version: delivery.version,
			eventId: delivery.eventId,
			resource,
			event,
		}
	})
