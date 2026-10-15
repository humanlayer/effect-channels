/**
 * The delivery an AgentSession is working on. It waits in the Durable Object's key-value storage, not in Fold's
 * log: it carries the delivery's access token, which the model must never see.
 */
import { DeliveryId } from '@humanlayer/channels-delivery'
import * as Cloudflare from 'alchemy/Cloudflare'
import { Context, Effect, Layer, Option, Predicate, Schema } from 'effect'

import { AgentSessionMessage } from './AgentSessionMessage'

const ACTIVE_DELIVERY_KEY = 'active_delivery'

/** The delivery being worked on. */
export const ActiveDeliveryRecord = Schema.Struct({
	message: AgentSessionMessage,
	/** Where the delivery's entries begin in Fold's log; saved just before its prompt is sent. */
	fromSeq: Schema.optionalKey(Schema.Int),
})
export type ActiveDeliveryRecord = typeof ActiveDeliveryRecord.Type

/** The AgentSession's one {@link ActiveDeliveryRecord}. */
export class ActiveDelivery extends Context.Service<
	ActiveDelivery,
	{
		readonly current: Effect.Effect<Option.Option<ActiveDeliveryRecord>>
		readonly save: (record: ActiveDeliveryRecord) => Effect.Effect<void>
		/** Forget the record if it is still the one for `deliveryId`. */
		readonly clear: (input: { readonly deliveryId: DeliveryId }) => Effect.Effect<void>
	}
>()('alchemy-cloudflare/ActiveDelivery', {
	make: Effect.gen(function* () {
		const { storage } = (yield* Cloudflare.DurableObjectState).raw
		const remove = Effect.promise(() => storage.delete(ACTIVE_DELIVERY_KEY)).pipe(Effect.asVoid)
		const current = Effect.promise(() => storage.get(ACTIVE_DELIVERY_KEY)).pipe(
			Effect.flatMap((value) =>
				Predicate.isUndefined(value)
					? Effect.succeedNone
					: Effect.asSome(Schema.decodeUnknownEffect(ActiveDeliveryRecord)(value)),
			),
			Effect.catchTag('SchemaError', (error) =>
				Effect.logError('active_delivery: the saved delivery is unreadable; forgetting it', error).pipe(
					Effect.andThen(remove),
					Effect.as(Option.none<ActiveDeliveryRecord>()),
				),
			),
		)

		return {
			current,
			save: (record) =>
				Schema.encodeEffect(ActiveDeliveryRecord)(record).pipe(
					Effect.orDie,
					Effect.flatMap((encoded) => Effect.promise(() => storage.put(ACTIVE_DELIVERY_KEY, encoded))),
				),
			clear: ({ deliveryId }) =>
				Effect.flatMap(current, (saved) =>
					Option.exists(saved, (record) => record.message.deliveryId === deliveryId) ? remove : Effect.void,
				),
		}
	}),
}) {
	/** Over the Durable Object's key-value storage. */

	static readonly layer = Layer.effect(this, this.make)
}
