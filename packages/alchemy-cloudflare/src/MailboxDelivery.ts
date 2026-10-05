import {
	deliveryMailboxKey,
	DeliveryAdmission,
	DeliveryReceipt,
	MailboxDelivery,
	MailboxDeliveryUnavailable,
	Timestamp,
	requestDeliveryInterrupt,
} from '@humanlayer/channels-delivery'
import { RuntimeContext } from 'alchemy/RuntimeContext'
import { Clock, Context, Effect, Layer, Predicate, Schema } from 'effect'

import type { DeliveryRequest, DeliveryResponse } from './DeliveryControl'
import { DurableMailboxState, emptyMailboxState, mailboxStateKey } from './MailboxState'
import { MailboxStorage } from './MailboxStorage'
import { writeMailboxState } from './MailboxTransaction'

const unavailable = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
	effect.pipe(
		Effect.tapError((error) => Effect.logError('Cloudflare mailbox admission failed', error)),
		Effect.mapError(() => new MailboxDeliveryUnavailable({ reason: 'cloudflare_unavailable' })),
		Effect.catchDefect((defect) =>
			Effect.logError('Cloudflare mailbox admission failed', defect).pipe(
				Effect.andThen(Effect.fail(new MailboxDeliveryUnavailable({ reason: 'cloudflare_unavailable' }))),
			),
		),
	)

/** Build the admission RPC over the current Durable Object's persistent storage. */
export const makeDeliverFromDurableObjectStorage = Effect.gen(function* () {
	const storage = yield* MailboxStorage

	return Effect.fn('delivery.cloudflare_durable_object.deliver')(function* (input: DeliveryAdmission) {
		const admission = yield* Schema.decodeEffect(DeliveryAdmission)(input).pipe(
			Effect.catchTag('SchemaError', (error) =>
				Effect.logError('Cloudflare mailbox admission decode failed', error).pipe(
					Effect.andThen(Effect.die(error)),
				),
			),
		)
		const now = Timestamp.make(yield* Clock.currentTimeMillis)
		return yield* storage.transaction((transaction) =>
			Effect.gen(function* () {
				const eventKey = `event:${admission.eventId}`
				if (Predicate.isNotUndefined(yield* transaction.get(eventKey))) return { accepted: false }
				const stored = yield* transaction.get(mailboxStateKey)
				const current = Predicate.isUndefined(stored)
					? emptyMailboxState({ mailboxKey: deliveryMailboxKey(admission), provider: admission.provider })
					: yield* Schema.decodeUnknownEffect(DurableMailboxState)(stored).pipe(
							Effect.catchTag('SchemaError', (error) =>
								Effect.logError('Cloudflare mailbox state decode failed', error).pipe(
									Effect.andThen(Effect.die(error)),
								),
							),
						)
				/** An interrupting event marks the active delivery in this same write. */
				const deliveries = Predicate.isNotUndefined(admission.interrupt)
					? requestDeliveryInterrupt(current.deliveries, now)
					: current.deliveries
				/**
				 * A mailbox with an active delivery is woken by that delivery ending, not by new events. Its
				 * alarm is still put back to `readyAt`, so an event reaches a mailbox that lost its alarm.
				 */
				const wakesMailbox = Predicate.isNull(deliveries.active)
				const next = DurableMailboxState.make({
					...current,
					nextSequence: current.nextSequence + 1,
					waiting: [...current.waiting, { sequence: current.nextSequence, arrivedAt: now, admission }],
					deliveries: wakesMailbox ? { ...deliveries, readyAt: now } : deliveries,
				})
				yield* transaction.put(eventKey, current.nextSequence)
				yield* writeMailboxState(transaction, next)
				return { accepted: true }
			}),
		)
	})
})

export type DeliveryMailboxNamespace = {
	readonly getByName: (mailboxKey: string) => {
		readonly deliver: (
			admission: DeliveryAdmission,
		) => Effect.Effect<{ readonly accepted: boolean }, never, RuntimeContext>
		/** Read or change a delivery this mailbox owns. See `DeliveryControlAlchemyCloudflare`. */
		readonly deliveryRequest: (
			request: typeof DeliveryRequest.Encoded,
		) => Effect.Effect<typeof DeliveryResponse.Encoded, never, RuntimeContext>
	}
}

/** The application's mailbox Durable Object namespace, as the Worker's routes reach it. */
export class DeliveryMailboxes extends Context.Service<DeliveryMailboxes, DeliveryMailboxNamespace>()(
	'@humanlayer/channels-alchemy-cloudflare/DeliveryMailboxes',
) {}

/** Route host-level MailboxDelivery calls to the application-owned DO namespace. */
export const MailboxDeliveryAlchemyCloudflare = Layer.effect(
	MailboxDelivery,
	Effect.gen(function* () {
		const mailboxes = yield* DeliveryMailboxes
		return MailboxDelivery.of({
			deliver: (admission: DeliveryAdmission) => {
				const mailboxKey = deliveryMailboxKey(admission)
				return mailboxes
					.getByName(mailboxKey)
					.deliver(admission)
					.pipe(
						Effect.map(({ accepted }) => DeliveryReceipt.make({ mailboxKey, accepted })),
						Effect.provide(RuntimeContext.phantom),
						unavailable,
					)
			},
		})
	}),
)

export const layer = MailboxDeliveryAlchemyCloudflare
