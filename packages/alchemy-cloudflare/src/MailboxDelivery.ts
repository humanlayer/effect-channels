import {
	deliveryMailboxKey,
	DeliveryAdmission,
	DeliveryReceipt,
	MailboxDelivery,
	MailboxDeliveryUnavailable,
	Timestamp,
} from '@humanlayer/channels-delivery-next'
import * as Cloudflare from 'alchemy/Cloudflare'
import { RuntimeContext } from 'alchemy/RuntimeContext'
import { Clock, Effect, Layer, Predicate, Schema } from 'effect'

import { DurableMailboxState, emptyMailboxState, mailboxStateKey } from './MailboxState'

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
	const state = yield* Cloudflare.DurableObjectState

	return Effect.fn('delivery.cloudflare_durable_object.deliver')(function* (input: DeliveryAdmission) {
		const admission = yield* Schema.decodeEffect(DeliveryAdmission)(input).pipe(
			Effect.catchTag('SchemaError', (error) =>
				Effect.logError('Cloudflare mailbox admission decode failed', error).pipe(
					Effect.andThen(Effect.die(error)),
				),
			),
		)
		const now = Timestamp.make(yield* Clock.currentTimeMillis)
		return yield* state.storage.transaction((transaction) =>
			Effect.gen(function* () {
				const eventKey = `event:${admission.eventId}`
				if (Predicate.isNotUndefined(yield* transaction.get<number>(eventKey))) return { accepted: false }
				const stored = yield* transaction.get(mailboxStateKey)
				const current = Predicate.isUndefined(stored)
					? emptyMailboxState(deliveryMailboxKey(admission))
					: yield* Schema.decodeUnknownEffect(DurableMailboxState)(stored).pipe(
							Effect.catchTag('SchemaError', (error) =>
								Effect.logError('Cloudflare mailbox state decode failed', error).pipe(
									Effect.andThen(Effect.die(error)),
								),
							),
						)
				const next = DurableMailboxState.make({
					...current,
					nextSequence: current.nextSequence + 1,
					pending: [...current.pending, admission],
					readyAt: current.status === 'idle' ? now : current.readyAt,
				})
				yield* transaction.put({ [eventKey]: current.nextSequence, [mailboxStateKey]: next })
				if (Predicate.isNotNull(next.readyAt)) yield* transaction.setAlarm(next.readyAt)
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
	}
}

/** Route host-level MailboxDelivery calls to the application-owned DO namespace. */
export const MailboxDeliveryAlchemyCloudflare = (mailboxes: DeliveryMailboxNamespace) =>
	Layer.succeed(
		MailboxDelivery,
		MailboxDelivery.of({
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
		}),
	)

export const layer = MailboxDeliveryAlchemyCloudflare
