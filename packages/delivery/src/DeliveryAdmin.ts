import { Clock, Context, Effect, Layer, Predicate, Schema } from 'effect'

import { deliveryIdFromOperationId, DeliveryOperation, PendingDeliveryOperation } from './DeliveryOperation.js'
import { decodeDeliveryReference, DeliveryReference } from './DeliveryReference.js'
import { currentMailbox } from './Mailbox.js'
import { DeliveryLocatorStore, MailboxStore } from './MailboxStore.js'
import { DeliveryOperationId } from './protocol.js'

export class DeliveryOperationNotFound extends Schema.TaggedError<DeliveryOperationNotFound>()(
	'DeliveryOperationNotFound',
	{ operationId: DeliveryOperationId, message: Schema.String },
) {}
export class DeliveryOperationConflict extends Schema.TaggedError<DeliveryOperationConflict>()(
	'DeliveryOperationConflict',
	{ operationId: DeliveryOperationId, message: Schema.String },
) {}
export class DeliveryAdminUnavailable extends Schema.TaggedError<DeliveryAdminUnavailable>()(
	'DeliveryAdminUnavailable',
	{ operation: Schema.String, message: Schema.String },
) {}

export type DeliveryAdminError = DeliveryOperationNotFound | DeliveryOperationConflict | DeliveryAdminUnavailable

const notFound = (operationId: DeliveryOperationId) =>
	DeliveryOperationNotFound.make({ operationId, message: 'The requested delivery operation was not found.' })
const unavailable = (operation: string) =>
	DeliveryAdminUnavailable.make({ operation, message: 'Delivery administration is temporarily unavailable.' })

export class DeliveryAdmin extends Context.Service<
	DeliveryAdmin,
	{
		readonly inspect: (input: {
			readonly operationId: DeliveryOperationId
		}) => Effect.Effect<DeliveryOperation, DeliveryAdminError>
		readonly redeliver: (input: {
			readonly operationId: DeliveryOperationId
		}) => Effect.Effect<DeliveryOperation, DeliveryAdminError>
	}
>()('delivery/DeliveryAdmin') {
	static readonly layer = Layer.effect(
		DeliveryAdmin,
		Effect.gen(function* () {
			const store = yield* MailboxStore
			const locator = yield* DeliveryLocatorStore
			const referenceFor = Effect.fn('delivery.admin.reference')(function* (input: {
				readonly operationId: DeliveryOperationId
			}) {
				const deliveryId = yield* deliveryIdFromOperationId(input.operationId).pipe(
					Effect.mapError(() => notFound(input.operationId)),
				)
				if (deliveryId.startsWith('delivery:v1:'))
					return yield* decodeDeliveryReference({ deliveryId }).pipe(
						Effect.mapError(() => notFound(input.operationId)),
					)
				const mailboxKey = yield* locator
					.locateDelivery({ deliveryId })
					.pipe(Effect.mapError(() => unavailable('lookup')))
				if (mailboxKey === undefined) return yield* notFound(input.operationId)
				return DeliveryReference.make({ deliveryId, mailboxKey })
			})
			const inspect = Effect.fn('delivery.admin.inspect')(function* (input: {
				readonly operationId: DeliveryOperationId
			}) {
				const reference = yield* referenceFor(input)
				const snapshot = yield* store
					.loadMailbox({ key: reference.mailboxKey })
					.pipe(Effect.mapError(() => unavailable('inspect')))
				const operation =
					snapshot === undefined
						? undefined
						: currentMailbox(snapshot.state).operations?.find(
								(entry) => entry.operationId === input.operationId,
							)
				return operation ?? (yield* notFound(input.operationId))
			})
			const redeliver = Effect.fn('delivery.admin.redeliver')(function* (input: {
				readonly operationId: DeliveryOperationId
			}) {
				const reference = yield* referenceFor(input)
				for (let attempt = 0; attempt <= 8; attempt++) {
					const snapshot = yield* store
						.loadMailbox({ key: reference.mailboxKey })
						.pipe(Effect.mapError(() => unavailable('redeliver')))
					if (snapshot === undefined) return yield* notFound(input.operationId)
					const state = currentMailbox(snapshot.state)
					const operation = state.operations?.find((entry) => entry.operationId === input.operationId)
					if (operation === undefined) return yield* notFound(input.operationId)
					if (!Predicate.isTagged('DeliveryFailed')(operation.state))
						return yield* DeliveryOperationConflict.make({
							operationId: input.operationId,
							message: 'Only a delivery_failed operation can be explicitly redelivered.',
						})
					const now = yield* Clock.currentTimeMillis
					const redelivery = {
						...operation,
						state: PendingDeliveryOperation.make({
							attempt: 0,
							readyAt: now,
							hadAmbiguousAttempt: operation.state.hadAmbiguousAttempt,
						}),
					}
					const committed = yield* store
						.commitMailbox({
							key: reference.mailboxKey,
							expectedRevision: snapshot.revision,
							nextState: {
								...state,
								operations: state.operations?.map((entry) =>
									entry === operation ? redelivery : entry,
								),
								readyAt: Math.min(state.readyAt ?? now, now),
							},
						})
						.pipe(Effect.mapError(() => unavailable('redeliver')))
					if (committed === 'committed') return redelivery
				}
				return yield* unavailable('redeliver')
			})
			return DeliveryAdmin.of({ inspect, redeliver })
		}),
	)
}
