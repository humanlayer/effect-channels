import { Clock, Context, Effect, Layer, Predicate, Schema } from 'effect'

import { decodeDeliveryReference, DeliveryId, DeliveryReference } from './DeliveryReference.js'
import { activeBatches, currentMailbox, type Envelope, type Outcome, parseMailboxAddress } from './Mailbox.js'
import { DeliveryLocatorStore, MailboxStore } from './MailboxStore.js'
import {
	deliveryControlUnavailable,
	deliveryNotFound,
	deliveryOutcomeConflict,
	DeliveryControlUnavailable,
	DeliveryNotFound,
	DeliveryOutcomeConflict,
	DeliveryTerminalOutcome,
	DeliveryTerminalReceipt,
} from './protocol.js'

export {
	DeliveryControlUnavailable,
	DeliveryNotFound,
	DeliveryOutcomeConflict,
	DeliveryTerminalOutcome,
	DeliveryTerminalReceipt,
} from './protocol.js'
export type { DeliveryTerminalOutcome as DeliveryTerminalOutcomeType } from './protocol.js'

export const FinishDelivery = Schema.Struct({
	deliveryId: DeliveryId,
	outcome: DeliveryTerminalOutcome,
})
export interface FinishDelivery extends Schema.Schema.Type<typeof FinishDelivery> {}

export const ResolvedDelivery = Schema.Struct({
	deliveryId: DeliveryId,
	provider: Schema.NonEmptyString,
	installation: Schema.NonEmptyString,
	organizationId: Schema.NonEmptyString,
	definition: Schema.NonEmptyString,
	version: Schema.NonEmptyString,
	eventId: Schema.NonEmptyString,
	resource: Schema.String,
	payload: Schema.String,
})
export interface ResolvedDelivery extends Schema.Schema.Type<typeof ResolvedDelivery> {}

const ResolvedMetadata = Schema.Struct({
	organizationId: Schema.NonEmptyString,
	definition: Schema.NonEmptyString,
	version: Schema.NonEmptyString,
	eventId: Schema.NonEmptyString,
	resource: Schema.String,
	payload: Schema.String,
})

const resolutionMetadata = (envelope: Envelope | undefined, outcome: Outcome | undefined) => ({
	organizationId: envelope?.organizationId ?? outcome?.organizationId,
	definition: envelope?.definition ?? outcome?.definition,
	version: envelope?.version ?? outcome?.version,
	eventId: envelope?.eventId ?? outcome?.eventId,
	resource: envelope?.resource ?? outcome?.resource,
	payload: envelope?.payload ?? outcome?.payload,
})

export type DeliveryControlError = DeliveryNotFound | DeliveryOutcomeConflict | DeliveryControlUnavailable

export interface DeliveryControlService {
	readonly resolve: (input: {
		readonly deliveryId: DeliveryId
	}) => Effect.Effect<ResolvedDelivery, DeliveryControlError>
	readonly finish: (input: FinishDelivery) => Effect.Effect<DeliveryTerminalReceipt, DeliveryControlError>
}

export class DeliveryControl extends Context.Service<DeliveryControl, DeliveryControlService>()(
	'delivery/DeliveryControl',
) {
	static readonly layer = Layer.effect(
		DeliveryControl,
		Effect.gen(function* () {
			const store = yield* MailboxStore
			const locator = yield* DeliveryLocatorStore
			const referenceFor = Effect.fn('delivery.control.reference')(function* (input: {
				readonly deliveryId: DeliveryId
			}) {
				if (input.deliveryId.startsWith('delivery:v1:'))
					return yield* decodeDeliveryReference(input).pipe(
						Effect.mapError(() => deliveryNotFound(input.deliveryId)),
					)
				const mailboxKey = yield* locator
					.locateDelivery(input)
					.pipe(Effect.mapError(() => deliveryControlUnavailable('lookup')))
				if (mailboxKey === undefined) return yield* deliveryNotFound(input.deliveryId)
				return DeliveryReference.make({ deliveryId: input.deliveryId, mailboxKey })
			})
			const resolve = Effect.fn('delivery.control.resolve')(function* (input: {
				readonly deliveryId: DeliveryId
			}) {
				const parsed = yield* referenceFor(input)
				const snapshot = yield* store
					.loadMailbox({ key: parsed.mailboxKey })
					.pipe(Effect.mapError(() => deliveryControlUnavailable('lookup')))
				const batch =
					snapshot === undefined
						? undefined
						: activeBatches(snapshot.state).find((entry) => entry.deliveryId === parsed.deliveryId)
				const address = parseMailboxAddress(parsed.mailboxKey)
				const envelope = batch?.envelopes.at(-1)
				const now = yield* Clock.currentTimeMillis
				const outcome = snapshot?.state.outcomes.findLast(
					(entry) => entry.deliveryId === parsed.deliveryId && entry.expiresAt > now,
				)
				const metadata = resolutionMetadata(envelope, outcome)
				if (address === undefined || !Schema.is(ResolvedMetadata)(metadata))
					return yield* deliveryNotFound(parsed.deliveryId)
				return ResolvedDelivery.make({
					deliveryId: parsed.deliveryId,
					provider: address.provider,
					installation: address.installation,
					...metadata,
				})
			})
			const finish = Effect.fn('delivery.control.finish')(function* (input: FinishDelivery) {
				const parsed = yield* referenceFor(input)
				for (let attempt = 0; attempt <= 8; attempt++) {
					const snapshot = yield* store
						.loadMailbox({ key: parsed.mailboxKey })
						.pipe(Effect.mapError(() => deliveryControlUnavailable('lookup')))
					if (snapshot === undefined) return yield* deliveryNotFound(parsed.deliveryId)
					const state = currentMailbox(snapshot.state)
					const now = yield* Clock.currentTimeMillis
					const batch = activeBatches(state).find((entry) => entry.deliveryId === parsed.deliveryId)
					if (batch === undefined) {
						const outcome = state.outcomes.findLast(
							(entry) => entry.deliveryId === parsed.deliveryId && entry.expiresAt > now,
						)
						const recorded =
							outcome?.kind === 'completed' || outcome?.kind === 'failed' ? outcome.kind : undefined
						if (recorded === undefined) return yield* deliveryNotFound(parsed.deliveryId)
						if (recorded !== input.outcome)
							return yield* deliveryOutcomeConflict({
								deliveryId: parsed.deliveryId,
								recordedOutcome: recorded,
								requestedOutcome: input.outcome,
							})
						return DeliveryTerminalReceipt.make({
							deliveryId: parsed.deliveryId,
							outcome: recorded,
							status: 'already_recorded',
						})
					}
					const recorded = batch.stage?.terminalOutcome
					if (recorded !== undefined) {
						if (recorded !== input.outcome)
							return yield* deliveryOutcomeConflict({
								deliveryId: parsed.deliveryId,
								recordedOutcome: recorded,
								requestedOutcome: input.outcome,
							})
						return DeliveryTerminalReceipt.make({
							deliveryId: parsed.deliveryId,
							outcome: recorded,
							status: 'already_recorded',
						})
					}
					if (batch.stage === undefined) return yield* deliveryNotFound(parsed.deliveryId)
					const nextBatch = {
						...batch,
						stage: { ...batch.stage, terminalOutcome: input.outcome },
					}
					const batches = activeBatches(state).map((entry) => (entry === batch ? nextBatch : entry))
					const readyAt =
						Predicate.isTagged('External')(batch.stage) && batch.stage.cleanupOwner === null
							? Math.min(state.readyAt ?? now, now)
							: state.readyAt
					const committed = yield* store
						.commitMailbox({
							key: parsed.mailboxKey,
							expectedRevision: snapshot.revision,
							nextState: {
								...state,
								active: batches[0] ?? null,
								additionalActive: batches.slice(1),
								readyAt,
							},
						})
						.pipe(Effect.mapError(() => deliveryControlUnavailable('finish')))
					if (committed === 'committed')
						return DeliveryTerminalReceipt.make({
							deliveryId: parsed.deliveryId,
							outcome: input.outcome,
							status: 'accepted',
						})
				}
				return yield* deliveryControlUnavailable('finish')
			})
			return DeliveryControl.of({ resolve, finish })
		}),
	)
}
