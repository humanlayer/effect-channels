import { Clock, Context, Effect, Layer, Schema } from 'effect'

import { DeliveryPolicy } from './DeliveryPolicy'
import {
	activeBatches,
	currentMailbox,
	emptyMailbox,
	eventIdentity,
	mailboxCapacityUsage,
	Outcome,
	retainedOutcomes,
} from './Mailbox'
import { MailboxStore, type MailboxStoreError } from './MailboxStore'

export const InterruptDelivery = Schema.Struct({
	key: Schema.NonEmptyString,
	controlId: Schema.NonEmptyString,
	eventId: Schema.optionalKey(Schema.NonEmptyString),
	definition: Schema.NonEmptyString,
	policy: DeliveryPolicy,
})
export interface InterruptDelivery extends Schema.Schema.Type<typeof InterruptDelivery> {}

export class DeliveryInterruptionError extends Schema.TaggedError<DeliveryInterruptionError>()(
	'DeliveryInterruptionError',
	{ reason: Schema.Literals(['capacity', 'conflict']) },
) {}

/** Shared addressed cancellation transition used by server and Durable Object client implementations. */
export const interruptDelivery = Effect.fn('delivery.interruption.interrupt')(function* (input: InterruptDelivery) {
	const encodedSize = new TextEncoder().encode(input.controlId + input.key + (input.eventId ?? '')).byteLength
	if (encodedSize > input.policy.maxPayloadBytes) return yield* DeliveryInterruptionError.make({ reason: 'capacity' })
	const store = yield* MailboxStore
	const initial = yield* store.loadMailbox({ key: input.key })
	const target =
		initial === undefined
			? undefined
			: activeBatches(initial.state).find(
					(batch) =>
						input.eventId === undefined ||
						batch.envelopes.some(
							(envelope) =>
								envelope.definition === input.definition && envelope.eventId === input.eventId,
						),
				)
	const identity = `control:${input.controlId}`
	for (let attempt = 0; attempt <= input.policy.conflictRetries; attempt++) {
		const snapshot = yield* store.loadMailbox({ key: input.key })
		const now = yield* Clock.currentTimeMillis
		const state = currentMailbox(snapshot?.state ?? emptyMailbox())
		const current = { ...state, outcomes: retainedOutcomes(state, now) }
		if (current.outcomes.some((outcome) => outcome.identity === identity)) return false
		if (mailboxCapacityUsage(current) >= input.policy.maxOutcomes)
			return yield* DeliveryInterruptionError.make({ reason: 'capacity' })
		const batches = activeBatches(current)
		const targeted =
			target === undefined
				? undefined
				: batches.find(
						(batch) =>
							batch.owner === target.owner &&
							batch.attempt === target.attempt &&
							batch.envelopes[0].acceptedAt === target.envelopes[0].acceptedAt &&
							eventIdentity(batch.envelopes[0]) === eventIdentity(target.envelopes[0]),
					)
		const nextState = {
			...current,
			active: batches.map((batch) => (batch === targeted ? { ...batch, cancelled: true } : batch))[0] ?? null,
			additionalActive: batches
				.map((batch) => (batch === targeted ? { ...batch, cancelled: true } : batch))
				.slice(1),
			outcomes: [
				...current.outcomes,
				Outcome.make({
					identity,
					kind: 'control',
					expiresAt: now + input.policy.retentionMs,
					cancellationTarget:
						targeted === undefined
							? null
							: {
									identity: eventIdentity(targeted.envelopes[0]),
									acceptedAt: targeted.envelopes[0].acceptedAt,
								},
				}),
			],
		}
		const committed = yield* store.commitMailbox({
			key: input.key,
			expectedRevision: snapshot?.revision ?? null,
			nextState,
		})
		if (committed === 'committed') return targeted !== undefined
	}
	return yield* DeliveryInterruptionError.make({ reason: 'conflict' })
})

export class DeliveryInterruption extends Context.Service<
	DeliveryInterruption,
	{
		readonly interrupt: (
			input: InterruptDelivery,
		) => Effect.Effect<boolean, DeliveryInterruptionError | MailboxStoreError>
	}
>()('delivery/DeliveryInterruption') {
	static readonly layerMailboxStore = Layer.effect(
		DeliveryInterruption,
		Effect.gen(function* () {
			const store = yield* MailboxStore
			const clock = yield* Clock.Clock
			return DeliveryInterruption.of({
				interrupt: (input) =>
					interruptDelivery(input).pipe(
						Effect.provideService(MailboxStore, store),
						Effect.provideService(Clock.Clock, clock),
					),
			})
		}),
	)
}
