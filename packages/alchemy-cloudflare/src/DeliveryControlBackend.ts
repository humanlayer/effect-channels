/**
 * This file defines the store half of delivery control over a mailbox Durable Object's storage.
 *
 * The token check and the change run in one storage transaction, through the shared
 * `DeliveryLifecycle` rules, and the alarm moves with the mailbox's new `readyAt`: a result or a link
 * saves its output and wakes the mailbox at once to send it.
 */
import {
	DeliveryControlBackend,
	DeliveryControlUnavailable,
	DeliveryNotFound,
	applyDeliverySlotMutation,
	readDeliverySlotStatus,
	type ApplyDeliveryMutation,
	type ReadDeliveryStatus,
} from '@humanlayer/channels-delivery-next'
import { Array as Arr, Clock, Effect, Layer, Option, Predicate, Result } from 'effect'

import { mailboxStateKey } from './MailboxState'
import { MailboxStorage } from './MailboxStorage'
import { changeDeliveries, decodeMailboxState } from './MailboxTransaction'

/** Log the raw storage failure, then narrow it to `DeliveryControlUnavailable`. */
const narrowToUnavailable =
	(message: string) =>
	<A, E, R>(effect: Effect.Effect<A, E, R>) =>
		effect.pipe(
			Effect.tapError((error) => Effect.logError(message, error)),
			Effect.mapError(() => new DeliveryControlUnavailable({ reason: 'cloudflare_unavailable' })),
			Effect.catchDefect((defect) =>
				Effect.logError(message, defect).pipe(
					Effect.andThen(Effect.fail(new DeliveryControlUnavailable({ reason: 'cloudflare_unavailable' }))),
				),
			),
		)

/** Delivery control over the current Durable Object's persistent storage. */
export const makeDeliveryControlBackendFromDurableObjectStorage = Effect.gen(function* () {
	const storage = yield* MailboxStorage

	return DeliveryControlBackend.of({
		readDeliveryStatus: Effect.fn('delivery.cloudflare.read_delivery_status')(function* (input: ReadDeliveryStatus) {
			const now = yield* Clock.currentTimeMillis
			const stored = yield* storage
				.get(mailboxStateKey)
				.pipe(
					Effect.flatMap((value) =>
						Predicate.isUndefined(value) ? Effect.succeedNone : Effect.asSome(decodeMailboxState(value)),
					),
					narrowToUnavailable('Cloudflare delivery status read failed'),
				)
			const mailbox = Option.filter(stored, ({ mailboxKey }) => mailboxKey === input.reference.mailboxKey)
			if (Option.isNone(mailbox)) return yield* new DeliveryNotFound()
			return yield* Effect.fromResult(readDeliverySlotStatus(mailbox.value.deliveries, { ...input, now }))
		}),

		applyDeliveryMutation: Effect.fn('delivery.cloudflare.apply_delivery_mutation')(function* (
			input: ApplyDeliveryMutation,
		) {
			const now = yield* Clock.currentTimeMillis
			const recorded = yield* changeDeliveries(storage, {
				onMissing: new DeliveryNotFound(),
				change: (current) =>
					current.mailboxKey === input.reference.mailboxKey
						? Result.map(
								applyDeliverySlotMutation(current.deliveries, {
									...input,
									now,
									hasWaiting: Arr.isReadonlyArrayNonEmpty(current.waiting),
								}),
								({ slot, receipt }) => ({ slot, value: receipt }),
							)
						: Result.fail(new DeliveryNotFound()),
			}).pipe(narrowToUnavailable('Cloudflare delivery mutation failed'))
			return yield* Effect.fromResult(recorded)
		}),
	})
})

export const DeliveryControlBackendFromDurableObjectStorage = Layer.effect(
	DeliveryControlBackend,
	makeDeliveryControlBackendFromDurableObjectStorage,
)
