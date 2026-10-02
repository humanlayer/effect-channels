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
import { Array as Arr, Clock, Effect, Layer, Option, Predicate, type Schema } from 'effect'

import { mailboxStateKey } from './MailboxState'
import { MailboxStorage } from './MailboxStorage'
import { changeDeliveries, decodeMailboxState } from './MailboxTransaction'

/** Log a stored mailbox that cannot be decoded, where it is read, then narrow it to `DeliveryControlUnavailable`. */
const undecodable = (message: string) => (error: Schema.SchemaError) =>
	Effect.logError(message, error).pipe(
		Effect.andThen(Effect.fail(new DeliveryControlUnavailable({ reason: 'cloudflare_unavailable' }))),
	)

/**
 * Storage failures arrive as defects (see `MailboxStorage`): log one, then narrow it to
 * `DeliveryControlUnavailable`. Typed errors, such as a closed delivery, are not touched.
 */
const narrowStorageDefect =
	(message: string) =>
	<A, E, R>(effect: Effect.Effect<A, E, R>) =>
		effect.pipe(
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
						Predicate.isUndefined(value)
							? Effect.succeedNone
							: Effect.asSome(
									decodeMailboxState(value).pipe(
										Effect.catchTag('SchemaError', undecodable('Cloudflare delivery status read failed')),
									),
								),
					),
					narrowStorageDefect('Cloudflare delivery status read failed'),
				)
			const mailbox = Option.filter(stored, ({ mailboxKey }) => mailboxKey === input.reference.mailboxKey)
			if (Option.isNone(mailbox)) return yield* new DeliveryNotFound()
			return yield* readDeliverySlotStatus(mailbox.value.deliveries, { ...input, now })
		}),

		applyDeliveryMutation: Effect.fn('delivery.cloudflare.apply_delivery_mutation')(function* (
			input: ApplyDeliveryMutation,
		) {
			const now = yield* Clock.currentTimeMillis
			return yield* changeDeliveries(storage, {
				onMissing: new DeliveryNotFound(),
				change: (current) =>
					current.mailboxKey === input.reference.mailboxKey
						? applyDeliverySlotMutation(current.deliveries, {
								...input,
								now,
								hasWaiting: Arr.isReadonlyArrayNonEmpty(current.waiting),
							}).pipe(Effect.map(({ slot, receipt }) => ({ slot, value: receipt })))
						: Effect.fail(new DeliveryNotFound()),
				onUndecodable: undecodable('Cloudflare delivery mutation failed'),
			}).pipe(narrowStorageDefect('Cloudflare delivery mutation failed'))
		}),
	})
})

export const DeliveryControlBackendFromDurableObjectStorage = Layer.effect(
	DeliveryControlBackend,
	makeDeliveryControlBackendFromDurableObjectStorage,
)
