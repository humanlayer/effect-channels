import {
	MailboxSubscriptionCreatedResult,
	MailboxSubscriptionError,
	MailboxSubscriptionAlreadyExistsResult,
	type MailboxSubscriptionOperation,
	type MailboxSubscriptionResult,
	MailboxSubscriptions,
} from '@humanlayer/channels-delivery-next'
import { Effect, Exit, Layer, Predicate, Schema } from 'effect'

import { MailboxStorage } from './MailboxStorage'

const decodeMarker = Schema.decodeUnknownEffect(Schema.Literal(true))

const subscriptionStorageKey = (mailboxKey: string) => `mailbox-subscription:${mailboxKey}`

const unavailable = <A, E, R>(operation: MailboxSubscriptionOperation, effect: Effect.Effect<A, E, R>) =>
	effect.pipe(
		Effect.tapError((error) =>
			Effect.logError('Cloudflare mailbox subscription operation failed', error).pipe(
				Effect.annotateLogs({ operation }),
			),
		),
		Effect.mapError(
			() =>
				new MailboxSubscriptionError({
					operation,
					reason: 'cloudflare_unavailable',
				}),
		),
		Effect.catchDefect((defect) =>
			Effect.logError('Cloudflare mailbox subscription operation failed', defect).pipe(
				Effect.annotateLogs({ operation }),
				Effect.andThen(
					Effect.fail(
						new MailboxSubscriptionError({
							operation,
							reason: 'cloudflare_unavailable',
						}),
					),
				),
			),
		),
	)

export const MailboxSubscriptionsFromDurableObjectStorage = Layer.effect(
	MailboxSubscriptions,
	Effect.gen(function* () {
		const storage = yield* MailboxStorage

		return MailboxSubscriptions.of({
			subscribe: ({ mailboxKey }) =>
				storage
					.transaction((transaction) =>
						Effect.gen(function* () {
							const key = subscriptionStorageKey(mailboxKey)
							const stored = yield* transaction.get(key)

							if (Predicate.isUndefined(stored)) {
								yield* transaction.put(key, true)
								return Exit.succeed<MailboxSubscriptionResult>(
									MailboxSubscriptionCreatedResult.make({}),
								)
							}

							const decoded = yield* decodeMarker(stored).pipe(Effect.exit)
							if (Exit.isFailure(decoded)) return Exit.failCause(decoded.cause)
							return Exit.succeed<MailboxSubscriptionResult>(
								MailboxSubscriptionAlreadyExistsResult.make({}),
							)
						}),
					)
					.pipe(
						Effect.flatten,
						(effect) => unavailable('subscribe', effect),
						Effect.withSpan('delivery.cloudflare.subscriptions.subscribe'),
					),

			isSubscribed: Effect.fn('delivery.cloudflare.subscriptions.is_subscribed')(
				function* ({ mailboxKey }) {
					const stored = yield* storage.get(subscriptionStorageKey(mailboxKey))

					if (Predicate.isUndefined(stored)) {
						return false
					}

					yield* decodeMarker(stored)

					return true
				},
				(effect) => unavailable('is_subscribed', effect),
			),

			unsubscribe: ({ mailboxKey }) =>
				storage
					.delete(subscriptionStorageKey(mailboxKey))
					.pipe(
						Effect.asVoid,
						(effect) => unavailable('unsubscribe', effect),
						Effect.withSpan('delivery.cloudflare.subscriptions.unsubscribe'),
					),
		})
	}),
)
