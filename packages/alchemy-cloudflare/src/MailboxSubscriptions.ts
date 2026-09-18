import {
	MailboxSubscriptionCreatedResult,
	MailboxSubscriptionError,
	MailboxSubscriptionAlreadyExistsResult,
	type MailboxSubscriptionOperation,
	type MailboxSubscriptionResult,
	MailboxSubscriptions,
} from '@humanlayer/channels-delivery-next'
import * as Cloudflare from 'alchemy/Cloudflare'
import { RuntimeContext } from 'alchemy/RuntimeContext'
import { Effect, Layer, Predicate, Schema } from 'effect'

const marker = Schema.Literal(true)

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
		const state = yield* Cloudflare.DurableObjectState
		const runtimeContext = yield* RuntimeContext

		return MailboxSubscriptions.of({
			subscribe: ({ mailboxKey }) =>
				state.storage
					.transaction((transaction) =>
						Effect.gen(function* () {
							const key = subscriptionStorageKey(mailboxKey)
							const stored = yield* transaction.get<unknown>(key)

							if (Predicate.isUndefined(stored)) {
								yield* transaction.put(key, true)
								const result: MailboxSubscriptionResult = MailboxSubscriptionCreatedResult.make({})
								return result
							}

							Schema.decodeUnknownSync(marker)(stored)
							const result: MailboxSubscriptionResult = MailboxSubscriptionAlreadyExistsResult.make({})
							return result
						}),
					)
					.pipe(
						Effect.provideService(RuntimeContext, runtimeContext),
						(effect) => unavailable('subscribe', effect),
						Effect.withSpan('delivery.cloudflare.subscriptions.subscribe'),
					),

			isSubscribed: ({ mailboxKey }) =>
				Effect.gen(function* () {
					const stored = yield* state.storage.get<unknown>(subscriptionStorageKey(mailboxKey))

					if (Predicate.isUndefined(stored)) {
						return false
					}

					yield* Schema.decodeUnknownEffect(marker)(stored)

					return true
				}).pipe(
					Effect.provideService(RuntimeContext, runtimeContext),
					(effect) => unavailable('is_subscribed', effect),
					Effect.withSpan('delivery.cloudflare.subscriptions.is_subscribed'),
				),

			unsubscribe: ({ mailboxKey }) =>
				state.storage
					.delete(subscriptionStorageKey(mailboxKey))
					.pipe(
						Effect.asVoid,
						Effect.provideService(RuntimeContext, runtimeContext),
						(effect) => unavailable('unsubscribe', effect),
						Effect.withSpan('delivery.cloudflare.subscriptions.unsubscribe'),
					),
		})
	}),
)
