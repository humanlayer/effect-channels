import { Effect, Layer, Ref } from 'effect'

import {
	MailboxSubscriptionAlreadyExistsResult,
	MailboxSubscriptionCreatedResult,
	type MailboxSubscriptionResult,
	MailboxSubscriptions,
} from './MailboxSubscriptions'

/** In-memory subscription storage, for tests and local development. */
export const MailboxSubscriptionsMemory = Layer.effect(
	MailboxSubscriptions,
	Effect.gen(function* () {
		const subscriptions = yield* Ref.make(new Set<string>())

		return MailboxSubscriptions.of({
			subscribe: ({ mailboxKey }) =>
				Ref.modify(subscriptions, (current): readonly [MailboxSubscriptionResult, Set<string>] => {
					if (current.has(mailboxKey)) {
						return [MailboxSubscriptionAlreadyExistsResult.make({}), current]
					}
					const updated = new Set(current)
					updated.add(mailboxKey)
					return [MailboxSubscriptionCreatedResult.make({}), updated]
				}),
			isSubscribed: ({ mailboxKey }) =>
				Ref.get(subscriptions).pipe(Effect.map((current) => current.has(mailboxKey))),
			unsubscribe: ({ mailboxKey }) =>
				Ref.update(subscriptions, (current) => {
					if (!current.has(mailboxKey)) return current
					const updated = new Set(current)
					updated.delete(mailboxKey)
					return updated
				}),
		})
	}),
)
