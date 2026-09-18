/**
 * This file defines a service used for tracking the subscribed state of a thread
 */

import { Context, Effect, Schema } from 'effect'

export const MailboxSubscriptionInput = Schema.Struct({ mailboxKey: Schema.NonEmptyString })
export type MailboxSubscriptionInput = typeof MailboxSubscriptionInput.Type

export const MailboxSubscriptionCreatedResult = Schema.TaggedStruct('Created', {})
export type MailboxSubscriptionCreatedResult = typeof MailboxSubscriptionCreatedResult.Type

export const MailboxSubscriptionAlreadyExistsResult = Schema.TaggedStruct('AlreadyExists', {})
export type MailboxSubscriptionAlreadyExistsResult = typeof MailboxSubscriptionAlreadyExistsResult.Type

export const MailboxSubscriptionResult = Schema.Union([
	MailboxSubscriptionCreatedResult,
	MailboxSubscriptionAlreadyExistsResult,
])
export type MailboxSubscriptionResult = typeof MailboxSubscriptionResult.Type

export const MailboxSubscriptionOperation = Schema.Literals(['migrate', 'subscribe', 'is_subscribed', 'unsubscribe'])
export type MailboxSubscriptionOperation = typeof MailboxSubscriptionOperation.Type

export class MailboxSubscriptionError extends Schema.TaggedError<MailboxSubscriptionError>()(
	'MailboxSubscriptionError',
	{
		operation: MailboxSubscriptionOperation,
		reason: Schema.NonEmptyString,
	},
) {}

/**
 * This service fronts storage (DO/SQL/Redis ) for tracking whether a thread is subscribed to or not & updating subscriptions
 */
export class MailboxSubscriptions extends Context.Service<
	MailboxSubscriptions,
	{
		readonly subscribe: (
			input: MailboxSubscriptionInput,
		) => Effect.Effect<MailboxSubscriptionResult, MailboxSubscriptionError>
		readonly isSubscribed: (input: MailboxSubscriptionInput) => Effect.Effect<boolean, MailboxSubscriptionError>
		readonly unsubscribe: (input: MailboxSubscriptionInput) => Effect.Effect<void, MailboxSubscriptionError>
	}
>()('@humanlayer/delivery-next/MailboxSubscriptions') {}
