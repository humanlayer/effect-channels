import { Context, Effect, Schema } from 'effect'

import { MailboxSnapshot, MailboxState } from './Mailbox'
import { DeliveryId } from './protocol'

export class MailboxStoreError extends Schema.TaggedError<MailboxStoreError>()('MailboxStoreError', {
	operation: Schema.Literals(['load', 'commit', 'scan', 'locate']),
}) {}

export const LoadMailbox = Schema.Struct({ key: Schema.NonEmptyString })
export type LoadMailbox = typeof LoadMailbox.Type

export const CommitMailbox = Schema.Struct({
	key: Schema.NonEmptyString,
	expectedRevision: Schema.NullOr(Schema.Natural),
	nextState: MailboxState,
})
export type CommitMailbox = typeof CommitMailbox.Type

export class MailboxStore extends Context.Service<
	MailboxStore,
	{
		readonly loadMailbox: (input: LoadMailbox) => Effect.Effect<MailboxSnapshot | undefined, MailboxStoreError>
		readonly commitMailbox: (input: CommitMailbox) => Effect.Effect<'committed' | 'conflict', MailboxStoreError>
	}
>()('delivery/MailboxStore') {}

export const ScanReady = Schema.Struct({
	prefix: Schema.String,
	now: Schema.Finite,
	limit: Schema.Int.check(Schema.isGreaterThan(0)),
})
export type ScanReady = typeof ScanReady.Type

export class MailboxReadiness extends Context.Service<
	MailboxReadiness,
	{ readonly scanReady: (input: ScanReady) => Effect.Effect<ReadonlyArray<string>, MailboxStoreError> }
>()('delivery/MailboxReadiness') {}

export const LocateDelivery = Schema.Struct({ deliveryId: DeliveryId })
export type LocateDelivery = typeof LocateDelivery.Type

export class DeliveryLocatorStore extends Context.Service<
	DeliveryLocatorStore,
	{ readonly locateDelivery: (input: LocateDelivery) => Effect.Effect<string | undefined, MailboxStoreError> }
>()('delivery/DeliveryLocatorStore') {}
