/**
 * This file defines `ChannelsMemory.make`: in-memory mailbox storage as `Channels.make` takes it.
 * It is for tests and local development. Everything is lost when the process stops.
 */
import { Layer } from 'effect'

import type { DeliveryControlBackend } from './DeliveryControl'
import type { ChannelsStorage } from './ChannelsStorage'
import { MailboxBackendMemory } from './MailboxBackendMemory'
import { MailboxSubscriptionsMemory } from './MailboxSubscriptionsMemory'

/** @property polling - how often to look for due mailboxes; memory has nothing else to wake processing */
export type MakeOptions = {
	readonly polling: { readonly intervalMs: number }
}

export const make = (options: MakeOptions) =>
	({
		polling: options.polling,
		layer: Layer.merge(MailboxBackendMemory, MailboxSubscriptionsMemory),
	}) satisfies ChannelsStorage<never, never, DeliveryControlBackend>
