/**
 * This file defines what a storage package hands to `Channels.make`.
 */
import type { Layer } from 'effect'

import type { DeliveryControlBackend } from './DeliveryControl'
import type { MailboxDelivery } from './MailboxDelivery'
import type { MailboxProcessingBackend, MailboxProcessingOptions } from './MailboxProcessing'
import type { MailboxSubscriptions } from './MailboxSubscriptions'

/** The services every store provides. */
export type ChannelsStorageServices = MailboxDelivery | MailboxProcessingBackend | MailboxSubscriptions

/**
 * One place to keep mailboxes, such as Postgres or Redis.
 *
 * @property polling - how processing is woken; stores with no wake-up of their own poll
 * @property layer - the storage services; `R` is the store's client, such as `SqlClient`.
 * `Control` is `DeliveryControlBackend` for stores that let remote workers finish handed-off
 * deliveries, and `never` for stores that do not yet.
 */
export type ChannelsStorage<E = never, R = never, Control extends DeliveryControlBackend = never> = {
	readonly polling: MailboxProcessingOptions['polling']
	readonly layer: Layer.Layer<ChannelsStorageServices | Control, E, R>
}
