/**
 * This file defines what a storage package hands to `Channels.make`.
 */
import type { Layer } from 'effect'

import type { MailboxDelivery } from './MailboxDelivery'
import type { MailboxProcessingBackend, MailboxProcessingOptions } from './MailboxProcessing'
import type { MailboxSubscriptions } from './MailboxSubscriptions'

/**
 * One place to keep mailboxes, such as Postgres or Redis.
 *
 * @property polling - how processing is woken; stores with no wake-up of their own poll
 * @property layer - the three storage services; `R` is the store's client, such as `SqlClient`
 */
export type ChannelsStorage<E = never, R = never> = {
	readonly polling: MailboxProcessingOptions['polling']
	readonly layer: Layer.Layer<MailboxDelivery | MailboxProcessingBackend | MailboxSubscriptions, E, R>
}
