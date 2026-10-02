/**
 * This file defines `ChannelsRedis.make`: Redis mailbox storage as `Channels.make` takes it.
 */
import type { ChannelsStorage, DeliveryControlBackend } from '@humanlayer/channels-delivery-next'
import { Layer } from 'effect'

import { DeliveryControlBackendRedis } from './DeliveryControlBackend'
import { MailboxDeliveryRedis } from './MailboxDelivery'
import { MailboxProcessingBackendRedis } from './MailboxProcessingBackend'
import { MailboxSubscriptionsRedis } from './MailboxSubscriptions'

/**
 * @property claimLimit - the most due mailboxes one look reports
 * @property polling - how often to look for due mailboxes; Redis has nothing else to wake processing
 */
export type MakeOptions = {
	readonly claimLimit: number
	readonly polling: { readonly intervalMs: number }
}

/**
 * The application provides the `Redis` client. The storage supports handoff, so a bot on it can serve
 * `bot.deliveryApi` for remote workers.
 */
export const make = (options: MakeOptions) =>
	({
		polling: options.polling,
		layer: Layer.mergeAll(
			MailboxDeliveryRedis,
			MailboxProcessingBackendRedis({ claimLimit: options.claimLimit }),
			MailboxSubscriptionsRedis,
			DeliveryControlBackendRedis,
		),
	}) satisfies ChannelsStorage<unknown, unknown, DeliveryControlBackend>
