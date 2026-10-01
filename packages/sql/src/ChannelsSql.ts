/**
 * This file defines `ChannelsSql.make`: Postgres mailbox storage as `Channels.make` takes it.
 */
import type { ChannelsStorage, DeliveryControlBackend } from '@humanlayer/channels-delivery-next'
import { Layer } from 'effect'

import { DeliveryControlBackendSql } from './DeliveryControlBackend'
import { MailboxDeliverySql } from './MailboxDelivery'
import { MailboxProcessingBackendSql } from './MailboxProcessingBackend'
import { MailboxSubscriptionsSql } from './MailboxSubscriptions'

/**
 * @property claimLimit - the most due mailboxes one look reports
 * @property runMigrations - create the tables when the storage is built
 * @property polling - how often to look for due mailboxes; Postgres has nothing else to wake processing
 */
export type MakeOptions = {
	readonly claimLimit: number
	readonly runMigrations: boolean
	readonly polling: { readonly intervalMs: number }
}

/**
 * The application provides the `SqlClient`. The storage supports handoff, so a bot on it can serve
 * `bot.deliveryApi` for remote workers.
 */
export const make = (options: MakeOptions) =>
	({
		polling: options.polling,
		layer: Layer.mergeAll(
			MailboxDeliverySql({ runMigrations: options.runMigrations }),
			MailboxProcessingBackendSql({ claimLimit: options.claimLimit, runMigrations: options.runMigrations }),
			MailboxSubscriptionsSql({ runMigrations: options.runMigrations }),
			DeliveryControlBackendSql,
		),
	}) satisfies ChannelsStorage<unknown, unknown, DeliveryControlBackend>
