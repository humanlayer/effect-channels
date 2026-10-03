/**
 * This file defines `ChannelsSql.make`: Postgres mailbox storage as `Channels.make` takes it.
 */
import type { ChannelsStorage, DeliveryControlBackend } from '@humanlayer/channels-delivery'
import { Layer } from 'effect'

import { DeliveryControlBackendSql } from './DeliveryControlBackend'
import { MailboxDeliverySql } from './MailboxDelivery'
import { MailboxProcessingBackendSql } from './MailboxProcessingBackend'
import { MailboxSubscriptionsSql } from './MailboxSubscriptions'
import { MigrationsSql } from './Migrations'

/**
 * @property claimLimit - the most due mailboxes one look reports
 * @property runMigrations - create or update the tables once, before the storage's services are built
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
export const make = (options: MakeOptions) => {
	const services = Layer.mergeAll(
		MailboxDeliverySql,
		MailboxProcessingBackendSql({ claimLimit: options.claimLimit }),
		MailboxSubscriptionsSql,
		DeliveryControlBackendSql,
	)
	return {
		polling: options.polling,
		layer: options.runMigrations ? services.pipe(Layer.provide(MigrationsSql)) : services,
	} satisfies ChannelsStorage<unknown, unknown, DeliveryControlBackend>
}
