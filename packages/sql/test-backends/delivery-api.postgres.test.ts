/**
 * The delivery API over Postgres, end to end: `Channels.make` with `ChannelsSql.make` storage serves a
 * provider webhook and `bot.deliveryApi`, polls the store on its own, and a remote worker finishes a
 * handed-off delivery through the generated client.
 */
import { deliveryApiScenario } from '../../delivery/test/delivery-api-scenario'
import { ChannelsSql } from '../src'
import { client, emptyTables } from './postgres'

deliveryApiScenario('sql', {
	namespace: 'channels-sql-test',
	storage: ChannelsSql.make({ claimLimit: 10, runMigrations: true, polling: { intervalMs: 10 } }),
	client,
	empty: emptyTables,
})
