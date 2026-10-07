import * as NodeCrypto from '@effect/platform-node/NodeCrypto'
import { DeliveryMailboxes } from '@humanlayer/channels-alchemy-cloudflare'
import * as Cloudflare from 'alchemy/Cloudflare'
import { Effect, Layer } from 'effect'
import { FetchHttpClient } from 'effect/http'

import { bot } from './Bot'
import { DeliveryMailbox, DeliveryMailboxDOLive } from './DeliveryMailboxDO'

/** Give Channels access to this application's Durable Object mailbox namespace. */
const ChannelsDeliveryMailboxesLive = Layer.effect(DeliveryMailboxes, DeliveryMailbox)

/** Everything the Worker and its mailbox Durable Objects need. */
const WorkerLive = ChannelsDeliveryMailboxesLive.pipe(
	Layer.provideMerge(DeliveryMailboxDOLive),
	Layer.provideMerge(FetchHttpClient.layer),
	Layer.provideMerge(NodeCrypto.layer),
)

/**
 * The Worker and the Durable Objects it hosts run at one pinned compatibility date, so an Alchemy
 * upgrade cannot change their behavior. 2026-10-01 turns on `durable_object_io_tasks_prevent_eviction`:
 * pending I/O, such as an RPC to another Durable Object or a timer, keeps an object alive for up to 15
 * minutes. Listing that flag as well is an error, because the date already includes it.
 */
export default Cloudflare.Worker(
	'IngressWorker',
	{ main: import.meta.url, compatibility: { date: '2026-10-01' }, name: 'humanlayer-channels-app' },
	Effect.gen(function* () {
		const fetch = yield* bot.fetch
		return { fetch }
	}).pipe(Effect.provide(WorkerLive)),
)
