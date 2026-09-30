import * as NodeCrypto from '@effect/platform-node/NodeCrypto'
import { ChannelsCloudflare, DeliveryMailboxes } from '@humanlayer/channels-alchemy-cloudflare'
import * as Cloudflare from 'alchemy/Cloudflare'
import { Effect, Layer } from 'effect'
import { FetchHttpClient } from 'effect/unstable/http'

import { bot } from './Bot'
import { DeliveryMailbox, DeliveryMailboxLive } from './DeliveryMailboxDO'
import { FakeRemoteAgentLive } from './FakeRemoteAgentDO'

/** The Durable Objects this Worker hosts, and the services their code needs. */
const HostedObjectsLive = DeliveryMailboxLive.pipe(
	Layer.provideMerge(FakeRemoteAgentLive),
	Layer.provide(FetchHttpClient.layer),
	Layer.provideMerge(NodeCrypto.layer),
)

/** The mailbox namespace the bot's routes forward to. */
const DeliveryMailboxesLive = Layer.effect(DeliveryMailboxes, DeliveryMailbox)

/** Provider webhooks and the delivery API. Leave `bot.deliveryApi` out to serve webhooks only. */
const RoutesLive = Layer.merge(bot.routes, bot.deliveryApi).pipe(Layer.provide(DeliveryMailboxesLive))

export default Cloudflare.Worker(
	'IngressWorker',
	{ main: import.meta.url },
	Effect.gen(function* () {
		const fetch = yield* ChannelsCloudflare.serve(RoutesLive)
		return { fetch }
	}).pipe(Effect.provide(HostedObjectsLive)),
)
