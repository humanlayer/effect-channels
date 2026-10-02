import * as NodeCrypto from '@effect/platform-node/NodeCrypto'
import { ChannelsCloudflare, DeliveryMailboxes } from '@humanlayer/channels-alchemy-cloudflare'
import * as Cloudflare from 'alchemy/Cloudflare'
import { Effect, Layer, Schema } from 'effect'
import { FetchHttpClient, HttpRouter, HttpServerResponse } from 'effect/unstable/http'

import { bot } from './Bot'
import { DeliveryMailbox, DeliveryMailboxLive } from './DeliveryMailboxDO'
import { FakeRemoteAgent, FakeRemoteAgentLive } from './FakeRemoteAgentDO'

/** The Durable Objects this Worker hosts, and the services their code needs. */
const HostedObjectsLive = DeliveryMailboxLive.pipe(
	Layer.provideMerge(FakeRemoteAgentLive),
	Layer.provide(FetchHttpClient.layer),
	Layer.provideMerge(NodeCrypto.layer),
)

/** The mailbox namespace the bot's routes forward to. */
const DeliveryMailboxesLive = Layer.effect(DeliveryMailboxes, DeliveryMailbox)

/**
 * A plain-text page saying how far a fake remote agent job has got. It is the run-log link a handed-off
 * Linear session shows. It never shows the job's token.
 */
const FakeAgentRunLogLive = Layer.unwrap(
	Effect.gen(function* () {
		const agents = yield* FakeRemoteAgent
		return HttpRouter.add(
			'GET',
			'/fake-agent/runs/:deliveryId',
			Effect.gen(function* () {
				const { deliveryId } = yield* HttpRouter.schemaPathParams(Schema.Struct({ deliveryId: Schema.NonEmptyString }))
				return HttpServerResponse.text(yield* agents.getByName(deliveryId).describe())
			}).pipe(Effect.catchTag('SchemaError', () => Effect.succeed(HttpServerResponse.empty({ status: 404 })))),
		)
	}),
)

/** Provider webhooks, the delivery API, and the fake agent's run log. Leave `bot.deliveryApi` out to serve webhooks only. */
const RoutesLive = Layer.mergeAll(bot.routes, bot.deliveryApi, FakeAgentRunLogLive).pipe(
	Layer.provide(DeliveryMailboxesLive),
)

export default Cloudflare.Worker(
	'IngressWorker',
	{ main: import.meta.url },
	Effect.gen(function* () {
		const fetch = yield* ChannelsCloudflare.serve(RoutesLive)
		return { fetch }
	}).pipe(Effect.provide(HostedObjectsLive)),
)
