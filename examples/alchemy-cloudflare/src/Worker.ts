import * as NodeCrypto from '@effect/platform-node/NodeCrypto'
import * as Cloudflare from 'alchemy/Cloudflare'
import { Effect } from 'effect'

import { bot } from './Bot'
import { DeliveryMailbox } from './DurableObject'

export default Cloudflare.Worker(
	'IngressWorker',
	{ main: import.meta.url },
	Effect.gen(function* () {
		const mailboxes = yield* DeliveryMailbox
		const fetch = yield* bot.ingress(mailboxes).fetch.pipe(Effect.provide(NodeCrypto.layer))

		return { fetch }
	}),
)
