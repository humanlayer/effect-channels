import * as NodeCrypto from '@effect/platform-node/NodeCrypto'
import * as Cloudflare from 'alchemy/Cloudflare'
import { Config, Effect } from 'effect'

import { bot } from './Bot'
import { DeliveryMailbox } from './DurableObject'

/** Reads during Worker initialization bind runtime configuration as Cloudflare secrets at deploy time. */
const RuntimeConfig = Config.all({
	SLACK_SIGNING_SECRET: Config.redacted('SLACK_SIGNING_SECRET'),
	SLACK_BOT_TOKEN: Config.redacted('SLACK_BOT_TOKEN'),
	GITHUB_WEBHOOK_SECRET: Config.redacted('GITHUB_WEBHOOK_SECRET'),
	GITHUB_APP_ID: Config.string('GITHUB_APP_ID'),
	GITHUB_PRIVATE_KEY: Config.redacted('GITHUB_PRIVATE_KEY'),
	GITHUB_BOT_MENTION_NAME: Config.string('GITHUB_BOT_MENTION_NAME'),
	GITHUB_BOT_USER_ID: Config.string('GITHUB_BOT_USER_ID'),
})

export default Cloudflare.Worker(
	'IngressWorker',
	{ main: import.meta.url },
	Effect.gen(function* () {
		yield* RuntimeConfig
		const mailboxes = yield* DeliveryMailbox
		const fetch = yield* bot.ingress(mailboxes).fetch.pipe(Effect.provide(NodeCrypto.layer))

		return { fetch }
	}),
)
