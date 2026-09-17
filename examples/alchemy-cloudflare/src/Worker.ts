import * as NodeCrypto from '@effect/platform-node/NodeCrypto'
import { MailboxDeliveryAlchemyCloudflare } from '@humanlayer/channels-alchemy-cloudflare'
import { webhookRoutes } from '@humanlayer/channels-delivery-next'
import { makeSlackWebhookProvider } from '@humanlayer/channels-slack-next'
import * as Cloudflare from 'alchemy/Cloudflare'
import { Config, Effect, Layer } from 'effect'
import * as HttpRouter from 'effect/unstable/http/HttpRouter'

import { applicationNamespace } from './config'
import { DeliveryMailbox } from './DurableObject'
import { SlackApiAlchemyCloudflare } from './SlackProvider'

export default Cloudflare.Worker(
	'IngressWorker',
	{ main: import.meta.url },
	Effect.gen(function* () {
		const signingSecret = yield* Config.redacted('SLACK_SIGNING_SECRET')
		const mailboxes = yield* DeliveryMailbox
		const slack = makeSlackWebhookProvider({
			namespace: applicationNamespace,
			signingSecret,
		})
		const requestServices = Layer.mergeAll(
			NodeCrypto.layer,
			MailboxDeliveryAlchemyCloudflare(mailboxes),
			SlackApiAlchemyCloudflare,
		)
		const routes = webhookRoutes([slack]).pipe(HttpRouter.provideRequest(requestServices))

		return {
			fetch: HttpRouter.toHttpEffect(routes).pipe(Effect.flatten),
		}
	}),
)
