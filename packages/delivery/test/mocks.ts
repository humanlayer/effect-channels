import { Context, Effect } from 'effect'
import { HttpRouter } from 'effect/http'

import { DeliveryReceipt, MailboxDelivery, webhookRoutes, type WebhookProvider } from '../src'

/**
 * Test MailboxDelivery that accepts every admission without storing it.
 */
export const noopMailboxDelivery: typeof MailboxDelivery.Service = {
	deliver: () =>
		Effect.succeed(
			DeliveryReceipt.make({
				mailboxKey: 'test-mailbox',
				accepted: true,
			}),
		),
}

export const makeWebhookTestApp = (
	handle: WebhookProvider<never>['handle'],
	mailboxDelivery: typeof MailboxDelivery.Service = noopMailboxDelivery,
	maxBodyBytes?: number,
) =>
	Effect.gen(function* () {
		const provider: WebhookProvider<never> = {
			providerName: 'example',
			maxBodyBytes,
			handle,
		}
		const web = HttpRouter.toWebHandler(webhookRoutes([provider]), { disableLogger: true })
		yield* Effect.addFinalizer(() => Effect.promise(web.dispose))
		const context = Context.make(MailboxDelivery, mailboxDelivery)
		return {
			post: (integration = provider.providerName, body?: string) =>
				Effect.promise(() =>
					web.handler(
						new Request(`http://localhost/integrations/${integration}/webhook`, { method: 'POST', body }),
						context,
					),
				),
		}
	})
