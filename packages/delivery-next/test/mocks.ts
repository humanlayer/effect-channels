import { Context, Effect } from 'effect'
import { HttpRouter } from 'effect/unstable/http'

import { DeliveryQueue, DeliveryReceipt, webhookRoutes, type WebhookProvider } from '../src'

/**
 * Test DeliveryQueue that accepts every admission without storing it.
 */
export const noopDeliveryQueue: typeof DeliveryQueue.Service = {
	enqueue: () =>
		Effect.succeed(
			DeliveryReceipt.make({
				mailboxKey: 'test-mailbox',
				accepted: true,
			}),
		),
}

export const makeWebhookTestApp = (
	handle: WebhookProvider<never>['handle'],
	queue: typeof DeliveryQueue.Service = noopDeliveryQueue,
) =>
	Effect.gen(function* () {
		const provider: WebhookProvider<never> = {
			key: 'example',
			handle,
		}
		const web = HttpRouter.toWebHandler(webhookRoutes([provider]), { disableLogger: true })
		yield* Effect.addFinalizer(() => Effect.promise(web.dispose))
		const context = Context.make(DeliveryQueue, queue)
		return {
			post: (integration = provider.key) =>
				Effect.promise(() =>
					web.handler(
						new Request(`http://localhost/integrations/${integration}/webhook`, { method: 'POST' }),
						context,
					),
				),
		}
	})
