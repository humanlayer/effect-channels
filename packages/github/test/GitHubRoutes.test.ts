import { assert, it } from '@effect/vitest'
import { DeliveryQueue, IngressAttributionStore } from '@humanlayer/channels-delivery'
import { Effect, Layer, Redacted } from 'effect'
import { HttpRouter } from 'effect/unstable/http'

import { GitHubCredentials, GitHubCrypto, GitHubIngress, GitHubRoutes, GitHubSubscriptionStore } from '../src/index'

it.effect('reports relative and application-mounted webhook paths', () =>
	Effect.sync(() => {
		assert.strictEqual(GitHubRoutes.webhookPath, '/integrations/github/webhook')
		assert.strictEqual(
			GitHubRoutes.mountedWebhookPath('/agents/support'),
			'/agents/support/integrations/github/webhook',
		)
	}),
)

it.effect('mounts webhook admission with addressed services and no mailbox processing services', () =>
	Effect.acquireUseRelease(
		Effect.sync(() => {
			const admission = GitHubIngress.layer({
				namespace: 'route-contract',
				policy: {
					mode: 'serial',
					maxPayloadBytes: 4096,
					maxEnvelopes: 16,
					maxOutcomes: 64,
					retentionMs: 60_000,
					maxAttempts: 3,
					retryBaseMs: 100,
					retryMaxMs: 1000,
					leaseMs: 1000,
					heartbeatMs: 100,
					conflictRetries: 8,
				},
				handlers: [],
			}).pipe(
				Layer.provide(
					Layer.mergeAll(
						Layer.mock(DeliveryQueue, {}),
						Layer.mock(IngressAttributionStore, {}),
						Layer.mock(GitHubSubscriptionStore, {}),
					),
				),
			)
			const routes = GitHubRoutes.layerMounted('/api/v1', {
				signingSecret: Redacted.make('secret'),
				maxBodyBytes: 4096,
			}).pipe(
				Layer.provide(admission),
				Layer.provide(
					Layer.merge(
						Layer.mock(GitHubCrypto, {}),
						Layer.mock(GitHubCredentials, {
							apiUrl: 'https://api.github.com',
							botUserId: 1,
							acceptsInstallation: () => true,
						}),
					),
				),
			)
			return HttpRouter.toWebHandler(routes, { disableLogger: true })
		}),
		({ handler }) =>
			Effect.promise(() => handler(new Request('http://localhost/unrouted', { method: 'POST' }))).pipe(
				Effect.map((response) => assert.strictEqual(response.status, 404)),
			),
		({ dispose }) => Effect.promise(dispose),
	),
)
