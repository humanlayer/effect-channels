import { NodeCrypto } from '@effect/platform-node'
import { describe, it } from '@effect/vitest'
import { Channels, ChannelsMemory } from '@humanlayer/channels-delivery-next'
import { Config, Deferred, Effect, Layer, Redacted } from 'effect'
import { TestClock } from 'effect/testing'
import { HttpRouter, HttpServerRequest } from 'effect/unstable/http'

import { LinearAuth, LinearBot } from '../src/index'
import { LinearApi } from '../src/LinearApi'
import { linearAppUserId, linearOrganizationId, linearWebhookSecret, signedLinearInput } from './fixtures'

describe('Linear Channels storage composition', () => {
	it.effect('runs the same provider through ChannelsMemory and persists subscription state', ({ expect }) =>
		Effect.gen(function* () {
			const created = yield* Deferred.make<{ readonly identifier: string; readonly subscribed: boolean }>()
			const channels = Channels.make({
				namespace: 'linear-storage-test',
				providers: [
					LinearBot.make({
						webhookSecret: Config.succeed(Redacted.make(linearWebhookSecret)),
						bot: { organizationId: linearOrganizationId, appUserId: linearAppUserId },
						auth: LinearAuth.clientCredentials({
							clientId: Config.succeed('test-client'),
							clientSecret: Config.succeed(Redacted.make('test-secret')),
						}),
						linearApi: Layer.mock(LinearApi, {}),
						handlers: {
							onIssueCreated: (event) =>
								Effect.gen(function* () {
									yield* event.issue.subscribe()
									yield* Deferred.succeed(created, {
										identifier: event.issue.identifier,
										subscribed: yield* event.issue.isSubscribed(),
									})
								}),
						},
					}),
				],
				eventProcessing: { concurrency: 1, leaseMs: 30_000 },
				storage: ChannelsMemory.make({ polling: { intervalMs: 1_000 } }),
			})
			const fetch = yield* HttpRouter.toHttpEffect(channels.routes).pipe(Effect.provide(NodeCrypto.layer))
			const signed = signedLinearInput()
			const response = yield* fetch.pipe(
				Effect.provideService(
					HttpServerRequest.HttpServerRequest,
					HttpServerRequest.fromWeb(
						new Request('http://localhost/integrations/linear/webhook', {
							method: 'POST',
							headers: signed.headers,
							body: new TextDecoder().decode(signed.body),
						}),
					),
				),
			)
			yield* TestClock.adjust(1_000)
			expect(response.status).toBe(200)
			expect(yield* Deferred.await(created)).toEqual({ identifier: 'ENG-123', subscribed: true })
		}),
	)
})
