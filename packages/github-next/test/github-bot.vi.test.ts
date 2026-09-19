import * as NodeCrypto from '@effect/platform-node/NodeCrypto'
import { describe, it } from '@effect/vitest'
import { Channels, ChannelsMemory, QueueDeliveryMode } from '@humanlayer/channels-delivery-next'
import { Config, Deferred, Effect, Layer, Redacted } from 'effect'
import { TestClock } from 'effect/testing'
import { HttpRouter, HttpServerRequest } from 'effect/unstable/http'

import { GitHubApi, GitHubBot, GitHubId } from '../src'
import { githubWebhookSecret, issuePayload, signedGitHubInput } from './fixtures'

describe('GitHubBot.make', () => {
	it.effect('carries a signed issue webhook through Channels.make to onIssueCreated', ({ expect }) =>
		Effect.gen(function* () {
			const created = yield* Deferred.make<{ readonly number: number; readonly subscribed: boolean }>()
			const bot = Channels.make({
				namespace: 'github-bot-test',
				basePath: '/api/channels',
				providers: [
					GitHubBot.make({
						webhookSecret: Config.succeed(Redacted.make(githubWebhookSecret)),
						deliveryMode: QueueDeliveryMode.make({}),
						bot: { mentionNames: ['channels-bot'], botUserId: GitHubId.make(999) },
						gitHubApi: Layer.mock(GitHubApi, {}),
						handlers: {
							onIssueCreated: (event) =>
								Effect.gen(function* () {
									yield* event.issue.subscribe()
									const subscribed = yield* event.issue.isSubscribed()
									yield* Deferred.succeed(created, { number: event.issue.ref.number, subscribed })
								}),
						},
					}),
				],
				eventProcessing: { concurrency: 1, leaseMs: 30_000 },
				storage: ChannelsMemory.make({ polling: { intervalMs: 1_000 } }),
			})
			const fetch = yield* HttpRouter.toHttpEffect(bot.routes).pipe(Effect.provide(NodeCrypto.layer))
			const signed = signedGitHubInput('issues', issuePayload('opened'))

			const response = yield* fetch.pipe(
				Effect.provideService(
					HttpServerRequest.HttpServerRequest,
					HttpServerRequest.fromWeb(
						new Request('http://localhost/api/channels/integrations/github/webhook', {
							method: 'POST',
							headers: signed.headers,
							body: new TextDecoder().decode(signed.body),
						}),
					),
				),
			)
			yield* TestClock.adjust(1_000)

			expect(response.status).toBe(200)
			expect(yield* Deferred.await(created)).toEqual({ number: 42, subscribed: true })
		}),
	)
})
