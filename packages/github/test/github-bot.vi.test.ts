import * as NodeCrypto from '@effect/platform-node/NodeCrypto'
import { describe, it } from '@effect/vitest'
import {
	Channels,
	ChannelsMemory,
	QueueDeliveryMode,
	makeDeliveryClient,
	type DeliveryContext,
} from '@humanlayer/channels-delivery'
import { Config, ConfigProvider, Deferred, Effect, Layer, Queue, Redacted, Schema, Scope } from 'effect'
import {
	HttpClient,
	HttpClientRequest,
	HttpClientResponse,
	HttpRouter,
	HttpServerRequest,
	HttpServerResponse,
} from 'effect/http'
import { TestClock } from 'effect/testing'

import { GitHubApi, GitHubBot, GitHubId, GitHubIssueMentioned } from '../src'
import { githubWebhookSecret, issuePayload, signedGitHubInput } from './fixtures'

describe('GitHubBot.make', () => {
	for (const handoff of [false, true]) {
		it.effect(
			`runs the saved opened-issue mention after creation subscribes${handoff ? ' and its handoff completes' : ''}`,
			({ expect }) =>
				Effect.gen(function* () {
					const created = yield* Deferred.make<DeliveryContext>()
					const mentioned = yield* Deferred.make<DeliveryContext>()
					const calls = yield* Queue.unbounded<string>()
					const payload = issuePayload('opened')
					const body = '@channels-bot please investigate'
					const bot = Channels.make({
						namespace: 'github-creation-mention-test',
						basePath: '/api/channels',
						providers: [
							GitHubBot.make({
								webhookSecret: Config.succeed(Redacted.make(githubWebhookSecret)),
								deliveryMode: QueueDeliveryMode.make({}),
								bot: { mentionNames: ['channels-bot'], botUserId: GitHubId.make(999) },
								gitHubApi: Layer.mock(GitHubApi, {}),
								handlers: {
									onIssueCreated: (event, delivery) =>
										Effect.gen(function* () {
											expect(event.trigger.body).toBe(body)
											expect(event.issue.ref.number).toBe(42)
											yield* event.issue.subscribe()
											expect(yield* event.issue.isSubscribed()).toBe(true)
											yield* Queue.offer(calls, 'created')
											if (handoff) {
												const result = yield* delivery.handoff()
												yield* Deferred.succeed(created, delivery)
												return result
											}
											yield* Deferred.succeed(created, delivery)
										}),
									onMentioned: (event, delivery) =>
										Effect.gen(function* () {
											const issueMention = yield* Schema.decodeUnknownEffect(
												GitHubIssueMentioned,
											)(event).pipe(Effect.orDie)
											expect(issueMention.issue.ref.number).toBe(42)
											expect(issueMention.trigger).toMatchObject({
												_tag: 'GitHubIssueOpened',
												body,
											})
											expect(issueMention.events).toEqual([])
											expect(yield* issueMention.issue.isSubscribed()).toBe(true)
											yield* Queue.offer(calls, 'mentioned')
											yield* Deferred.succeed(mentioned, delivery)
										}),
									onSubscribedIssueEvents: () =>
										Effect.die(
											new Error('the saved mention must not be reselected after subscribing'),
										),
								},
							}),
						],
						eventProcessing: { concurrency: 1, leaseMs: 30_000 },
						storage: ChannelsMemory.make({ polling: { intervalMs: 1_000 } }),
					})
					const scope = yield* Effect.scope
					const fetch = yield* HttpRouter.toHttpEffect(Layer.merge(bot.routes, bot.deliveryApi)).pipe(
						Effect.provide(
							Layer.merge(
								NodeCrypto.layer,
								Layer.succeed(HttpRouter.RouterConfig, Channels.routerConfig),
							),
						),
					)
					const request = (web: Request) =>
						fetch.pipe(
							Effect.provideService(HttpServerRequest.HttpServerRequest, HttpServerRequest.fromWeb(web)),
						)
					const http = HttpClient.make((input) =>
						HttpClientRequest.toWeb(input).pipe(
							Effect.orDie,
							Effect.flatMap((web) =>
								request(web).pipe(Effect.provideService(Scope.Scope, scope), Effect.orDie),
							),
							Effect.map((response) =>
								HttpClientResponse.fromWeb(input, HttpServerResponse.toWeb(response)),
							),
						),
					)
					const client = yield* makeDeliveryClient({
						baseUrl: 'http://localhost',
						basePath: '/api/channels',
					}).pipe(Effect.provideService(HttpClient.HttpClient, http))
					const signed = signedGitHubInput('issues', { ...payload, issue: { ...payload.issue, body } })
					expect(
						(yield* request(
							new Request('http://localhost/api/channels/integrations/github/webhook', {
								method: 'POST',
								headers: signed.headers,
								body: new TextDecoder().decode(signed.body),
							}),
						)).status,
					).toBe(200)
					yield* TestClock.adjust(1_000)
					const first = yield* Deferred.await(created)
					const target = { deliveryId: first.deliveryId, accessToken: first.accessToken }
					if (handoff) {
						expect((yield* client.status(target)).stage).toBe('ExternalWaiting')
						yield* TestClock.adjust(1_000)
						expect(yield* Deferred.isDone(mentioned)).toBe(false)
						expect(yield* Queue.size(calls)).toBe(1)
						expect((yield* client.complete(target)).status).toBe('accepted')
						yield* TestClock.adjust(1_000)
					}
					const second = yield* Deferred.await(mentioned)
					expect(second.deliveryId).not.toBe(first.deliveryId)
					expect(second.conversationId).toBe(first.conversationId)
					expect((yield* client.status(target)).stage).toBe('Retired')
					expect(yield* Queue.take(calls)).toBe('created')
					expect(yield* Queue.take(calls)).toBe('mentioned')
					expect(yield* Queue.size(calls)).toBe(0)
				}),
		)
	}

	it.effect('reads callback configuration while building the webhook provider', ({ expect }) =>
		Effect.gen(function* () {
			const provider = GitHubBot.make({
				webhookSecret: Config.succeed(Redacted.make(githubWebhookSecret)),
				deliveryMode: QueueDeliveryMode.make({}),
				bot: Config.all({
					mentionNames: Config.String('GITHUB_BOT_MENTION_NAME').pipe(Config.map((name) => [name])),
					botUserId: Config.schema(GitHubId, 'GITHUB_BOT_USER_ID'),
				}),
				gitHubApi: Layer.mock(GitHubApi, {}),
				handlers: {},
			})

			const error = yield* provider
				.webhookProvider({ namespace: 'github-bot-test' })
				.pipe(Effect.scoped, Effect.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({}))), Effect.flip)

			expect(error).toBeInstanceOf(Config.ConfigError)
		}),
	)

	it.effect('reads the default API configuration while building the webhook provider', ({ expect }) =>
		Effect.gen(function* () {
			const provider = GitHubBot.make({
				webhookSecret: Config.succeed(Redacted.make(githubWebhookSecret)),
				deliveryMode: QueueDeliveryMode.make({}),
				bot: { mentionNames: ['channels-bot'], botUserId: GitHubId.make(999) },
				handlers: {},
			})

			const error = yield* provider
				.webhookProvider({ namespace: 'github-bot-test' })
				.pipe(Effect.scoped, Effect.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({}))), Effect.flip)

			expect(error).toBeInstanceOf(Config.ConfigError)
		}),
	)

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
