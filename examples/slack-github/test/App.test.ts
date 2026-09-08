import { assert, it } from '@effect/vitest'
import { GitHubCommentData, GitHubIssueData } from '@humanlayer/channels-github'
import { ConfigProvider, Effect, Layer, Queue, Schema } from 'effect'
import { FetchHttpClient, HttpClient, HttpRouter } from 'effect/unstable/http'

import {
	adminCall,
	emulator,
	eventFor,
	payloadFor,
	secret,
	signedRequest,
} from '../../../packages/github/test/support.js'
import { slackEmulatorSigningSecret } from '../../../packages/slack/test/integration/support/SlackEmulator.js'
import { application } from '../src/app.js'
import {
	SlackEmulator,
	makeExampleTestTransport,
	HistoryResponse,
	PostedMessageResponse,
	slackEmulatorBotToken,
	slackEmulatorAliceToken,
} from './support.js'

it.live('combined host runs GitHub fanout to both providers and independent signed Slack mentions', () =>
	Effect.gen(function* () {
		const em = yield* emulator()
		const slack = yield* SlackEmulator
		const test = yield* makeExampleTestTransport
		const githubPosts = yield* Queue.unbounded<void>()
		const transport = Layer.effect(
			HttpClient.HttpClient,
			Effect.gen(function* () {
				const client = yield* HttpClient.HttpClient
				return HttpClient.make((request) =>
					client
						.execute(request)
						.pipe(
							Effect.tap(() =>
								request.method === 'POST' &&
								request.url.startsWith(em.resource.url) &&
								request.url.endsWith('/comments')
									? Queue.offer(githubPosts, undefined)
									: Effect.void,
							),
						),
				)
			}),
		).pipe(Layer.provideMerge(Layer.merge(em.credentials, test.transport)))
		const dependencies = Layer.mergeAll(
			transport,
			ConfigProvider.layer(
				ConfigProvider.fromUnknown({
					GITHUB_WEBHOOK_SECRET: secret,
					GITHUB_BOT_LOGIN: 'channels[bot]',
					SLACK_TEAM_ID: slack.teamId,
					SLACK_NOTIFICATION_CHANNEL_ID: slack.publicChannelId,
					SLACK_SIGNING_SECRET: slackEmulatorSigningSecret,
				}),
			),
		)
		const memoMap = yield* Layer.makeMemoMap
		const app = application.pipe(Layer.provide(dependencies))
		const web = HttpRouter.toWebHandler(app, { memoMap, disableLogger: true })
		yield* Effect.addFinalizer(() => Effect.promise(web.dispose))
		const issue = yield* adminCall(em.resource.url, '/repos/alice/project/issues', GitHubIssueData, {
			title: 'Combined',
			body: '@channels One integration event',
		})
		const value = eventFor(em.repository, issue)
		const request = signedRequest('issues', JSON.stringify(payloadFor(value)), value.deliveryId)
		assert.equal((yield* Effect.promise(() => web.handler(request))).status, 200)
		yield* Queue.take(test.posts)
		yield* Queue.take(githubPosts)
		const history = yield* slack.call(
			slackEmulatorBotToken,
			'conversations.history',
			{ channel: slack.publicChannelId },
			HistoryResponse,
		)
		assert.ok(
			history.messages.some(
				(message) => message.text === `GitHub mention on github.issue #${issue.number} in alice/project`,
			),
		)
		const mention = yield* slack.call(
			slackEmulatorAliceToken,
			'chat.postMessage',
			{ channel: slack.publicChannelId, text: 'hello combined' },
			PostedMessageResponse,
		)
		assert.equal(
			(yield* Effect.promise(() =>
				web.handler(
					slack.signedWebhook({
						type: 'event_callback',
						team_id: slack.teamId,
						event_id: 'combined-slack-1',
						event_time: 1,
						event: {
							type: 'app_mention',
							user: slack.aliceUserId,
							channel: slack.publicChannelId,
							ts: mention.ts,
							text: 'hello combined',
						},
					}),
				),
			)).status,
			200,
		)
		yield* Queue.take(test.posts)
		const replies = yield* slack.call(
			slackEmulatorBotToken,
			'conversations.replies',
			{ channel: slack.publicChannelId, ts: mention.ts },
			HistoryResponse,
		)
		assert.ok(replies.messages.some((message) => message.text === 'Durable echo: hello combined'))
		const comments = yield* adminCall(
			em.resource.url,
			`/repos/alice/project/issues/${issue.number}/comments`,
			Schema.Array(GitHubCommentData),
		)
		assert.equal(comments.length, 1)
		assert.equal(
			(yield* Effect.promise(() => web.handler(new Request('http://test/unknown', { method: 'POST' })))).status,
			404,
		)
		assert.equal(
			(yield* Effect.promise(() =>
				web.handler(
					new Request('http://test/api/v1/integrations/slack/webhook', { method: 'POST', body: '{}' }),
				),
			)).status,
			401,
		)
	}).pipe(Effect.provide(SlackEmulator.layer), Effect.provide(FetchHttpClient.layer)),
)
