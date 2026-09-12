import { NodeCrypto } from '@effect/platform-node'
import { assert, it } from '@effect/vitest'
import { MailboxStore } from '@humanlayer/channels-delivery'
import { GitHubIssueData, GitHubSubscriptions } from '@humanlayer/channels-github'
import {
	SlackTeamId,
	SlackChannelId,
	SlackMessageTs,
	SlackSubscriptions,
	encodeSlackThreadId,
} from '@humanlayer/channels-slack'
import { connections } from '@humanlayer/channels-slack/memory'
import { ConfigProvider, Context, Effect, Layer, Queue, Redacted, Ref, Schema } from 'effect'
import { FetchHttpClient, HttpClient, HttpClientRequest, HttpRouter } from 'effect/unstable/http'

import { adminCall, captureWebhooks, emulator, secret } from '../../../packages/github/test/support.js'
import {
	HistoryResponse,
	PostedMessageResponse,
	SlackEmulator,
	slackEmulatorAliceToken,
	slackEmulatorBotToken,
	slackEmulatorBotUserId,
	slackEmulatorBotId,
	slackEmulatorSigningSecret,
} from '../../../packages/slack/test/integration/support/SlackEmulator.js'
import { application } from '../src/app.js'
import { githubNamespace } from '../src/handlers.js'
import { storage } from '../src/storage.js'

it.live(
	'the complete application runs both signed routes and workers with the supplied organization callbacks',
	() =>
		Effect.gen(function* () {
			const slack = yield* SlackEmulator
			const capture = yield* captureWebhooks
			const github = yield* emulator(capture.url)
			const completed = yield* Queue.unbounded<string>()
			const commits = yield* Ref.make(0)
			const stored = yield* Layer.build(storage)
			const underlying = Context.get(stored, MailboxStore)
			const stores = Layer.succeedContext(
				Context.add(
					stored,
					MailboxStore,
					MailboxStore.of({
						...underlying,
						commitMailbox: (input) =>
							Effect.gen(function* () {
								const previous = yield* underlying.loadMailbox({ key: input.key })
								const result = yield* underlying.commitMailbox(input)
								if (result === 'committed') yield* Ref.update(commits, (count) => count + 1)
								if (
									result === 'committed' &&
									input.nextState.outcomes.some(
										(outcome) =>
											outcome.kind === 'completed' &&
											!previous?.state.outcomes.some(
												(saved) => saved.identity === outcome.identity,
											),
									)
								)
									yield* Queue.offer(completed, input.key)
								return result
							}),
					}),
				),
			)
			const client = yield* HttpClient.HttpClient
			const http = Layer.succeed(
				HttpClient.HttpClient,
				HttpClient.make((request) => {
					if (request.url.startsWith('https://slack.com/api/'))
						return client.execute(
							HttpClientRequest.setUrl(
								request,
								request.url.replace('https://slack.com/api', `${slack.emulator.url}/api`),
							),
						)
					if (request.url.startsWith(`${github.resource.url}/`)) return client.execute(request)
					return Effect.die('Unexpected provider origin')
				}),
			)
			const dependencies = Layer.mergeAll(
				stores,
				github.credentials,
				NodeCrypto.layer,
				connections({
					connections: [
						{
							workspaceId: SlackTeamId.make('T_NORTH'),
							connection: {
								credentials: {
									botToken: Redacted.make(slackEmulatorBotToken),
									botUserId: slackEmulatorBotUserId,
									botId: slackEmulatorBotId,
								},
							},
						},
					],
				}),
				ConfigProvider.layer(
					ConfigProvider.fromUnknown({
						SLACK_SIGNING_SECRET: slackEmulatorSigningSecret,
						GITHUB_WEBHOOK_SECRET: secret,
						GITHUB_BOT_LOGIN: 'channels[bot]',
					}),
				),
				http,
			)
			const web = HttpRouter.toWebHandler(application.pipe(Layer.provide(dependencies)), { disableLogger: true })
			yield* Effect.addFinalizer(() => Effect.promise(web.dispose))
			const root = yield* slack.call(
				slackEmulatorAliceToken,
				'chat.postMessage',
				{
					channel: slack.publicChannelId,
					text: 'Lookup application',
				},
				PostedMessageResponse,
			)
			assert.strictEqual(
				(yield* Effect.promise(() =>
					web.handler(
						slack.signedWebhook({
							type: 'event_callback',
							team_id: 'T_NORTH',
							event_id: 'Ev_app_organization',
							event_time: 1,
							event: {
								type: 'app_mention',
								channel: root.channel,
								ts: root.ts,
								text: 'hello',
								user: slack.aliceUserId,
							},
						}),
					),
				)).status,
				200,
			)
			const slackKey = yield* Queue.take(completed)
			assert.strictEqual((yield* underlying.loadMailbox({ key: slackKey }))?.state.outcomes[0]?.kind, 'completed')
			const history = yield* slack.call(
				slackEmulatorBotToken,
				'conversations.replies',
				{ channel: root.channel, ts: root.ts },
				HistoryResponse,
			)
			assert.deepStrictEqual(
				history.messages
					.filter((message) => message.user === slackEmulatorBotUserId)
					.map((message) => message.text),
				['Organization: north'],
			)
			const slackSubscriptions = Context.get(stored, SlackSubscriptions)
			const threadId = encodeSlackThreadId({
				teamId: SlackTeamId.make('T_NORTH'),
				channelId: SlackChannelId.make(root.channel),
				threadTs: SlackMessageTs.make(root.ts),
			})
			assert.isTrue(yield* slackSubscriptions.isSubscribed({ threadId }))
			const slackReplies = ['Organization: north']
			for (const [index, step] of [
				{ text: 'follow up', mention: false, reply: 'Organization: north', subscribed: true },
				{ text: 'please unsubscribe', mention: false, reply: 'Organization: north', subscribed: true },
				{
					text: '  UnSuBsCrIbE  ',
					mention: false,
					reply: 'Unsubscribed. Mention me to engage again.',
					subscribed: false,
				},
				{ text: 'not followed', mention: false, reply: null, subscribed: false },
				{
					text: `<@${slackEmulatorBotUserId}> hello again`,
					mention: true,
					reply: 'Organization: north',
					subscribed: true,
				},
				{
					text: `<@${slackEmulatorBotUserId}>  UNSUBSCRIBE `,
					mention: true,
					reply: 'Unsubscribed. Mention me to engage again.',
					subscribed: false,
				},
			].entries()) {
				const before = yield* underlying.loadMailbox({ key: slackKey })
				const commitsBefore = yield* Ref.get(commits)
				const message = yield* slack.call(
					slackEmulatorAliceToken,
					'chat.postMessage',
					{ channel: root.channel, thread_ts: root.ts, text: step.text },
					PostedMessageResponse,
				)
				assert.strictEqual(
					(yield* Effect.promise(() =>
						web.handler(
							slack.signedWebhook({
								type: 'event_callback',
								team_id: 'T_NORTH',
								event_id: `Ev_follow_${index}`,
								event_time: 1,
								event: {
									type: step.mention ? 'app_mention' : 'message',
									channel: root.channel,
									ts: message.ts,
									thread_ts: root.ts,
									text: step.text,
									user: slack.aliceUserId,
								},
							}),
						),
					)).status,
					200,
				)
				if (step.reply !== null) {
					const key = yield* Queue.take(completed)
					const saved = yield* underlying.loadMailbox({ key })
					assert.deepStrictEqual(saved?.state.pending, [])
					assert.strictEqual(saved?.state.active, null)
					slackReplies.push(step.reply)
				} else {
					assert.deepStrictEqual(yield* underlying.loadMailbox({ key: slackKey }), before)
					assert.strictEqual(yield* Ref.get(commits), commitsBefore)
					assert.strictEqual(yield* Queue.size(completed), 0)
				}
				assert.strictEqual(yield* slackSubscriptions.isSubscribed({ threadId }), step.subscribed)
				const replies = yield* slack.call(
					slackEmulatorBotToken,
					'conversations.replies',
					{ channel: root.channel, ts: root.ts },
					HistoryResponse,
				)
				assert.deepStrictEqual(
					replies.messages
						.filter((message) => message.user === slackEmulatorBotUserId)
						.map((message) => message.text),
					slackReplies,
				)
			}
			const issue = yield* adminCall(github.resource.url, '/repos/alice/project/issues', GitHubIssueData, {
				title: 'Lookup application',
				body: '@channels[bot] hello',
			})
			const request = yield* capture.take
			assert.strictEqual((yield* Effect.promise(() => web.handler(request))).status, 200)
			const githubKey = yield* Queue.take(completed)
			assert.notStrictEqual(slackKey, githubKey)
			assert.strictEqual(
				(yield* underlying.loadMailbox({ key: githubKey }))?.state.outcomes[0]?.kind,
				'completed',
			)
			const comments = yield* adminCall(
				github.resource.url,
				`/repos/alice/project/issues/${issue.number}/comments`,
				Schema.Array(Schema.Struct({ body: Schema.String })),
			)
			assert.deepStrictEqual(
				comments.map((comment) => comment.body),
				['Organization: north'],
			)
			yield* capture.take
			const subscriptions = Context.get(stored, GitHubSubscriptions)
			const subscription = {
				namespace: githubNamespace,
				resource: { kind: 'github.issue' as const, repository: github.repository, number: issue.number },
			}
			assert.isTrue(yield* subscriptions.isSubscribed(subscription))
			const bodies = ['Organization: north']
			for (const step of [
				{ text: 'follow up', reply: 'Organization: north', subscribed: true },
				{ text: 'unsubscribe please', reply: 'Organization: north', subscribed: true },
				{ text: '@channels[bot]unsubscribe', reply: 'Organization: north', subscribed: true },
				{ text: '  UnSuBsCrIbE  ', reply: 'Unsubscribed. Mention me to engage again.', subscribed: false },
				{ text: 'not followed', reply: null, subscribed: false },
				{ text: '@channels[bot] hello again', reply: 'Organization: north', subscribed: true },
				{
					text: '@channels[bot] UNSUBSCRIBE ',
					reply: 'Unsubscribed. Mention me to engage again.',
					subscribed: false,
				},
			]) {
				const before = yield* underlying.loadMailbox({ key: githubKey })
				const commitsBefore = yield* Ref.get(commits)
				yield* adminCall(
					github.resource.url,
					`/repos/alice/project/issues/${issue.number}/comments`,
					Schema.Struct({ body: Schema.String }),
					{ body: step.text },
				)
				bodies.push(step.text)
				const incoming = yield* capture.take
				assert.strictEqual((yield* Effect.promise(() => web.handler(incoming))).status, 200)
				if (step.reply !== null) {
					const key = yield* Queue.take(completed)
					const saved = yield* underlying.loadMailbox({ key })
					assert.deepStrictEqual(saved?.state.pending, [])
					assert.strictEqual(saved?.state.active, null)
					bodies.push(step.reply)
					yield* capture.take
				} else {
					assert.deepStrictEqual(yield* underlying.loadMailbox({ key: githubKey }), before)
					assert.strictEqual(yield* Ref.get(commits), commitsBefore)
					assert.strictEqual(yield* Queue.size(completed), 0)
				}
				assert.strictEqual(yield* subscriptions.isSubscribed(subscription), step.subscribed)
				const replies = yield* adminCall(
					github.resource.url,
					`/repos/alice/project/issues/${issue.number}/comments`,
					Schema.Array(Schema.Struct({ body: Schema.String })),
				)
				assert.deepStrictEqual(
					replies.map((comment) => comment.body),
					bodies,
				)
			}
		}).pipe(Effect.provide(Layer.merge(SlackEmulator.layer, FetchHttpClient.layer))),
	{ timeout: 20000 },
)
