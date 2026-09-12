import { assert, it } from '@effect/vitest'
import {
	GitHub,
	GitHubCommentData,
	GitHubIngress,
	GitHubIssueData,
	GitHubSubscriptions,
	type GitHubActivityEvent,
} from '@humanlayer/channels-github'
import { layer as memory } from '@humanlayer/channels-github/memory'
import { ConfigProvider, Context, Effect, Layer, Logger, Schema } from 'effect'
import { FetchHttpClient, HttpClient, HttpClientResponse } from 'effect/unstable/http'

import { adminCall, captureWebhooks, emulator, eventFor, host, secret } from '../../../packages/github/test/support.js'
import { bot } from '../src/app.js'

it.live('example routes accept emulator-generated signed App deliveries and write native comments', () =>
	Effect.gen(function* () {
		const capture = yield* captureWebhooks
		const em = yield* emulator(capture.url)
		const storage = memory({ maxMailboxes: 10 })
		const config = ConfigProvider.layer(
			ConfigProvider.fromUnknown({ GITHUB_WEBHOOK_SECRET: secret, GITHUB_BOT_LOGIN: 'channels[bot]' }),
		)
		const environment = yield* Layer.build(
			bot.services.pipe(Layer.provideMerge(storage), Layer.provideMerge(em.credentials)),
		)
		const send = yield* host(
			bot.routes.pipe(
				Layer.provide(Layer.succeedContext(environment)),
				Layer.provide(storage),
				Layer.provide(em.credentials),
				Layer.provide(config),
			),
		)
		const unmentioned = yield* adminCall(em.resource.url, '/repos/alice/project/issues', GitHubIssueData, {
			title: 'Observe only',
			body: 'No opt-in',
		})
		const creation = yield* capture.take
		const creationId = creation.headers.get('x-github-delivery')
		assert.ok(creationId)
		assert.equal((yield* send(creation)).status, 200)
		yield* Context.get(environment, GitHubIngress).processActivity({
			event: eventFor(em.repository, unmentioned, creationId),
		})
		assert.isFalse(
			yield* Context.get(environment, GitHubSubscriptions).isSubscribed({
				namespace: 'github-example',
				resource: eventFor(em.repository, unmentioned).resource,
			}),
		)
		assert.deepEqual(
			yield* Context.get(environment, GitHub).listComments({
				issue: eventFor(em.repository, unmentioned).resource,
			}),
			[],
		)
		const issue = yield* adminCall(em.resource.url, '/repos/alice/project/issues', GitHubIssueData, {
			title: 'Example',
			body: '@channels Native generated webhook',
		})
		const webhook = yield* capture.take
		const deliveryId = webhook.headers.get('x-github-delivery')
		assert.ok(deliveryId)
		const duplicate = webhook.clone()
		assert.equal((yield* send(webhook)).status, 200)
		const ingress = Context.get(environment, GitHubIngress)
		const logs: string[] = []
		yield* ingress
			.processActivity({ event: eventFor(em.repository, issue, deliveryId) })
			.pipe(Effect.provide(Logger.layer([Logger.make((entry) => logs.push(JSON.stringify(entry.message)))])))
		assert.equal(logs.filter((log) => log.includes('Creation observed')).length, 1)
		assert.notInclude(logs.join(''), 'Followed GitHub activity')
		assert.equal((yield* send(duplicate)).status, 200)
		yield* ingress.processActivity({ event: eventFor(em.repository, issue, deliveryId) })
		const comments = yield* adminCall(
			em.resource.url,
			`/repos/alice/project/issues/${issue.number}/comments`,
			Schema.Array(GitHubCommentData),
		)
		assert.deepEqual(
			comments.map((comment) => comment.body),
			[`Received issues for #${issue.number}.`],
		)
		yield* capture.take
		yield* adminCall(em.resource.url, `/repos/alice/project/issues/${issue.number}/comments`, GitHubCommentData, {
			body: '@channels A human followup',
		})
		const followup = yield* capture.take
		assert.equal(followup.headers.get('x-github-event'), 'issue_comment')
		assert.equal((yield* send(followup)).status, 200)
		yield* ingress.processActivity({ event: eventFor(em.repository, issue) })
		const final = yield* adminCall(
			em.resource.url,
			`/repos/alice/project/issues/${issue.number}/comments`,
			Schema.Array(GitHubCommentData),
		)
		assert.deepEqual(
			final.map((comment) => comment.body),
			[comments[0]?.body, '@channels A human followup', `Received issue_comment for #${issue.number}.`],
		)
		assert.equal(
			yield* Context.get(environment, GitHubSubscriptions).isSubscribed({
				namespace: 'github-example',
				resource: eventFor(em.repository, issue).resource,
			}),
			true,
		)
		yield* capture.take
		yield* adminCall(em.resource.url, `/repos/alice/project/issues/${issue.number}/comments`, GitHubCommentData, {
			body: 'Ordinary followed activity',
		})
		assert.equal((yield* send(yield* capture.take)).status, 200)
		yield* ingress.processActivity({ event: eventFor(em.repository, issue) })
		const silent = yield* adminCall(
			em.resource.url,
			`/repos/alice/project/issues/${issue.number}/comments`,
			Schema.Array(GitHubCommentData),
		)
		assert.deepEqual(
			silent.map((comment) => comment.body),
			[...final.map((comment) => comment.body), 'Ordinary followed activity'],
		)
	}).pipe(Effect.provide(FetchHttpClient.layer)),
)

it.live('emulator PR mention and native discussion writes with synthetic shared issue read for emulator gap', () =>
	Effect.gen(function* () {
		const capture = yield* captureWebhooks
		const em = yield* emulator(capture.url)
		const pull_request = yield* adminCall(em.resource.url, '/repos/alice/project/pulls', GitHubIssueData, {
			title: 'Mention on a PR',
			head: 'feature',
			base: 'main',
			body: '@channels please investigate',
		})
		const sharedIssueRead = Layer.effect(
			HttpClient.HttpClient,
			Effect.gen(function* () {
				const client = yield* HttpClient.HttpClient
				return HttpClient.make((request) =>
					request.method === 'GET' &&
					request.url === `${em.resource.url}/repos/alice/project/issues/${pull_request.number}`
						? Effect.succeed(
								HttpClientResponse.fromWeb(
									request,
									Response.json({
										...pull_request,
										pull_request: {
											url: `${em.resource.url}/repos/alice/project/pulls/${pull_request.number}`,
										},
									}),
								),
							)
						: client.execute(request),
				)
			}),
		).pipe(Layer.provide(FetchHttpClient.layer))
		const storage = memory({ maxMailboxes: 10 })
		const environment = yield* Layer.build(
			bot.services.pipe(
				Layer.provideMerge(storage),
				Layer.provideMerge(sharedIssueRead),
				Layer.provideMerge(em.credentials),
			),
		)
		const send = yield* host(
			bot.routes.pipe(
				Layer.provide(Layer.succeedContext(environment)),
				Layer.provide(storage),
				Layer.provide(em.credentials),
				Layer.provide(
					ConfigProvider.layer(
						ConfigProvider.fromUnknown({
							GITHUB_WEBHOOK_SECRET: secret,
							GITHUB_BOT_LOGIN: 'channels[bot]',
						}),
					),
				),
			),
		)
		const webhook = yield* capture.take
		assert.equal(webhook.headers.get('x-github-event'), 'pull_request')
		const deliveryId = webhook.headers.get('x-github-delivery')
		assert.ok(deliveryId)
		const event: GitHubActivityEvent = {
			event: 'pull_request',
			action: 'opened',
			deliveryId,
			pull_request,
			resource: { kind: 'github.pull-request', repository: em.repository, number: pull_request.number },
			sender: pull_request.user,
		}
		assert.equal((yield* send(webhook)).status, 200)
		yield* Context.get(environment, GitHubIngress).processActivity({ event })
		const github = Context.get(environment, GitHub)
		const comments = yield* github.listComments({ issue: event.resource })
		assert.equal(comments.length, 1)
		const comment = comments[0]
		assert.ok(comment)
		assert.equal(comment.data.body, `Received pull_request for #${pull_request.number}.`)
		yield* github.updateComment({ comment: comment.ref, body: 'Updated PR discussion reply' })
		assert.equal(
			(yield* github.listComments({ issue: event.resource }))[0]?.data.body,
			'Updated PR discussion reply',
		)
		assert.equal(
			(yield* github
				.createComment({ issue: { ...event.resource, kind: 'github.issue' }, body: 'wrong kind' })
				.pipe(Effect.flip)).reason,
			'invalid_input',
		)
		assert.equal((yield* github.listComments({ issue: event.resource })).length, 1)
	}).pipe(Effect.provide(FetchHttpClient.layer)),
)
